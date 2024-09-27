import logging
import asyncio
import os
import time
import signal
import subprocess
import sys
import random
from datetime import datetime
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from rich.console import Console
from rich.table import Table
from rich.live import Live
from rich.panel import Panel
from rich.text import Text
from rich.layout import Layout
from asyncio import Lock

# -------------------------------
# Configuration Parameters
# -------------------------------

# Setting up logging for better debug information
logging.basicConfig(
    level=logging.DEBUG,  # Changed to DEBUG for more detailed logs
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("script.log"),  # Log to a file
        logging.StreamHandler(),  # Re-add StreamHandler for console logging
    ],
)

USERNAME_FILE = "usernames.txt"  # File containing usernames to monitor
ACTIVE_TASKS = set()  # Track currently running username tasks to avoid duplicates
WORKING_DIR = Path(__file__).parent.resolve()
MIN_INTERVAL = 60  # Minimum interval of 1 minute
MAX_INTERVAL = 300  # Maximum interval of 5 minutes
LOGS_PER_USER = 10  # Maximum number of log files to keep per user
MAX_CONCURRENT_DOWNLOADS = 5  # Limit concurrent downloads
console = Console()

# Status dictionary for usernames
user_status = {}
# Runtime tracking dictionary
user_runtime = {}

# Registry to keep track of running subprocesses
running_processes = {}  # username: subprocess

# Locks to protect shared resources
active_tasks_lock = Lock()
running_processes_lock = Lock()

semaphore = None  # Will be initialized in main()

# To keep track of asyncio tasks for each user
user_tasks = {}  # username: asyncio.Task

# Logs buffer and its lock
logs_buffer = []
logs_lock = Lock()
MAX_LOG_LINES = 1000  # Maximum number of log lines to keep


VERBOSE = False  # Set to True to view this script's logs in the console


class UsernameListHandler(FileSystemEventHandler):
    """Handles the file modification events to reload username list."""

    def __init__(self, callback, loop):
        super().__init__()
        self.callback = callback
        self.loop = loop

    def on_modified(self, event):
        if Path(event.src_path).name == USERNAME_FILE:
            if VERBOSE:
                logging.info("Username file updated; reloading usernames.")
            asyncio.run_coroutine_threadsafe(self.callback(), self.loop)


def check_downloader_script():
    """Check if the Node.js downloader script exists."""
    downloader_path = WORKING_DIR / "build" / "app.js"
    if not downloader_path.is_file():
        logging.error(f"Downloader script not found at {downloader_path}. Exiting...")
        sys.exit(1)


def format_runtime(seconds):
    """Convert seconds to a human-readable format."""
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    minutes, sec = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m {sec}s"
    hours, min_ = divmod(minutes, 60)
    return f"{hours}h {min_}m {sec}s"


def manage_user_logs(username):
    """Ensure only the last N log files are kept for each user."""
    log_dir = WORKING_DIR / "logs"
    log_dir.mkdir(exist_ok=True)
    user_logs = sorted(
        [
            f
            for f in log_dir.iterdir()
            if f.is_file() and f.name.startswith(f"{username}_") and f.suffix == ".log"
        ],
        key=lambda f: f.stat().st_mtime,
    )

    if len(user_logs) > LOGS_PER_USER:
        for old_log in user_logs[:-LOGS_PER_USER]:
            try:
                old_log.unlink()
                if VERBOSE:
                    logging.info(f"Removed old log file: {old_log.name}")
            except Exception as e:
                logging.error(f"Error removing log file {old_log.name}: {e}")


async def run_downloader(username):
    """Run the Node.js downloader script for the given username."""
    command = [
        "node",
        str(WORKING_DIR / "build" / "app.js"),
        username,
        "--output",
        str(WORKING_DIR / "downloads" / username),
        "--format",
        "mkv",
    ]
    try:
        async with running_processes_lock:
            if username in running_processes:
                if VERBOSE:
                    logging.warning(
                        f"Downloader already running for {username}. Skipping."
                    )
                return
            user_status[username] = "Checking"
            start_time = time.time()
            user_runtime[username] = start_time  # Record the start time

            # Define creation flags based on the operating system
            if os.name == "nt":
                # On Windows, use CREATE_NEW_PROCESS_GROUP to allow sending CTRL_BREAK_EVENT
                creationflags = subprocess.CREATE_NEW_PROCESS_GROUP
            else:
                creationflags = 0  # No special flags on Unix

            # Start the downloader process with pipes for stdout and stderr
            process = await asyncio.create_subprocess_exec(
                *command,
                cwd=WORKING_DIR,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                creationflags=creationflags,
                env=os.environ.copy(),  # Pass the current environment
            )

            # Register the running process
            running_processes[username] = process

    except Exception as e:
        user_status[username] = f"Error: {str(e)}"
        logging.error(f"Error in run_downloader for {username}: {e}")
    else:
        # Open log file for writing only if the process started successfully
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = WORKING_DIR / "logs" / f"{username}_{timestamp}.log"

        # Manage existing logs to keep only the last LOGS_PER_USER
        manage_user_logs(username)

        with log_filename.open("a", encoding="utf-8") as log_file:
            # Create tasks to read stdout and stderr
            await asyncio.gather(
                read_stream(process.stdout, log_file, "stdout"),
                read_stream(process.stderr, log_file, "stderr"),
                monitor_process(process, username),
            )

    finally:
        async with running_processes_lock:
            # Remove from running_processes when finished
            running_processes.pop(username, None)
        async with active_tasks_lock:
            ACTIVE_TASKS.discard(username)
        # Remove runtime tracking
        user_runtime.pop(username, None)


async def read_stream(stream, log_file, stream_name="stdout"):
    """Read from stream and write to log file."""
    global logs_buffer
    try:
        while True:
            chunk = await stream.read(1024)
            if not chunk:
                break
            decoded_chunk = chunk.decode("utf-8", errors="replace")
            log_file.write(decoded_chunk)
            log_file.flush()
            log_entry = f"[{stream_name}] {decoded_chunk.strip()}"
            async with logs_lock:
                logs_buffer.append(log_entry)
                if len(logs_buffer) > MAX_LOG_LINES:
                    logs_buffer = logs_buffer[-MAX_LOG_LINES:]
    except Exception as e:
        logging.error(f"Error reading {stream_name}: {e}")


async def monitor_process(process, username):
    """Monitor the process and update status based on its state."""
    threshold = 5  # Threshold in seconds
    try:
        # Wait for the process to exit or timeout
        await asyncio.wait_for(process.wait(), timeout=threshold)
        # Process exited before threshold duration
        exit_code = process.returncode
        if exit_code == 1:
            user_status[username] = "Offline"
        elif exit_code == 0:
            user_status[username] = "Offline"  # Changed from "Completed" to "Offline"
        else:
            user_status[username] = f"Error: Exit code {exit_code}"
    except asyncio.TimeoutError:
        # Process is still running after threshold duration
        user_status[username] = "Running"
        # Wait until the process completes
        await process.wait()
        exit_code = process.returncode
        if exit_code != 0:
            user_status[username] = f"Error: Exit code {exit_code}"
        else:
            user_status[username] = "Offline"  # Changed from "Completed" to "Offline"


async def monitor_user(username):
    """Monitor the username for activity, adjusting the interval based on user status."""
    interval = MIN_INTERVAL

    # Introduce initial random delay to stagger the first check
    initial_offset = random.uniform(0, 10)  # Random delay between 0 and 10 seconds
    await asyncio.sleep(initial_offset)
    if VERBOSE:
        logging.info(
            f"Initial delay of {initial_offset:.2f} seconds for user '{username}'."
        )

    while True:
        async with active_tasks_lock:
            if username not in ACTIVE_TASKS:
                ACTIVE_TASKS.add(username)
                user_status[username] = "Waiting"
                # Start the downloader
                asyncio.create_task(run_downloader(username))

        # Adjust interval based on user status
        current_status = user_status.get(username, "Unknown")
        if current_status in ["Offline"] or current_status.startswith("Error"):
            # Increase interval but cap it
            interval = min(interval + 60, MAX_INTERVAL)
        else:
            # Reset interval if the user was running or waiting
            interval = MIN_INTERVAL

        # Random offset to prevent synchronized requests in subsequent iterations
        random_offset = random.uniform(-5, 5)  # Random offset between -5 and +5 seconds
        await asyncio.sleep(interval + random_offset)


def load_usernames():
    """Load usernames from a file."""
    try:
        with open(USERNAME_FILE, "r", encoding="utf-8") as f:
            usernames = [line.strip() for line in f.readlines() if line.strip()]
        if VERBOSE:
            logging.info(f"Loaded {len(usernames)} usernames.")
        return usernames
    except FileNotFoundError:
        logging.error(f"Username file '{USERNAME_FILE}' not found.")
        return []


async def scheduler(usernames):
    """Centralized scheduler to manage user checks."""
    tasks = []
    for username in usernames:
        if username not in user_tasks:
            user_status[username] = "Initializing"
            task = asyncio.create_task(monitor_user(username))
            user_tasks[username] = task
            tasks.append(task)
    if tasks:
        await asyncio.gather(*tasks)


def build_layout():
    """Builds the Rich layout with Logs and Status Table panels."""
    layout = Layout()

    layout.split(
        Layout(name="logs", ratio=1),
        Layout(name="table", ratio=1),
    )

    return layout


async def build_logs_panel():
    """Builds the Logs panel."""
    async with logs_lock:
        logs_text = "\n".join(logs_buffer[-25:])  # Show last 25 lines
    return Panel(Text(logs_text, style="white"), title="Logs", border_style="green")


def build_status_panel():
    """Builds the Status Table panel."""

    def get_table():
        table = Table(expand=True)  # Allows the table to expand to the console width

        # Add columns without fixed styles for dynamic styling
        table.add_column("Username", no_wrap=True, justify="left")
        table.add_column(
            "Status",
            style="magenta",
            width=25,  # Fixed width for Status column
            overflow="ellipsis",  # Truncate with ellipsis if content exceeds width
            justify="right",
        )
        table.add_column("Runtime", style="green", width=10, justify="left")

        current_time = time.time()
        for username in sorted(user_status.keys()):
            status = user_status[username]
            runtime = "-"
            if (
                status in ["Running", "Checking", "Initializing", "Waiting"]
                and username in user_runtime
            ):
                elapsed = current_time - user_runtime[username]
                runtime = format_runtime(elapsed)

            # Apply dynamic styling to the username
            if status == "Running":
                username_display = f"[green]https://www.tiktok.com/@{username}[/green]"
            elif status == "Offline":
                username_display = f"[blue]https://www.tiktok.com/@{username}[/blue]"
            elif status.startswith("Error"):
                username_display = f"[red]https://www.tiktok.com/@{username}[/red]"
            else:
                username_display = (
                    f"[yellow]https://www.tiktok.com/@{username}[/yellow]"
                )

            table.add_row(username_display, status, runtime)

        return table

    return Panel(get_table(), title="Status", border_style="blue")


async def shutdown(shutdown_signal, loop):
    """Cleanup tasks tied to the service's shutdown."""
    logging.info(f"Received exit signal {shutdown_signal.name}...")

    # Send termination signals to all running subprocesses
    async with running_processes_lock:
        for username, process in running_processes.items():
            if process.returncode is None:
                logging.info(f"Sending termination signal to {username}'s downloader.")
                try:
                    if os.name == "nt":
                        # On Windows, send CTRL_BREAK_EVENT to the process group
                        process.send_signal(signal.CTRL_BREAK_EVENT)
                    else:
                        # On Unix/Linux, send SIGINT
                        process.send_signal(signal.SIGINT)
                except Exception as e:
                    logging.error(
                        f"Error sending termination signal to {username}: {e}"
                    )

    # Allow some time for subprocesses to terminate gracefully
    await asyncio.sleep(5)

    # Force kill any subprocesses that are still running
    async with running_processes_lock:
        for username, process in running_processes.items():
            if process.returncode is None:
                logging.info(f"Force killing {username}'s downloader.")
                try:
                    process.kill()
                except Exception as e:
                    logging.error(f"Error killing {username}'s downloader: {e}")

    # Cancel all user monitoring tasks
    async with active_tasks_lock:
        for username, task in user_tasks.items():
            if not task.done():
                logging.info(f"Cancelling task for user '{username}'.")
                task.cancel()

    # Wait for all tasks to be cancelled
    tasks = [task for task in user_tasks.values()]
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

    # Stop the event loop
    loop.stop()


async def main():
    global semaphore
    # Initialize the semaphore
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)

    # Get the current event loop
    loop = asyncio.get_running_loop()

    # Set up watchdog to monitor the usernames file
    event_handler = UsernameListHandler(on_username_file_change, loop)
    observer = Observer()
    observer.schedule(event_handler, str(WORKING_DIR), recursive=False)
    observer.start()
    logging.info("Started watchdog observer.")

    # Register shutdown signals
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(
                sig, lambda s=sig: asyncio.create_task(shutdown(s, loop))
            )
            logging.info(f"Registered handler for signal: {sig.name}")
        except NotImplementedError:
            # Signals are not implemented on some platforms like Windows for certain loops
            logging.warning(f"Signal {sig.name} not implemented on this platform.")

    # Load initial usernames
    usernames = load_usernames()

    # Start the scheduler
    scheduler_task = asyncio.create_task(scheduler(usernames))

    # Build the initial layout
    layout = build_layout()

    async def live_update():
        with Live(layout, refresh_per_second=1, console=console, screen=True) as live:
            try:
                while True:
                    # Update Logs Panel
                    logs_panel = await build_logs_panel()
                    layout["logs"].update(logs_panel)

                    # Update Status Panel
                    status_panel = build_status_panel()
                    layout["table"].update(status_panel)

                    live.update(layout)
                    await asyncio.sleep(1)
            except Exception as e:
                logging.error(f"Exception in live_update: {e}")
            except asyncio.CancelledError:
                pass
            finally:
                observer.stop()
                observer.join()
                logging.info("Watchdog observer stopped.")

    # Run both the scheduler and live update concurrently
    await asyncio.gather(scheduler_task, live_update())


async def on_username_file_change():
    """Callback to handle changes in the username file."""
    new_usernames = load_usernames()
    current_usernames = set(user_tasks.keys())

    added_usernames = set(new_usernames) - current_usernames
    removed_usernames = current_usernames - set(new_usernames)

    # Handle added usernames
    for username in added_usernames:
        if VERBOSE:
            logging.info(f"Adding new user '{username}' to monitoring.")
        user_status[username] = "Initializing"
        task = asyncio.create_task(monitor_user(username))
        user_tasks[username] = task

    # Handle removed usernames
    for username in removed_usernames:
        if VERBOSE:
            logging.info(f"Removing user '{username}' from monitoring.")
        # Cancel the monitoring task
        task = user_tasks.get(username)
        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                logging.info(f"Task for user '{username}' cancelled.")
        # Terminate any running subprocess
        async with running_processes_lock:
            process = running_processes.get(username)
            if process and process.returncode is None:
                logging.info(f"Terminating subprocess for removed user '{username}'.")
                try:
                    if os.name == "nt":
                        process.send_signal(signal.CTRL_BREAK_EVENT)
                    else:
                        process.send_signal(signal.SIGINT)
                except Exception as e:
                    logging.error(
                        f"Error sending termination signal to {username}: {e}"
                    )
        # Remove from tracking dictionaries
        user_status.pop(username, None)
        user_runtime.pop(username, None)
        user_tasks.pop(username, None)


if __name__ == "__main__":
    # Ensure the script starts in the correct working directory
    os.system("cls" if os.name == "nt" else "clear")
    os.chdir(WORKING_DIR)

    # Create the logs and downloads directories if they don't exist
    (WORKING_DIR / "logs").mkdir(exist_ok=True)
    (WORKING_DIR / "downloads").mkdir(exist_ok=True)

    try:
        check_downloader_script()
        asyncio.run(main())
    except KeyboardInterrupt:
        # Handle Ctrl+C pressed before the shutdown coroutine was registered
        logging.info("KeyboardInterrupt received. Exiting...")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
