import logging
import asyncio
import os
import time
import signal
import subprocess
import sys
import random
from datetime import datetime
from trio import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from rich.console import Console
from rich.table import Table
from rich.live import Live

# -------------------------------
# Configuration Parameters
# -------------------------------

# Setting up logging for better debug information
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("script.log"),  # Log to a file
        # Uncomment the next line if you also want logs in the console
        # logging.StreamHandler()
    ],
)

USERNAME_FILE = "usernames.txt"  # File containing usernames to monitor
ACTIVE_TASKS = set()  # Track currently running username tasks to avoid duplicates
WORKING_DIR = Path(__file__).parent
MIN_INTERVAL = 60  # Minimum interval of 1 minute
MAX_INTERVAL = 300  # Maximum interval of 5 minutes
LOGS_PER_USER = 3  # Maximum number of log files to keep per user
MAX_CONCURRENT_DOWNLOADS = 3  # Limit concurrent downloads
console = Console()

# Status dictionary for usernames
user_status = {}
# Runtime tracking dictionary
user_runtime = {}

# Registry to keep track of running subprocesses
running_processes = {}  # username: subprocess

semaphore = None  # Will be initialized in main()


class UsernameListHandler(FileSystemEventHandler):
    """Handles the file modification events to reload username list."""

    def __init__(self, callback):
        super().__init__()
        self.callback = callback

    def on_modified(self, event):
        if event.src_path.endswith(USERNAME_FILE):
            logging.info("Username file updated; reloading usernames.")
            self.callback()


def check_downloader_script():
    """Check if the Node.js downloader script exists."""
    downloader_path = os.path.join(WORKING_DIR, "build", "app.js")
    if not os.path.isfile(downloader_path):
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
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    user_logs = [
        f
        for f in os.listdir(log_dir)
        if f.startswith(f"{username}_") and f.endswith(".log")
    ]

    # Extract timestamp and sort
    def extract_timestamp(log_filename):
        try:
            timestamp_str = log_filename[
                len(username) + 1 : -4
            ]  # Remove 'username_' and '.log'
            return datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")
        except ValueError:
            return datetime.min  # Place improperly formatted logs at the beginning

    user_logs_sorted = sorted(user_logs, key=extract_timestamp)

    if len(user_logs_sorted) > LOGS_PER_USER:
        for old_log in user_logs_sorted[:-LOGS_PER_USER]:
            try:
                os.remove(os.path.join(log_dir, old_log))
                logging.info(f"Removed old log file: {old_log}")
            except Exception as e:
                logging.error(f"Error removing log file {old_log}: {e}")


async def run_downloader(username):
    """Run the Node.js downloader script for the given username."""
    command = [
        "node",
        "build/app.js",
        username,
        "--output",
        f"downloads/{username}",
        "--format",
        "mp4",
    ]
    try:
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
        )

        # Register the running process
        running_processes[username] = process

        # Open log file for writing
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = os.path.join("logs", f"{username}_{timestamp}.log")
        os.makedirs(os.path.dirname(log_filename), exist_ok=True)

        # Manage existing logs to keep only the last LOGS_PER_USER
        manage_user_logs(username)

        with open(log_filename, "a", buffering=1, encoding="utf-8") as log_file:
            # Create tasks to read stdout and stderr
            await asyncio.gather(
                read_stream(process.stdout, log_file),
                read_stream(process.stderr, log_file),
                monitor_process(process, username),
            )

    except Exception as e:
        user_status[username] = f"Error: {str(e)}"
        logging.error(f"Error in run_downloader for {username}: {e}")
    finally:
        # Remove from ACTIVE_TASKS when finished
        ACTIVE_TASKS.discard(username)
        # Remove runtime tracking
        user_runtime.pop(username, None)
        # Unregister the running process
        running_processes.pop(username, None)


async def read_stream(stream, log_file):
    """Read from stream and write to log file."""
    while True:
        line = await stream.readline()
        if not line:
            break
        log_file.write(line.decode("utf-8"))
        log_file.flush()


async def monitor_process(process, username):
    """Monitor the process and update status based on its state."""
    threshold = 5  # Threshold in seconds
    try:
        # Wait for the process to exit or timeout
        await asyncio.wait_for(process.wait(), timeout=threshold)
        # Process exited before threshold duration
        exit_code = process.returncode
        if exit_code == 1 or exit_code == 0:
            user_status[username] = "Offline"
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
            user_status[username] = "Offline"


async def monitor_user(username):
    """Monitor the username for activity, adjusting the interval based on user status."""
    interval = MIN_INTERVAL

    # Introduce initial random delay to stagger the first check
    initial_offset = random.uniform(0, 10)  # Random delay between 0 and 10 seconds
    await asyncio.sleep(initial_offset)
    logging.info(
        f"Initial delay of {initial_offset:.2f} seconds for user '{username}'."
    )

    while True:
        async with semaphore:
            if username not in ACTIVE_TASKS:
                ACTIVE_TASKS.add(username)
                user_status[username] = "Waiting"
                await run_downloader(username)

                # Adjust interval based on user status
                if user_status[username] == "Offline":
                    # Increase interval but cap it
                    interval = min(interval + 60, MAX_INTERVAL)
                else:
                    # Reset interval if the user was live or an error occurred
                    interval = MIN_INTERVAL

                ACTIVE_TASKS.discard(username)

        # Random offset to prevent synchronized requests in subsequent iterations
        random_offset = random.uniform(-5, 5)  # Random offset between -5 and +5 seconds
        await asyncio.sleep(interval + random_offset)


def load_usernames():
    """Load usernames from a file."""
    try:
        with open(USERNAME_FILE, "r") as f:
            usernames = [line.strip() for line in f.readlines() if line.strip()]
        return usernames
    except FileNotFoundError:
        logging.error(f"Username file '{USERNAME_FILE}' not found.")
        return []


async def scheduler(usernames):
    """Centralized scheduler to manage user checks."""
    tasks = []
    for username in usernames:
        user_status[username] = "Initializing"
        task = asyncio.create_task(monitor_user(username))
        tasks.append(task)
    await asyncio.gather(*tasks)


def build_status_table():
    """Builds a status table for console display."""
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
    table.add_column("Runtime", style="green", width=10, justify="right")

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
            username_display = f"[green]{username}[/green]"
        else:
            username_display = f"[red]{username}[/red]"

        table.add_row(username_display, status, runtime)

    return table


async def shutdown(shutdown_signal, loop):
    """Cleanup tasks tied to the service's shutdown."""
    logging.info(f"Received exit signal {shutdown_signal.name}...")

    # Send termination signals to all running subprocesses
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
                logging.error(f"Error sending termination signal to {username}: {e}")

    # Allow some time for subprocesses to terminate gracefully
    await asyncio.sleep(2)

    # Force kill any subprocesses that are still running
    for username, process in running_processes.items():
        if process.returncode is None:
            logging.info(f"Force killing {username}'s downloader.")
            try:
                process.kill()
            except Exception as e:
                logging.error(f"Error killing {username}'s downloader: {e}")

    # Cancel all tasks
    tasks = [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]
    list(map(lambda task: task.cancel(), tasks))

    logging.info("Cancelling outstanding tasks")
    await asyncio.gather(*tasks, return_exceptions=True)
    loop.stop()


async def main():
    # Set up watchdog to monitor the usernames file
    loop = asyncio.get_event_loop()
    event_handler = UsernameListHandler(lambda: None)  # Placeholder
    observer = Observer()
    observer.schedule(event_handler, ".", recursive=False)
    observer.start()

    # Register shutdown signals
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(
                sig, lambda s=sig: asyncio.create_task(shutdown(s, loop))
            )
        except NotImplementedError:
            # Signals are not implemented on Windows for some cases like ProactorEventLoop
            logging.warning(f"Signal {sig} not implemented on this platform.")

    # Load usernames
    usernames = load_usernames()

    # Start the semaphore
    global semaphore
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)

    # Start the scheduler
    scheduler_task = asyncio.create_task(scheduler(usernames))

    async def live_update():
        with Live(
            build_status_table(), refresh_per_second=1, console=console, screen=False
        ) as live:
            try:
                while True:
                    # Update the live table
                    live.update(build_status_table())
                    await asyncio.sleep(1)
            except asyncio.CancelledError:
                pass
            finally:
                observer.stop()
                observer.join()

    # Run both the scheduler and live update concurrently
    await asyncio.gather(scheduler_task, live_update())


if __name__ == "__main__":
    # Ensure the script starts in the correct working directory
    os.system("cls" if os.name == "nt" else "clear")
    os.chdir(WORKING_DIR)

    # Create the logs directory if it doesn't exist
    if not os.path.exists("logs"):
        os.makedirs("logs")

    try:
        check_downloader_script()
        asyncio.run(main())
    except KeyboardInterrupt:
        # Handle Ctrl+C pressed before the shutdown coroutine was registered
        logging.info("KeyboardInterrupt received. Exiting...")
