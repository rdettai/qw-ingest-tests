import datetime
import os
import requests
import shutil
import signal
import subprocess
import sys
import threading
import time
from scenarii.scenario1 import (
    ingest_documents,
    LogResults,
    create_indexes,
)

os.environ["NO_COLOR"] = "true"


def start_quickwit(binary_path: str):
    process = subprocess.Popen(
        [binary_path, "run", "--config", "quickwit.yaml"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd=".",
    )
    return process


def wait_quickwit_ready(url: str, timeout_sec: int, interval_sec: int):
    print("Waiting for Quickwit to be ready...")
    start_time = time.time()
    last_error = "no error yet"
    while True:
        try:
            response = requests.get(f"{url}/health/readyz")
            if response.status_code == 200 and response.json():
                time.sleep(1)  # Wait a bit more to ensure Quickwit is fully ready.
                print("Quickwit is ready")
                return
            last_error = f"Error: {response.status_code}"
        except requests.RequestException as e:
            last_error = f"Error checking health: {e}"

        if time.time() - start_time > timeout_sec:
            print(last_error)
            raise TimeoutError(
                "Quickwit did not become ready within the timeout period."
            )

        time.sleep(interval_sec)


def monitor_stderr(process: subprocess.Popen, results: LogResults):
    """Monitor the stderr output for lines containing 'warn' and count them."""
    warn_count = 0
    current_time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    file_name = f"logs/{current_time}.log"
    results.log_file = file_name
    with open(file_name, "w") as f:
        for line in iter(process.stdout.readline, ""):
            f.write(line)
            results.on_new_log_line(line)
    return warn_count


def main(binary_path, url):
    quickwit_process = start_quickwit(binary_path)

    def signal_handler(sig, frame):
        print("Received SIGINT, terminating Quickwit.")
        quickwit_process.terminate()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)

    log_results = LogResults()
    stderr_thread = threading.Thread(
        target=monitor_stderr, args=(quickwit_process, log_results)
    )
    stderr_thread.start()

    try:
        wait_quickwit_ready(url, 15, 1)

        create_indexes(url)

        ingest_documents(url)
    except Exception as e:
        print("An error occurred", e)

    print("terminating Quickwit process...")
    quickwit_process.terminate()
    try:
        stderr_thread.join()
    except:
        print("joining stderr thread failed, killing QW process")
        quickwit_process.kill()
        raise
    log_results.print()


def cleanup_datadir():
    datadir = "./qwdata"
    if os.path.exists(datadir):
        shutil.rmtree(datadir)
    os.mkdir(datadir)


if __name__ == "__main__":
    cleanup_datadir()
    os.makedirs("./logs", exist_ok=True)

    binary_path = "/home/remi/workspace/quickwit/quickwit/target/release/quickwit"
    url = "http://localhost:7280"

    main(binary_path, url)
