import os
import shutil
import hashlib
import time
import logging
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Directories
DIRS = {
    "files": "files/",
    "error": "error/",
    "backup": "backup/",
    "hadoop": "hadoop/"
}

# Semaphore values
STATUS = {
    0: "ERROR",
    1: "RUNNING",
    2: "SUCCESS"
}

# Logging setup
LOG_FILE = "autofilebot.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def log_event(message, level="info"):
    if level == "error":
        logging.error(message)
    elif level == "warning":
        logging.warning(message)
    else:
        logging.info(message)

def get_file_hash(filepath):
    with open(filepath, 'rb') as f:
        return hashlib.md5(f.read()).hexdigest()

def detect_duplicates(directory):
    seen = {}
    duplicates = []
    for fname in os.listdir(directory):
        fpath = os.path.join(directory, fname)
        if os.path.isfile(fpath):
            fhash = get_file_hash(fpath)
            if fhash in seen:
                duplicates.append(fpath)
            else:
                seen[fhash] = fpath
    return duplicates

def move_files(src, dst):
    for fname in os.listdir(src):
        src_path = os.path.join(src, fname)
        dst_path = os.path.join(dst, fname)
        try:
            shutil.move(src_path, dst_path)
            log_event(f"Moved '{fname}' from '{src}' to '{dst}'")
        except Exception as e:
            log_event(f"Failed to move '{fname}' from '{src}' to '{dst}': {e}", level="error")

def process_directory(path, next_path, min_files=1):
    files = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
    if len(files) >= min_files:
        msg = f"[{STATUS[1]}] {path} has {len(files)} files. Moving to {next_path}"
        print(msg)
        log_event(msg)
        move_files(path, next_path)
        return True
    else:
        log_event(f"[INFO] {path} has {len(files)} file(s). No action taken.")
    return False

def agent_runner():
    try:
        
        # Handle Duplicates in 'files'
        duplicates = detect_duplicates(DIRS["files"])
        for dup in duplicates:
            msg = f"[{STATUS[0]}] Duplicate detected: {dup}"
            print(msg)
            log_event(msg, level="warning")
            shutil.move(dup, os.path.join(DIRS["error"], os.path.basename(dup)))
            log_event(f"Moved duplicate '{os.path.basename(dup)}' to 'error/'")

        signal = 2  # Default to success

        # Step-by-step: if any stage has overflow, move forward
        if process_directory(DIRS["files"], DIRS["error"], 1):
            signal = 1
        if process_directory(DIRS["error"], DIRS["backup"], 1):
            signal = 1
        if process_directory(DIRS["backup"], DIRS["hadoop"], 1):
            signal = 2

        signal_msg = f"[SEMAPHORE SIGNAL] {signal} = {STATUS[signal]}"
        print(signal_msg)
        log_event(signal_msg)

    except Exception as e:
        err_msg = f"[{STATUS[0]}] Unexpected error: {str(e)}"
        print(err_msg)
        log_event(err_msg, level="error")

# Watchdog Event Handler
class FileEventHandler(FileSystemEventHandler):
    def on_modified(self, event):
        if event.is_directory:
            return
        log_event(f"[WATCHDOG] File modified: {event.src_path}")
        agent_runner()

    def on_created(self, event):
        if event.is_directory:
            return
        log_event(f"[WATCHDOG] File created: {event.src_path}")
        agent_runner()

# Start Watchdog for multiple directories
if __name__ == "__main__":
    observer = Observer()
    event_handler = FileEventHandler()

    for d in ["files", "error", "backup"]:
        full_path = DIRS[d]
        observer.schedule(event_handler, path=full_path, recursive=False)

    observer.start()
    print("[INFO] Watching for changes in files/, error/, and backup/ ...")
    log_event("[INFO] Watchdog started. Monitoring directories: files/, error/, backup/")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        log_event("[INFO] Watchdog stopped by user (KeyboardInterrupt)")
    observer.join()
