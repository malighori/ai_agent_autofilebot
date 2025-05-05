import os
import shutil
import time
import hashlib

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
        shutil.move(os.path.join(src, fname), os.path.join(dst, fname))

def process_directory(path, next_path, threshold):
    files = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
    
    if len(files) > threshold:
        print(f"[{STATUS[1]}] {path} exceeded threshold. Moving files to {next_path}")
        move_files(path, next_path)
        return True
    return False

def agent_runner():
    try:
        # Handle Duplicates
        duplicates = detect_duplicates(DIRS["files"])
        for dup in duplicates:
            print(f"[{STATUS[0]}] Duplicate detected: {dup}")
            shutil.move(dup, os.path.join(DIRS["error"], os.path.basename(dup)))

        # Stage 1: Files → Error
        if process_directory(DIRS["files"], DIRS["error"], 2):
            signal = 1
        else:
            signal = 2

        # Stage 2: Error → Backup
        if process_directory(DIRS["error"], DIRS["backup"], 2):
            signal = 1

        # Stage 3: Backup → Hadoop
        if process_directory(DIRS["backup"], DIRS["hadoop"], 2):
            signal = 2

        print(f"[SEMAPHORE SIGNAL] {signal} = {STATUS[signal]}")

    except Exception as e:
        print(f"[{STATUS[0]}] Unexpected error: {str(e)}")

# Run the agent
# agent_runner()
while True:
    agent_runner()
    time.sleep(10) 