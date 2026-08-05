import os

DATA_DIR = "/app/data"

def load_binary_files():
    if not os.path.exists(DATA_DIR):
        return Exception("Error: Directory '{DATA_DIR}' does not exist.")

    files = [
        f for f in os.listdir(DATA_DIR)
        if os.path.isfile(os.path.join(DATA_DIR, f)) and f.endswith(".bin")
    ]

    if not files:
        return Exception("No bin files found in '{DATA_DIR}'.")

    files.sort(key=lambda f: os.path.getsize(os.path.join(DATA_DIR, f)))

    print(f"Found {len(files)} file(s).\n")
    return files