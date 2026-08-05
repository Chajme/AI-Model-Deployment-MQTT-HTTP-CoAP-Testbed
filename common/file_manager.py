import os

DATA_DIR = "/app/data"
OUTPUT_DIR = "/app/output"

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

def save_file(filename, data):
    output_directory_exists()

    filepath = os.path.join(OUTPUT_DIR, filename)

    with open(filepath, "wb") as f:
        f.write(data)

    print(f"Saved '{filename}' successfully.")

def get_file_path(filename):
    return os.path.join(OUTPUT_DIR, filename)

def output_directory_exists():
    if not os.path.exists(DATA_DIR):
        os.makedirs(OUTPUT_DIR, exist_ok=True)