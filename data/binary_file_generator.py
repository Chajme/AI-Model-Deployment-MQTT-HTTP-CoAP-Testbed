import os

def generate_model_file_kb(filename, size_kb):
    if file_exists(filename):
        return

    with open(filename, 'wb') as f:
        f.write(os.urandom(size_kb * 1024))
    print(f"Súbor {filename} ({size_kb} KB) bol vygenerovaný.")

def generate_model_file_mb(filename, size_mb):
    if file_exists(filename):
        return

    with open(filename, 'wb') as f:
        f.write(os.urandom(size_mb * 1024 * 1024))
    print(f"Súbor {filename} ({size_mb} MB) bol vygenerovaný.")

def file_exists(filename):
    if os.path.exists(filename):
        print(f"{filename} already exists, skipping.")
        return True
    return False

file_sizes_kb = [250]
file_sizes_mb = [1, 5, 20, 50]

for size_kb in file_sizes_kb:
    generate_model_file_kb(f"binary_file_{size_kb}.bin", size_kb)

for size_mb in file_sizes_mb:
    generate_model_file_mb(f"binary_file_{size_mb}.bin", size_mb)

