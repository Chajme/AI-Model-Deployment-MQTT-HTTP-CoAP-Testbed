from flask import Flask, request, Response
import hashlib
import os

app = Flask(__name__)

OUTPUT_DIR = "/app/output"
os.makedirs(OUTPUT_DIR, exist_ok=True)


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@app.route("/upload/<filename>", methods=["PUT"])
def upload(filename):
    data = request.data

    expected_checksum = request.headers.get("X-Checksum")
    actual_checksum = sha256(data)

    # 🔒 Integrity check happens HERE (server-side)
    if expected_checksum and expected_checksum != actual_checksum:
        print(f"[HTTP] CHECKSUM FAIL: {filename}")
        return Response("Checksum mismatch", status=400)

    filepath = os.path.join(OUTPUT_DIR, filename)

    with open(filepath, "wb") as f:
        f.write(data)

    print(f"[HTTP] OK: {filename} ({len(data)} bytes)")
    return Response("OK", status=200)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)