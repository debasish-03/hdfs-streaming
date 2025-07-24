import requests
import os
from urllib.parse import urlparse, urlunparse

WEBHDFS_HOST = "localhost"
WEBHDFS_PORT = 9870
HDFS_USER = "root"
HDFS_PATH = "/debasish/data/uploaded_file.txt"
LOCAL_FILE = "test.txt"

def upload_file_to_hdfs():
    # Step 1: Initiate file creation to get redirect URL
    create_url = (
        f"http://{WEBHDFS_HOST}:{WEBHDFS_PORT}/webhdfs/v1{HDFS_PATH}"
        f"?op=CREATE&user.name={HDFS_USER}&overwrite=true"
    )

    print(f"[+] Initiating file creation at {HDFS_PATH}")
    response = requests.put(create_url, allow_redirects=False)

    if response.status_code != 307:
        raise Exception(f"Failed to initiate file creation: {response.text}")


    redirect_url = response.headers["Location"]

    # Parse and override hostname to localhost
    parsed = urlparse(redirect_url)
    redirect_url = urlunparse(parsed._replace(netloc=f"localhost:{parsed.port}"))
    print(f"[+] Redirect URL for upload: {redirect_url}")

    # Step 2: Upload the file using redirect URL
    with open(LOCAL_FILE, "rb") as file_data:
        print(f"[+] Uploading {LOCAL_FILE} to HDFS...")
        upload_response = requests.put(redirect_url, data=file_data)

        if upload_response.status_code != 201:
            raise Exception(f"Upload failed: {upload_response.text}")

    print("File successfully uploaded to HDFS at:", HDFS_PATH)

if __name__ == "__main__":
    if not os.path.exists(LOCAL_FILE):
        print(f""Local file does not exist: {LOCAL_FILE}")
    else:
        upload_file_to_hdfs()
