# tests/conftest.py
import subprocess
import time
import pytest
import requests
import json
import os

def wait_for_server(url: str, timeout:int=30):
    """Poll the server until it responds or timeout."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            response = requests.get(url)
            if response.status_code < 500:  # Accept any non‑server‑error
                return True
        except requests.ConnectionError:
            pass
        time.sleep(0.5)
    raise RuntimeError(f"Server did not become ready within {timeout}s")

@pytest.fixture(scope="session")
def server_url():
    
    # Optional: write a config file
    config_file = os.path.dirname(__file__)+'/config/master_env.json'
    config = json.loads(config_file)
    
    port = config.MASTER_PORT
    base_url = f"http://{config.MASTER_HOST}:{port}"


    # 2. Start the server process
    # Adjust the command to match how you start your server
    server_cmd = [os.path.dirname(__file__)+'/../build/x86_64-linux/src/master/dc_master', "--config", str(config_file)]
    # If the server logs to stderr, you can capture output for debugging
    server_process = subprocess.Popen(
        server_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    )

    # 3. Wait for readiness
    try:
        wait_for_server(f"{base_url}/health")   # Assuming a health endpoint
    except Exception:
        # If startup fails, kill the process and show its output
        server_process.terminate()
        stdout, _ = server_process.communicate(timeout=5)
        raise RuntimeError(f"Server failed to start:\n{stdout}")

    # 4. Provide the URL to tests
    yield base_url

    # 5. Teardown: kill the server
    server_process.terminate()
    try:
        server_process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        server_process.kill()
        server_process.wait()
