#!/usr/bin/env python3
"""Exercise the computer plugin against a disposable Xvfb desktop."""

import base64
import json
import os
import select
import subprocess
import sys
import time


plugin = os.environ.get("KRABB_COMPUTER_PLUGIN")

if not plugin:
    raise SystemExit("Set KRABB_COMPUTER_PLUGIN to the built plugin executable.")

application = """
import tkinter as tk

root = tk.Tk()
root.title("ready")
root.geometry("640x400+0+0")
tk.Button(root, text="Click", command=lambda: root.title("clicked")).place(
    x=20, y=20, width=120, height=40
)
entry = tk.Entry(root)
entry.place(x=20, y=100, width=400, height=40)
entry.bind("<KeyRelease>", lambda _event: root.title("typed"))
root.mainloop()
"""

display = f":{100 + os.getpid() % 30000}"
environment = {
    **os.environ,
    "KRABB_COMPUTER_MODE": "isolated",
    "KRABB_COMPUTER_DISPLAY": display,
    "KRABB_COMPUTER_STARTUP": sys.executable,
    "KRABB_COMPUTER_STARTUP_ARGS": json.dumps(["-c", application]),
}

process = subprocess.Popen(
    [plugin], stdin=subprocess.PIPE, stdout=subprocess.PIPE, env=environment, text=True, bufsize=1
)

next_id = 1
session_id = None


def call(method, arguments=None, host=True):
    global next_id

    request_id = next_id
    next_id += 1
    params = {"arguments": arguments or {}}

    if host:
        params["host"] = {"session": "computer-integration", "turn": "integration-turn"}

    process.stdin.write(
        json.dumps({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params})
        + "\n"
    )

    process.stdin.flush()
    deadline = time.monotonic() + 10

    while time.monotonic() < deadline:
        ready, _, _ = select.select([process.stdout], [], [], max(0, deadline - time.monotonic()))

        if not ready:
            break

        response = json.loads(process.stdout.readline())

        if response.get("id") != request_id:
            continue

        if "error" in response:
            raise RuntimeError(
                f"{method} failed: {response['error'].get('message', response['error'])}"
            )

        return response.get("result", {})

    raise TimeoutError(f"Timed out waiting for {method}.")


def wait_for_title(title):
    deadline = time.monotonic() + 5

    while time.monotonic() < deadline:
        windows = call("computer_windows", {"session_id": session_id}).get("windows", [])

        if any(window["title"] == title for window in windows):
            return

        time.sleep(0.1)

    raise AssertionError(f"The isolated application did not reach window title {title!r}.")


try:
    call("hello", host=False)
    opened = call("computer_open")
    session_id = opened["session_id"]
    dimensions = call("computer_displays", {"session_id": session_id})
    assert (dimensions["width"], dimensions["height"]) == (1280, 720)
    wait_for_title("ready")

    screenshot = call("computer_screenshot", {"session_id": session_id})
    image_uri = screenshot["content"][1]["uri"]
    assert image_uri.startswith("data:image/png;base64,")
    image = base64.b64decode(image_uri.split(",", 1)[1], validate=True)
    assert image.startswith(b"\x89PNG\r\n\x1a\n")
    assert screenshot["metadata"]["width"] == 1280

    call("computer_click", {"session_id": session_id, "x": 60, "y": 40})
    wait_for_title("clicked")
    call("computer_click", {"session_id": session_id, "x": 80, "y": 120})
    call("computer_type", {"session_id": session_id, "text": "Krabb desktop fixture"})
    wait_for_title("typed")

    call("computer_close", {"session_id": session_id})
    session_id = None
    print("Disposable Xvfb computer integration passed.")
finally:
    if session_id:
        try:
            call("computer_close", {"session_id": session_id})
        except Exception:
            pass

    if process.poll() is None:
        try:
            call("shutdown", host=False)
            process.wait(timeout=5)
        except Exception:
            process.terminate()
            process.wait(timeout=5)
