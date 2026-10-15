"""Capture genuine screenshots of the GUI during real transfers.

Runs gui.App, performs an actual transfer with simulated loss, and grabs
the real window contents with PIL.ImageGrab at three moments:

    1. receiver tab, listening
    2. sender tab, mid-transfer under 20% simulated loss (progress visible)
    3. sender tab, completed with SHA-256 verification shown

Usage:  python capture_screens.py
Requires: pillow  (pip install pillow)
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import gui

SCREENSHOTS = Path(__file__).resolve().parent / "screenshots"


def grab_window(app, name: str) -> None:
    """Capture the exact window rectangle from the screen."""
    try:
        from PIL import ImageGrab
    except ImportError:
        print("pillow not installed - cannot capture screenshots",
              file=sys.stderr)
        raise SystemExit(2)
    app.update_idletasks()
    app.update()
    x, y = app.winfo_rootx(), app.winfo_rooty()
    w, h = app.winfo_width(), app.winfo_height()
    # window content area: skip the title bar (approx 32 px on Windows)
    bbox = (x, y + 32, x + w, y + h)
    img = ImageGrab.grab(bbox=bbox)
    SCREENSHOTS.mkdir(exist_ok=True)
    out = SCREENSHOTS / name
    img.save(out)
    print(f"saved {out}")


def main() -> int:
    app = gui.App()
    app.geometry("980x640+60+40")
    app.update_idletasks()
    app.update()

    # --- 1. receiver tab listening --------------------------------------
    app._start_receiver()
    for _ in range(10):
        app.update(); time.sleep(0.05)
    grab_window(app, "gui_receiver_listening.png")

    # --- 2. sender mid-transfer under loss -------------------------------
    app.notebook.select(0)           # sender tab
    app.var_port.set(app._receiver.bound_port)
    app.var_file.set("test_files/large.bin")
    app.var_loss.set(20.0)          # 20% simulated data loss
    app.var_seed.set(7)
    app._start_send()
    # pump until the progress bar is visibly part-way, then grab
    deadline = time.time() + 60
    grabbed = False
    while time.time() < deadline:
        app.update()
        try:
            if 5.0 <= float(app.progress["value"]) <= 80.0:
                time.sleep(0.05); app.update()
                grab_window(app, "gui_sender_transfer_loss.png")
                grabbed = True
                break
        except Exception:
            pass
        time.sleep(0.02)
    if not grabbed:                             # transfer too fast? still capture
        grab_window(app, "gui_sender_transfer_loss.png")

    # --- 3. completed transfer -------------------------------------------
    deadline = time.time() + 180
    while time.time() < deadline:
        app.update()
        if "Success" in app.var_status.get() or "FAILED" in app.var_status.get():
            break
        time.sleep(0.05)
    grab_window(app, "gui_sender_complete.png")

    app._on_close()
    print("done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
