"""Tkinter GUI for the Reliable UDP-Based File Transfer System.

Design notes (viva-friendly):
    * Two tabs: Sender and Receiver - the same two roles as the CLI tools.
    * All network work runs in background threads; the GUI thread only
      reads events from a queue via ``after()`` polling, so the window
      never freezes during a transfer.
    * Progress, bytes ACKed, live speed, retransmissions and the final
      SHA-256 verdict are all driven by the same callbacks the tests use.

Run:  python gui.py
"""

from __future__ import annotations

import queue
import threading
import time
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from pathlib import Path

import config
from network_simulator import SimSettings
from receiver import Receiver
from sender import MODE_STOP_AND_WAIT, MODE_WINDOW, send_file

POLL_MS = 100          # GUI event-loop poll interval


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Reliable UDP File Transfer")
        self.minsize(720, 520)
        self._events: queue.Queue = queue.Queue()
        self._receiver: Receiver | None = None
        self._recv_thread: threading.Thread | None = None
        self._sender_busy = False
        self._t0 = 0.0
        self._bytes_ack = 0

        notebook = ttk.Notebook(self)
        notebook.pack(fill="both", expand=True, padx=8, pady=8)
        self.notebook = notebook        # exposed for scripts/tests
        self._build_sender_tab(notebook)
        self._build_receiver_tab(notebook)
        self.protocol("WM_DELETE_WINDOW", self._on_close)
        self.after(POLL_MS, self._poll_events)

    # ================================================================ sender
    def _build_sender_tab(self, notebook: ttk.Notebook) -> None:
        tab = ttk.Frame(notebook, padding=10)
        notebook.add(tab, text="Sender")

        # --- connection row ------------------------------------------------
        row = ttk.Frame(tab); row.grid(row=0, column=0, sticky="ew", pady=3)
        ttk.Label(row, text="Receiver IP:").pack(side="left")
        self.var_host = tk.StringVar(value=config.DEFAULT_HOST)
        ttk.Entry(row, textvariable=self.var_host, width=14).pack(side="left",
                                                                  padx=4)
        ttk.Label(row, text="Port:").pack(side="left", padx=(10, 0))
        self.var_port = tk.IntVar(value=config.DEFAULT_PORT)
        ttk.Spinbox(row, from_=1, to=65535, textvariable=self.var_port,
                    width=7).pack(side="left", padx=4)
        ttk.Label(row, text="Mode:").pack(side="left", padx=(10, 0))
        self.var_mode = tk.StringVar(value=MODE_WINDOW)
        mode_box = ttk.Combobox(row, textvariable=self.var_mode, width=16,
                                state="readonly", values=[
                                    "Stop-and-wait", "Sliding window (GBN)"])
        mode_box.pack(side="left", padx=4)
        mode_box.bind("<<ComboboxSelected>>", self._mode_changed)

        # --- file row -------------------------------------------------------
        row = ttk.Frame(tab); row.grid(row=1, column=0, sticky="ew", pady=3)
        ttk.Label(row, text="File:").pack(side="left")
        self.var_file = tk.StringVar()
        ttk.Entry(row, textvariable=self.var_file).pack(side="left",
                                                        fill="x",
                                                        expand=True, padx=4)
        ttk.Button(row, text="Browse...", command=self._pick_file).pack(
            side="left")

        # --- simulation row ---------------------------------------------------
        row = ttk.LabelFrame(tab, text="Network simulation (outgoing packets)",
                             padding=6)
        row.grid(row=2, column=0, sticky="ew", pady=6)
        ttk.Label(row, text="Data loss %:").grid(row=0, column=0)
        self.var_loss = tk.DoubleVar(value=0.0)
        ttk.Spinbox(row, from_=0, to=90, increment=1, width=6,
                    textvariable=self.var_loss).grid(row=0, column=1,
                                                     padx=(2, 12))
        ttk.Label(row, text="Max delay ms:").grid(row=0, column=2)
        self.var_delay = tk.DoubleVar(value=0.0)
        ttk.Spinbox(row, from_=0, to=1000, increment=10, width=7,
                    textvariable=self.var_delay).grid(row=0, column=3,
                                                      padx=(2, 12))
        ttk.Label(row, text="Reorder %:").grid(row=0, column=4)
        self.var_reorder = tk.DoubleVar(value=0.0)
        ttk.Spinbox(row, from_=0, to=90, increment=1, width=6,
                    textvariable=self.var_reorder).grid(row=0, column=5,
                                                        padx=(2, 12))
        ttk.Label(row, text="Seed:").grid(row=0, column=6)
        self.var_seed = tk.IntVar(value=0)
        ttk.Spinbox(row, from_=0, to=999999, width=8,
                    textvariable=self.var_seed).grid(row=0, column=7,
                                                     padx=2)

        # --- start button + progress ------------------------------------------
        row = ttk.Frame(tab); row.grid(row=3, column=0, sticky="ew", pady=6)
        self.btn_send = ttk.Button(row, text="Start transfer",
                                   command=self._start_send)
        self.btn_send.pack(side="top", pady=2)
        self.progress = ttk.Progressbar(tab, maximum=100)
        self.progress.grid(row=4, column=0, sticky="ew", pady=4)

        # --- live stats ---------------------------------------------------------
        stats = ttk.LabelFrame(tab, text="Transfer status", padding=6)
        stats.grid(row=5, column=0, sticky="ew", pady=4)
        self.var_status = tk.StringVar(value="Idle")
        self.var_speed = tk.StringVar(value="-")
        self.var_retx = tk.StringVar(value="0")
        self.var_bytes = tk.StringVar(value="0 / 0 B")
        self.var_sha = tk.StringVar(value="-")
        for col, (label, var) in enumerate([
                ("Status:", self.var_status), ("Speed:", self.var_speed),
                ("Retransmissions:", self.var_retx),
                ("Bytes ACKed:", self.var_bytes),
                ("SHA-256:", self.var_sha)]):
            ttk.Label(stats, text=label).grid(row=col, column=0,
                                              sticky="w", pady=1)
            ttk.Label(stats, textvariable=var, width=46, anchor="w").grid(
                row=col, column=1, sticky="w")

        # --- log ----------------------------------------------------------
        log_box = ttk.LabelFrame(tab, text="Log", padding=4)
        log_box.grid(row=6, column=0, sticky="nsew", pady=4)
        self.send_log = tk.Text(log_box, height=9, state="disabled",
                                font=("Consolas", 9))
        scroll = ttk.Scrollbar(log_box, command=self.send_log.yview)
        self.send_log.configure(yscrollcommand=scroll.set)
        self.send_log.pack(side="left", fill="both", expand=True)
        scroll.pack(side="right", fill="y")

        tab.columnconfigure(0, weight=1)
        tab.rowconfigure(6, weight=1)

    def _mode_changed(self, _event=None) -> None:
        pass    # mode is read from the combobox when Start is pressed

    def _pick_file(self) -> None:
        chosen = filedialog.askopenfilename(
            title="Choose a file to send",
            initialdir=str(config.PROJECT_ROOT))
        if chosen:
            self.var_file.set(chosen)

    def _start_send(self) -> None:
        if self._sender_busy:
            messagebox.showinfo("Busy", "A transfer is already running.")
            return
        path = self.var_file.get().strip()
        if not path:
            messagebox.showwarning("No file", "Choose a file to send first.")
            return
        try:
            port = int(self.var_port.get())
            host = self.var_host.get().strip() or config.DEFAULT_HOST
        except (ValueError, tk.TclError):
            messagebox.showerror("Bad input", "IP/port values are invalid.")
            return
        mode = (MODE_STOP_AND_WAIT if "Stop" in self.var_mode.get()
                else MODE_WINDOW)

        seed = self.var_seed.get() or None
        sim = SimSettings(
            data_loss_pct=float(self.var_loss.get()),
            delay_ms=float(self.var_delay.get()),
            reorder_pct=float(self.var_reorder.get()),
            seed=seed)
        if not sim.enabled():
            sim = None

        self._sender_busy = True
        self.btn_send.state(["disabled"])
        self.var_status.set("Sending...")
        self.var_sha.set("-")
        self._t0 = time.monotonic()
        self._bytes_ack = 0

        threading.Thread(target=self._send_worker,
                         args=(path, host, port, mode, sim),
                         daemon=True).start()

    def _send_worker(self, path, host, port, mode, sim) -> None:
        try:
            result = send_file(path, host, port, mode=mode, sim=sim,
                               progress=self._events.put)
        except Exception as exc:            # GUI must never die silently
            self._events.put({"event": "error", "error": f"crash: {exc}"})
            return
        self._events.put({"event": "done", "result": result})

    # ============================================================== receiver
    def _build_receiver_tab(self, notebook: ttk.Notebook) -> None:
        tab = ttk.Frame(notebook, padding=10)
        notebook.add(tab, text="Receiver")

        row = ttk.Frame(tab); row.grid(row=0, column=0, sticky="ew", pady=3)
        ttk.Label(row, text="Listen on port:").pack(side="left")
        self.var_rport = tk.IntVar(value=config.DEFAULT_PORT)
        ttk.Spinbox(row, from_=1, to=65535, textvariable=self.var_rport,
                    width=7).pack(side="left", padx=4)
        ttk.Button(row, text="Start listening",
                   command=self._start_receiver).pack(side="left", padx=8)
        self.btn_stop_recv = ttk.Button(row, text="Stop",
                                        command=self._stop_receiver,
                                        state="disabled")
        self.btn_stop_recv.pack(side="left")

        row = ttk.Frame(tab); row.grid(row=1, column=0, sticky="ew", pady=3)
        ttk.Label(row, text="Save into:").pack(side="left")
        self.var_outdir = tk.StringVar(value=str(
            (config.PROJECT_ROOT / config.RECEIVER_DIR).resolve()))
        ttk.Entry(row, textvariable=self.var_outdir).pack(side="left",
                                                          fill="x",
                                                          expand=True, padx=4)
        ttk.Button(row, text="Browse...",
                   command=self._pick_outdir).pack(side="left")

        status = ttk.LabelFrame(tab, text="Receiver status", padding=6)
        status.grid(row=2, column=0, sticky="ew", pady=4)
        self.var_recv_state = tk.StringVar(value="Not listening")
        ttk.Label(status, textvariable=self.var_recv_state).pack(anchor="w")

        log_box = ttk.LabelFrame(tab, text="Log", padding=4)
        log_box.grid(row=3, column=0, sticky="nsew", pady=4)
        self.recv_log = tk.Text(log_box, height=16, state="disabled",
                                font=("Consolas", 9))
        scroll = ttk.Scrollbar(log_box, command=self.recv_log.yview)
        self.recv_log.configure(yscrollcommand=scroll.set)
        self.recv_log.pack(side="left", fill="both", expand=True)
        scroll.pack(side="right", fill="y")

        tab.columnconfigure(0, weight=1)
        tab.rowconfigure(3, weight=1)

    def _pick_outdir(self) -> None:
        chosen = filedialog.askdirectory(title="Choose the destination folder",
                                         initialdir=self.var_outdir.get())
        if chosen:
            self.var_outdir.set(chosen)

    def _start_receiver(self) -> None:
        if self._receiver is not None:
            messagebox.showinfo("Already running",
                                "Receiver is already listening.")
            return
        try:
            self._receiver = Receiver(
                port=int(self.var_rport.get()),
                output_dir=self.var_outdir.get(),
                log=config.setup_logging("gui-receiver", console=False),
                sim=None)
            self._receiver.on_event = self._events.put
        except OSError as exc:
            self._receiver = None
            messagebox.showerror("Cannot listen", str(exc))
            return
        self._recv_thread = self._receiver.start_in_background()
        self.var_recv_state.set(
            f"Listening on UDP port {self._receiver.bound_port} "
            f"(saving to {self.var_outdir.get()})")
        self.btn_stop_recv.state(["!disabled"])
        self._log(self.recv_log, f"receiver listening on port "
                  f"{self._receiver.bound_port}")

    def _stop_receiver(self) -> None:
        if self._receiver is None:
            return
        rx, self._receiver = self._receiver, None
        rx.stop()
        self.var_recv_state.set("Stopped")
        self.btn_stop_recv.state(["disabled"])
        self._log(self.recv_log, "receiver stopped")

    # ================================================================= events
    def _log(self, widget: tk.Text, line: str) -> None:
        widget.configure(state="normal")
        widget.insert("end", time.strftime("%H:%M:%S ") + line + "\n")
        widget.see("end")
        widget.configure(state="disabled")

    def _poll_events(self) -> None:
        if not self.winfo_exists():
            return                          # window is gone - stop polling
        try:
            while True:
                event = self._events.get_nowait()
                self._handle_event(event)
        except queue.Empty:
            pass
        self.after(POLL_MS, self._poll_events)

    def _handle_event(self, ev: dict) -> None:
        kind = ev.get("event")

        if kind == "progress":
            total = max(ev.get("total_packets", 1), 1)
            acked = ev.get("acked", 0)
            self.progress["value"] = 100.0 * acked / total
            self._bytes_ack = ev.get("bytes_acked", 0)
            size = ev.get("file_size", 0) or 1
            self.var_bytes.set(f"{self._bytes_ack:,} / {size:,} B")
            elapsed = max(time.monotonic() - self._t0, 1e-6)
            speed = self._bytes_ack / elapsed
            self.var_speed.set(f"{speed:,.0f} B/s")
            self.var_retx.set(str(ev.get("retransmissions", 0)))

        elif kind == "end":
            result = ev.get("result") if isinstance(ev.get("result"),
                                                    dict) else None
            self.var_status.set("Completed successfully")
            self.var_sha.set("VERIFIED - match" if ev.get("ok")
                             else "FAILED")

        elif kind == "done":
            self._sender_busy = False
            self.btn_send.state(["!disabled"])
            result = ev["result"]
            if result.ok:
                self.progress["value"] = 100
                self.var_status.set(
                    f"Success in {result.duration_s:.2f} s "
                    f"({result.throughput_kbps:.0f} kbit/s)")
                self.var_sha.set(
                    "VERIFIED - match" if result.sha_verified
                    else "NOT verified")
                self._log(self.send_log,
                          f"OK: saved as {result.saved_as}, "
                          f"{result.retransmissions} retransmissions, "
                          f"{result.timeouts} timeouts")
            else:
                self.var_status.set("FAILED")
                self.var_sha.set("-")
                self._log(self.send_log, f"FAILED: {result.error}")
                messagebox.showerror("Transfer failed", result.error or "?")

        elif kind == "error":
            self._sender_busy = False
            self.btn_send.state(["!disabled"])
            self.var_status.set(f"Error: {ev.get('error', '?')}")
            self._log(self.send_log, f"error: {ev.get('error')}")

        elif kind == "listening":
            self._log(self.recv_log, f"listening on port {ev.get('port')}")

        elif kind == "transfer_start":
            self._log(self.recv_log,
                      f"receiving {ev.get('filename')} "
                      f"({ev.get('size', 0):,} B, "
                      f"{ev.get('packets', 0)} packets)")

        elif kind == "transfer_done":
            if ev.get("ok"):
                self._log(self.recv_log,
                          f"VERIFIED (sha256 ok) - saved as "
                          f"{ev.get('saved_as')}")
            else:
                self._log(self.recv_log,
                          f"VERIFICATION FAILED - kept as "
                          f"{ev.get('saved_as')}")

        elif kind == "transfer_aborted":
            self._log(self.recv_log, f"aborted: {ev.get('reason')}")

    def _on_close(self) -> None:
        self._stop_receiver()
        try:
            self.destroy()
        except tk.TclError:
            pass                        # window already torn down


def main() -> int:
    app = App()
    app.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
