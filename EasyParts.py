#!/usr/bin/env python3
"""
GameParts Launcher — Parallel Downloader with Pause/Resume & Auto-Extract

Features
- Modern PySide6 UI (dark-ish look using Qt palettes)
- Add multiple part URLs (paste list or add one-by-one)
- Parallel downloads with configurable concurrency
- Per-file progress + overall progress
- Pause / Resume / Cancel
- Resume on restart (Range requests + .part temp files)
- Auto-extract multi-part archives (.zip/.7z/.rar, etc.) via patool/7z/unrar
- Option to delete parts after successful extraction

Requirements
  pip install PySide6 requests patool

Additionally, install a backend extractor:
  - Windows: install 7-Zip (adds 7z.exe) and/or WinRAR (unrar.exe) and ensure they are in PATH
  - macOS: brew install p7zip unrar
  - Linux: sudo apt-get install p7zip-full unrar (or distro equivalent)

Run
  python gameparts_launcher.py
"""
from __future__ import annotations

import os
import sys
import math
import time
import threading
from dataclasses import dataclass, field
from typing import List, Optional, Dict
from urllib.parse import urlparse, unquote

import requests
from requests.exceptions import RequestException

# UI
from PySide6.QtCore import (
    Qt,
    QAbstractTableModel,
    QModelIndex,
    QObject,
    QRunnable,
    QThreadPool,
    Signal,
    Slot,
)
from PySide6.QtGui import QAction, QIcon
from PySide6.QtWidgets import (
    QApplication,
    QMainWindow,
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QLineEdit,
    QFileDialog,
    QProgressBar,
    QSpinBox,
    QCheckBox,
    QTableView,
    QHeaderView,
    QMessageBox,
    QTextEdit,
    QSplitter,
    QStyle,
)

# Optional: extraction using patool
try:
    import patoolib
except Exception:  # pragma: no cover
    patoolib = None


# -----------------------------
# Data structures
# -----------------------------
@dataclass
class DownloadItem:
    url: str
    dest_dir: str
    filename: Optional[str] = None
    size: Optional[int] = None
    downloaded: int = 0
    status: str = "Queued"  # Queued, Downloading, Paused, Done, Error, Canceled
    error: Optional[str] = None
    index: int = 0  # for display order

    def final_path(self) -> str:
        if not self.filename:
            self.filename = infer_filename_from_url(self.url)
        return os.path.join(self.dest_dir, self.filename)

    def temp_path(self) -> str:
        base = self.final_path()
        return base + ".part"


# -----------------------------
# Helpers
# -----------------------------
USER_AGENT = "EasyPartsLauncher/1.0 (requests)"
CHUNK_SIZE = 1024 * 1024  # 1 MiB


def human_bytes(n: Optional[int]) -> str:
    if n is None or n < 0:
        return "?"
    units = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    f = float(n)
    while f >= 1024 and i < len(units) - 1:
        f /= 1024.0
        i += 1
    return f"{f:.2f} {units[i]}"


def infer_filename_from_url(url: str) -> str:
    path = urlparse(url).path
    candidate = os.path.basename(path) or "download.bin"
    return unquote(candidate)


def is_archive_first_part(name: str) -> bool:
    low = name.lower()
    # Typical patterns: .part1.rar, .001 (7z split), base.rar (single), base.zip, base.7z
    if ".part1." in low:
        return True
    if low.endswith((".rar", ".zip", ".7z")):
        return True
    if low.endswith(".001"):
        return True
    return False


# -----------------------------
# Worker & Manager
# -----------------------------
class DownloadSignals(QObject):
    progress = Signal(int, int, int)  # index, downloaded, total
    status = Signal(int, str)  # index, status
    error = Signal(int, str)  # index, message
    finished = Signal(int, str)  # index, path


class DownloadWorker(QRunnable):
    def __init__(self, item: DownloadItem, timeout: int = 30):
        super().__init__()
        self.item = item
        self.timeout = timeout
        self.signals = DownloadSignals()
        self._pause = threading.Event()
        self._stop = threading.Event()
        self._pause.clear()  # not paused initially

    # Control methods
    def request_pause(self):
        self._pause.set()

    def request_resume(self):
        self._pause.clear()

    def request_cancel(self):
        self._stop.set()

    def is_paused(self) -> bool:
        return self._pause.is_set()

    def is_stopped(self) -> bool:
        return self._stop.is_set()

    def run(self):  # executed in worker thread
        item = self.item
        os.makedirs(item.dest_dir, exist_ok=True)

        headers = {"User-Agent": USER_AGENT}
        temp_path = item.temp_path()
        final_path = item.final_path()

        # Resume support: if temp exists, continue from size
        resume_from = 0
        if os.path.exists(temp_path):
            resume_from = os.path.getsize(temp_path)
        elif os.path.exists(final_path):
            # already downloaded
            item.downloaded = os.path.getsize(final_path)
            item.size = item.downloaded
            self.signals.progress.emit(item.index, item.downloaded, item.size or -1)
            self.signals.status.emit(item.index, "Done")
            self.signals.finished.emit(item.index, final_path)
            return

        # Get headers/size & check if Range is supported
        try:
            with requests.head(item.url, allow_redirects=True, timeout=self.timeout, headers=headers) as rhead:
                rhead.raise_for_status()
                item.size = int(rhead.headers.get("Content-Length", "0")) or None
                accept_ranges = rhead.headers.get("Accept-Ranges", "").lower() == "bytes"
                # sometimes servers don't respond to HEAD; fall back later
        except RequestException:
            accept_ranges = True  # optimistic; many servers support range on GET even if HEAD fails

        mode = "ab" if resume_from else "wb"
        if resume_from:
            headers["Range"] = f"bytes={resume_from}-"
            item.downloaded = resume_from

        self.signals.status.emit(item.index, "Downloading")
        try:
            with requests.get(item.url, stream=True, timeout=self.timeout, headers=headers) as r:
                r.raise_for_status()
                # If server ignores Range, reset file
                if resume_from and r.status_code not in (206, 200):
                    # start over
                    resume_from = 0
                    item.downloaded = 0
                    mode = "wb"
                if r.status_code == 200 and resume_from:
                    # server ignored range, start fresh
                    resume_from = 0
                    item.downloaded = 0
                    mode = "wb"

                # Try to infer total size if unknown
                if item.size is None:
                    try:
                        total_from_resp = int(r.headers.get("Content-Length", "0"))
                        if total_from_resp > 0:
                            item.size = total_from_resp + resume_from
                    except Exception:
                        pass

                with open(temp_path, mode) as f:
                    last_emit = time.time()
                    for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                        if self.is_stopped():
                            self.signals.status.emit(item.index, "Canceled")
                            return
                        # Pause loop
                        while self.is_paused() and not self.is_stopped():
                            self.signals.status.emit(item.index, "Paused")
                            time.sleep(0.2)
                        if not chunk:
                            continue
                        f.write(chunk)
                        item.downloaded += len(chunk)
                        now = time.time()
                        if now - last_emit > 0.05:
                            self.signals.progress.emit(item.index, item.downloaded, item.size or -1)
                            last_emit = now

                # Done writing; rename
                os.replace(temp_path, final_path)
                self.signals.progress.emit(item.index, item.downloaded, item.size or -1)
                self.signals.status.emit(item.index, "Done")
                self.signals.finished.emit(item.index, final_path)
        except RequestException as e:
            item.error = str(e)
            self.signals.error.emit(item.index, item.error)
            self.signals.status.emit(item.index, "Error")


# -----------------------------
# Table Model
# -----------------------------
class DownloadsModel(QAbstractTableModel):
    HEADERS = ["#", "File", "Status", "Progress", "Size"]

    def __init__(self, items: List[DownloadItem]):
        super().__init__()
        self.items = items

    def rowCount(self, parent=QModelIndex()):
        return len(self.items)

    def columnCount(self, parent=QModelIndex()):
        return len(self.HEADERS)

    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if role == Qt.DisplayRole and orientation == Qt.Horizontal:
            return self.HEADERS[section]
        return super().headerData(section, orientation, role)

    def data(self, index: QModelIndex, role=Qt.DisplayRole):
        if not index.isValid():
            return None
        item = self.items[index.row()]
        col = index.column()
        if role == Qt.DisplayRole:
            if col == 0:
                return item.index + 1
            elif col == 1:
                return item.filename or infer_filename_from_url(item.url)
            elif col == 2:
                return item.status
            elif col == 3:
                if item.size:
                    pct = 100.0 * (item.downloaded / max(1, item.size))
                    return f"{pct:.1f}%"
                else:
                    return human_bytes(item.downloaded)
            elif col == 4:
                return human_bytes(item.size)
        return None

    def update_row(self, row: int):
        top = self.index(row, 0)
        bot = self.index(row, self.columnCount() - 1)
        self.dataChanged.emit(top, bot, [])


# -----------------------------
# Main Window
# -----------------------------
class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("EasyParts Launcher - Pre Release 1.0")
        self.resize(1100, 650)

        # State
        self.items: List[DownloadItem] = []
        self.workers: Dict[int, DownloadWorker] = {}
        self.threadpool = QThreadPool.globalInstance()
        self.max_concurrency = 3

        # UI
        self._build_ui()
        self._apply_dark_palette()

    # ----- UI construction -----
    def _build_ui(self):
        root = QWidget()
        self.setCentralWidget(root)
        layout = QVBoxLayout(root)

        # Controls row
        ctrl = QHBoxLayout()
        self.url_input = QLineEdit()
        self.url_input.setPlaceholderText("Paste a part URL then click + … or use Paste List")
        btn_add = QPushButton(self.style().standardIcon(QStyle.SP_DialogYesButton), "Add")
        btn_add.clicked.connect(self.add_url)

        btn_paste = QPushButton("Paste List")
        btn_paste.clicked.connect(self.paste_list)

        self.dest_label = QLabel("Output: Not set")
        btn_browse = QPushButton("Choose Folder")
        btn_browse.clicked.connect(self.choose_folder)

        self.concurrency = QSpinBox()
        self.concurrency.setRange(1, 16)
        self.concurrency.setValue(self.max_concurrency)
        self.concurrency.setPrefix("Parallel: ")
        self.concurrency.valueChanged.connect(self._update_concurrency)

        self.chk_extract = QCheckBox("Auto-extract after download")
        self.chk_cleanup = QCheckBox("Delete parts after extraction")
        self.chk_extract.setChecked(True)

        ctrl.addWidget(self.url_input, 4)
        ctrl.addWidget(btn_add)
        ctrl.addWidget(btn_paste)
        ctrl.addStretch(1)
        ctrl.addWidget(self.dest_label)
        ctrl.addWidget(btn_browse)
        ctrl.addWidget(self.concurrency)
        ctrl.addWidget(self.chk_extract)
        ctrl.addWidget(self.chk_cleanup)
        layout.addLayout(ctrl)

        # Table of downloads
        self.model = DownloadsModel(self.items)
        self.table = QTableView()
        self.table.setModel(self.model)
        self.table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self.table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(4, QHeaderView.ResizeToContents)
        layout.addWidget(self.table, 1)

        # Overall progress
        overall_box = QHBoxLayout()
        overall_box.addWidget(QLabel("Overall:"))
        self.overall_bar = QProgressBar()
        self.overall_bar.setRange(0, 1000)
        overall_box.addWidget(self.overall_bar, 1)
        self.lbl_overall = QLabel("0/0")
        overall_box.addWidget(self.lbl_overall)
        layout.addLayout(overall_box)

        # Buttons row
        btns = QHBoxLayout()
        self.btn_start = QPushButton("Start")
        self.btn_pause = QPushButton("Pause Selected")
        self.btn_resume = QPushButton("Resume Selected")
        self.btn_cancel = QPushButton("Cancel Selected")
        self.btn_remove = QPushButton("Remove Selected")
        self.btn_clear = QPushButton("Clear Finished")

        self.btn_start.clicked.connect(self.start_queue)
        self.btn_pause.clicked.connect(self.pause_selected)
        self.btn_resume.clicked.connect(self.resume_selected)
        self.btn_cancel.clicked.connect(self.cancel_selected)
        self.btn_remove.clicked.connect(self.remove_selected)
        self.btn_clear.clicked.connect(self.clear_finished)

        btns.addWidget(self.btn_start)
        btns.addWidget(self.btn_pause)
        btns.addWidget(self.btn_resume)
        btns.addWidget(self.btn_cancel)
        btns.addWidget(self.btn_remove)
        btns.addStretch(1)
        btns.addWidget(self.btn_clear)
        layout.addLayout(btns)

        # Footer / tips
        tips = QLabel(
            "Tip: You can paste multiple lines (one URL per line). The app supports resume. "
            "Use a download folder that has enough space."
        )
        tips.setStyleSheet("color: #aaa;")
        layout.addWidget(tips)

        # Menu actions
        act_quit = QAction("Quit", self)
        act_quit.triggered.connect(self.close)
        self.menuBar().addAction(act_quit)

    def _apply_dark_palette(self):
        # Minimal dark theme feel
        self.setStyleSheet(
            """
            QMainWindow { background: #14161a; }
            QLabel, QCheckBox { color: #e6e6e6; }
            QLineEdit, QTextEdit { background: #1e2127; color: #e6e6e6; border: 1px solid #2c313c; padding: 6px; }
            QPushButton { background: #2c313c; color: #e6e6e6; border: 1px solid #3a3f4b; padding: 8px 12px; border-radius: 8px; }
            QPushButton:hover { background: #3a3f4b; }
            QTableView { background: #1b1e23; color: #e6e6e6; gridline-color: #2c313c; }
            QHeaderView::section { background: #2c313c; color: #e6e6e6; padding: 6px; border: 0; }
            QProgressBar { border: 1px solid #3a3f4b; background: #1e2127; height: 16px; border-radius: 6px; text-align: center; color: #e6e6e6; }
            QProgressBar::chunk { background: #4a90e2; }
            """
        )

    # ----- Actions -----
    @Slot()
    def choose_folder(self):
        d = QFileDialog.getExistingDirectory(self, "Choose output folder")
        if d:
            self.dest_label.setText(f"Output: {d}")

    def current_output_dir(self) -> str:
        text = self.dest_label.text()
        if text.startswith("Output: "):
            return text[len("Output: "):]
        return os.path.abspath("downloads")

    @Slot()
    def add_url(self):
        url = self.url_input.text().strip()
        if not url:
            return
        self._add_item(url)
        self.url_input.clear()

    @Slot()
    def paste_list(self):
        # Paste from clipboard; assume one URL per line
        clip = QApplication.clipboard().text()
        lines = [ln.strip() for ln in clip.splitlines() if ln.strip()]
        if not lines:
            QMessageBox.information(self, "Nothing to paste", "Clipboard is empty or has no URLs.")
            return
        for url in lines:
            self._add_item(url)

    def _add_item(self, url: str):
        dest = self.current_output_dir()
        os.makedirs(dest, exist_ok=True)
        idx = len(self.items)
        item = DownloadItem(url=url, dest_dir=dest, index=idx)
        item.filename = infer_filename_from_url(url)
        self.model.beginInsertRows(QModelIndex(), idx, idx)
        self.items.append(item)
        self.model.endInsertRows()

    @Slot()
    def start_queue(self):
        self.max_concurrency = self.concurrency.value()
        # Launch up to N parallel workers; as workers finish, start next queued
        self._pump_queue()

    def _pump_queue(self):
        active = sum(1 for w in self.workers.values() if w and w.item.status == "Downloading")
        available_slots = self.max_concurrency - active
        if available_slots <= 0:
            return
        for item in self.items:
            if available_slots <= 0:
                break
            if item.status in ("Queued", "Paused", "Error", "Canceled"):
                # Create or reuse worker
                w = self.workers.get(item.index)
                if not w:
                    w = DownloadWorker(item)
                    self._wire_worker(w)
                    self.workers[item.index] = w
                # If paused, just resume; else start
                w.request_resume()
                item.status = "Downloading"
                self.model.update_row(item.index)
                self.threadpool.start(w)
                available_slots -= 1
        self._update_overall()

    def _wire_worker(self, w: DownloadWorker):
        w.signals.progress.connect(self.on_progress)
        w.signals.status.connect(self.on_status)
        w.signals.error.connect(self.on_error)
        w.signals.finished.connect(self.on_finished)

    @Slot(int, int, int)
    def on_progress(self, index: int, downloaded: int, total: int):
        item = self.items[index]
        item.downloaded = downloaded
        if total > 0:
            item.size = total
        self.model.update_row(index)
        self._update_overall()

    @Slot(int, str)
    def on_status(self, index: int, status: str):
        item = self.items[index]
        item.status = status
        self.model.update_row(index)
        self._update_overall()

    @Slot(int, str)
    def on_error(self, index: int, message: str):
        QMessageBox.warning(self, "Download error", f"Item {index+1}: {message}")
        self._update_overall()

    @Slot(int, str)
    def on_finished(self, index: int, path: str):
        # When all done, optionally extract
        if all(it.status in ("Done", "Error", "Canceled") for it in self.items if self.items):
            if self.chk_extract.isChecked():
                self.extract_all()
        self._update_overall()

    def _selected_rows(self) -> List[int]:
        sel = self.table.selectionModel().selectedRows()
        return [s.row() for s in sel]

    @Slot()
    def pause_selected(self):
        for row in self._selected_rows():
            w = self.workers.get(row)
            if w:
                w.request_pause()
                self.items[row].status = "Paused"
                self.model.update_row(row)
        self._update_overall()

    @Slot()
    def resume_selected(self):
        for row in self._selected_rows():
            w = self.workers.get(row)
            if w:
                w.request_resume()
                self.items[row].status = "Downloading"
                self.model.update_row(row)
        # ensure queue pumps
        self._pump_queue()

    @Slot()
    def cancel_selected(self):
        for row in self._selected_rows():
            w = self.workers.get(row)
            if w:
                w.request_cancel()
                self.items[row].status = "Canceled"
                self.model.update_row(row)
        self._update_overall()
    
    # ...existing code...
    @Slot()
    def cancel_selected(self):
        for row in self._selected_rows():
            w = self.workers.get(row)
            if w:
                w.request_cancel()
                self.items[row].status = "Canceled"
                self.model.update_row(row)
        self._update_overall()

    @Slot()
    def remove_selected(self):
        rows = sorted(self._selected_rows(), reverse=True)
        for row in rows:
            # Cancel if running
            w = self.workers.get(row)
            if w:
                w.request_cancel()
            # Remove from items and workers
            self.model.beginRemoveRows(QModelIndex(), row, row)
            del self.items[row]
            self.model.endRemoveRows()
            self.workers.pop(row, None)
        # Reindex items
        for i, it in enumerate(self.items):
            it.index = i
        self.model.items = self.items
        self._update_overall()
# ...existing code...

    @Slot()
    def clear_finished(self):
        # Remove Done/Error/Canceled rows from view (files are kept)
        keep: List[DownloadItem] = []
        for it in self.items:
            if it.status not in ("Done", "Error", "Canceled"):
                keep.append(it)
        self.model.beginResetModel()
        self.items = keep
        # reindex
        for i, it in enumerate(self.items):
            it.index = i
        self.model.items = self.items
        self.model.endResetModel()
        self.workers = {}
        self._update_overall()

    def _update_concurrency(self, val: int):
        self.max_concurrency = val
        self._pump_queue()

    def _update_overall(self):
        total = sum(it.size or 0 for it in self.items)
        done = sum(min(it.downloaded, it.size or it.downloaded) for it in self.items)
        pct = 0
        if total > 0:
            pct = int(1000 * (done / total))
        elif self.items:
            # unknown sizes: show fraction of done items
            finished = sum(1 for it in self.items if it.status == "Done")
            pct = int(1000 * (finished / len(self.items)))
        self.overall_bar.setValue(pct)
        self.lbl_overall.setText(f"{sum(1 for it in self.items if it.status=='Done')}/{len(self.items)} done")

    # ----- Extraction -----
    def extract_all(self):
        output_dir = self.current_output_dir()
        if not patoolib:
            QMessageBox.information(
                self,
                "Extractor not available",
                "patool is not installed. Run: pip install patool\n\n"
                "Also install 7-Zip/UnRAR so patool can extract .7z/.rar files.",
            )
            return

        # Find a likely first archive part in the folder
        files = sorted(os.listdir(output_dir))
        first_candidates = [f for f in files if is_archive_first_part(f)]
        if not first_candidates:
            QMessageBox.information(self, "No archive found", "Could not find an archive to extract.")
            return

        archive_path = os.path.join(output_dir, first_candidates[0])
        try:
            self.statusBar().showMessage(f"Extracting {os.path.basename(archive_path)}…")
            # patool chooses backend (7z/unrar/zip) by extension
            patoolib.extract_archive(archive_path, outdir=output_dir, interactive=False)
            self.statusBar().showMessage("Extraction complete")
            if self.chk_cleanup.isChecked():
                self._cleanup_parts(output_dir)
        except Exception as e:
            QMessageBox.warning(self, "Extraction failed", str(e))
            self.statusBar().showMessage("")

    def _cleanup_parts(self, output_dir: str):
        exts = (".rar", ".zip", ".7z", ".001")
        to_delete = [f for f in os.listdir(output_dir) if f.lower().endswith(exts) or ".part" in f.lower()]
        for f in to_delete:
            try:
                os.remove(os.path.join(output_dir, f))
            except Exception:
                pass


# -----------------------------
# App entry
# -----------------------------
def main():
    app = QApplication(sys.argv)
    win = MainWindow()
    win.show()
    # Prompt for an output dir on first run for convenience
    initial_dir = os.path.abspath("downloads")
    os.makedirs(initial_dir, exist_ok=True)
    win.dest_label.setText(f"Output: {initial_dir}")
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
