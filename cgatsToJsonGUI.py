\
from __future__ import annotations

import json
import logging
import os
import re
import sys
import uuid
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import tkinter as tk
from tkinter import ttk, filedialog, messagebox


# Converts a CGATS Color Data file to JSON format
# With GUI
# JSON file will be used to load data into Postgres database
#
# Version 2.0.0 - 10 Aug 2025
#   TKinter GUI application
#   Refactored for clarity and maintainability
#   Added --compact option for compact JSON output
#   Improved logging configuration
#   Added default values for PROJECT and TEMPLATE fields
#   Enhanced error handling and reporting
#   Improved parsing logic for robustness
#   Eliminated descriptive data that is included in the template table in the database

# ----------------------------
# Parsing / Conversion Logic
# ----------------------------

CGATS_KV_RE = re.compile(r"^\s*([A-Za-z0-9_./-]+)\s*[:=]\s*(.*?)\s*$")
COMMENT_RE = re.compile(r"^\s*(?:#|//|;|\*)")
WS_ONLY_RE = re.compile(r"^\s*$")


@dataclass(frozen=True)
class ParseResult:
    descriptive: Dict[str, str]
    keys: List[str]
    rows: List[Dict[str, str]]


def iter_nonempty_lines(text: Iterable[str]) -> Iterable[str]:
    for raw in text:
        line = raw.rstrip("\r\n")
        if not line:
            yield ""
        else:
            yield line


def parse_descriptive_and_formats(lines: Iterable[str], logger: logging.Logger) -> Tuple[Dict[str, str], List[str], List[str]]:
    descriptive: Dict[str, str] = {}
    keys: List[str] = []
    buffer_after_begin_data: List[str] = []

    in_df = False
    in_data = False

    for line in iter_nonempty_lines(lines):
        u = line.strip()

        if not in_df and not in_data and (COMMENT_RE.match(u) or WS_ONLY_RE.match(u)):
            continue

        if not in_df and not in_data:
            if u.upper().startswith("BEGIN_DATA_FORMAT"):
                in_df = True
                continue
            if u.upper().startswith("BEGIN_DATA"):
                in_data = True
                continue

            m = CGATS_KV_RE.match(line)
            if m:
                k, v = m.group(1), m.group(2)
                descriptive[k.strip().upper()] = v.strip()
            else:
                logger.debug("Ignoring header line: %r", line)
            continue

        if in_df and not in_data:
            if u.upper().startswith("END_DATA_FORMAT"):
                in_df = False
                continue
            if u.upper().startswith("BEGIN_DATA"):
                in_df = False
                in_data = True
                continue

            if u and not COMMENT_RE.match(u):
                parts = u.split()
                keys.extend([p.strip().upper() for p in parts if p.strip()])
            continue

        if in_data:
            buffer_after_begin_data.append(line)

    return descriptive, keys, buffer_after_begin_data


def parse_rows(after_begin_data_lines: List[str], keys: List[str], logger: logging.Logger) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for line in after_begin_data_lines:
        u = line.strip()
        if not u:
            continue
        up = u.upper()
        if up.startswith("END_DATA"):
            break
        if COMMENT_RE.match(u):
            continue

        cols = u.split()
        if len(cols) != len(keys):
            logger.warning("Row has %d cols but %d keys. Row: %r", len(cols), len(keys), u)
        if len(cols) < len(keys):
            cols.extend([""] * (len(keys) - len(cols)))
        elif len(cols) > len(keys):
            cols = cols[: len(keys)]

        row = {k: v for k, v in zip(keys, cols)}
        rows.append(row)
    return rows


def parse_cgats_text(text: str, logger: logging.Logger) -> ParseResult:
    descriptive, keys, after_begin_data = parse_descriptive_and_formats(text.splitlines(), logger)

    if not keys:
        logger.debug("No keys found; attempting to infer from first data line.")
        for line in after_begin_data:
            s = line.strip()
            if s and not COMMENT_RE.match(s) and not s.upper().startswith("END_DATA"):
                n = len(s.split())
                keys = [f"C{i+1}" for i in range(n)]
                logger.debug("Inferred %d columns: %s", n, keys)
                break

    rows = parse_rows(after_begin_data, keys, logger)
    return ParseResult(descriptive=descriptive, keys=keys, rows=rows)


def to_json(descriptive: Dict[str, str], rows: List[Dict[str, str]], measurement_uuid: str) -> Dict[str, object]:
    enriched_rows: List[Dict[str, str]] = []
    for r in rows:
        rr = dict(r)
        rr["MEASUREMENT_ID"] = measurement_uuid
        enriched_rows.append(rr)

    return {
        "descriptive_data": descriptive,
        "measurement_data": enriched_rows,
    }


# ----------------------------
# Prefs utilities (OS path + plain JSON)
# ----------------------------

def get_config_dir(app_name: str = "ColorDataMeasurement") -> Path:
    if sys.platform.startswith("win"):
        base = os.environ.get("APPDATA") or str(Path.home() / "AppData" / "Roaming")
        return Path(base) / app_name
    elif sys.platform == "darwin":
        return Path.home() / "Library" / "Application Support" / app_name
    else:
        base = os.environ.get("XDG_CONFIG_HOME", str(Path.home() / ".config"))
        return Path(base) / app_name

def load_prefs_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}

def save_prefs_json(path: Path, prefs: dict) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(prefs, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        print(f"Warning: could not save prefs: {e}", file=sys.stderr)


# ----------------------------
# GUI Application
# ----------------------------

INVALID_CHARS = r'<>:"/\\|?*'
RESERVED_BASENAMES = {
    "CON", "PRN", "AUX", "NUL",
    *{f"COM{i}" for i in range(1, 10)},
    *{f"LPT{i}" for i in range(1, 10)},
}

def sanitize_filename(name: str) -> tuple[str, bool, str]:
    original = name
    modified = False
    reason_parts = []
    if not name:
        name = "Measurement.json"
        modified = True
        reason_parts.append("empty filename")

    basename = name
    invalid_re = re.compile(f"[{re.escape(INVALID_CHARS)}]")
    if invalid_re.search(basename):
        basename = invalid_re.sub("_", basename)
        modified = True
        reason_parts.append("removed invalid characters")

    cleaned = basename.strip(" .")
    if cleaned != basename:
        basename = cleaned
        modified = True
        reason_parts.append("trimmed leading/trailing dots/spaces")

    if not basename:
        basename = "Measurement"
        modified = True
        reason_parts.append("empty after cleaning")

    stem, dot2, ext2 = basename.rpartition(".")
    if dot2 and ext2.lower() == "json":
        final_stem = stem
        final_ext = ".json"
    else:
        final_stem = basename
        final_ext = ".json"

    check_stem = final_stem.upper()
    if check_stem in RESERVED_BASENAMES:
        final_stem = final_stem + "_1"
        modified = True
        reason_parts.append("avoided reserved name")

    clean_name = f"{final_stem}{final_ext}"
    reason = ", ".join(reason_parts) if modified else ""
    return clean_name, modified, reason


def next_available_path(path: Path) -> Path:
    """Return a new Path with _1, _2, ... before the extension until it doesn't exist."""
    parent = path.parent
    stem = path.stem
    suffix = path.suffix
    i = 1
    while True:
        candidate = parent / f"{stem}_{i}{suffix}"
        if not candidate.exists():
            return candidate
        i += 1


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Color Data Measurement Processor")
        self.geometry("820x620")
        self.minsize(800, 580)

        # Paths / prefs
        self.startup_dir = Path.cwd()
        self.config_dir = get_config_dir()
        self.prefs_path = self.config_dir / "prefs.json"

        today_str = date.today().isoformat()
        default_output_name = f"Measurement_{today_str}.json"

        # Load prefs
        tmp_prefs = load_prefs_json(self.prefs_path)

        # Defaults with prefs overrides
        self.input_path: Path | None = Path(tmp_prefs["last_input_path"]) if tmp_prefs.get("last_input_path") else None
        self.output_folder: Path = Path(tmp_prefs["output_folder"]) if tmp_prefs.get("output_folder") else self.startup_dir
        self.output_filename: str = tmp_prefs.get("output_filename", default_output_name)
        self.default_output_name: str = default_output_name

        self._prefs_loaded = tmp_prefs  # store for initial UI values

        # Logger
        self.logger = logging.getLogger("gui")
        if not self.logger.handlers:
            h = logging.StreamHandler(sys.stdout)
            h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
            self.logger.addHandler(h)
            self.logger.setLevel(logging.INFO)

        self._build_ui()
        self.protocol("WM_DELETE_WINDOW", self._on_close)

    @property
    def output_path(self) -> Path:
        name = self.output_filename.strip() or self.default_output_name
        clean, modified, _ = sanitize_filename(name)
        if modified:
            self.output_filename = clean
            self.filename_var.set(clean)
        return self.output_folder / self.output_filename

    def _build_ui(self):
        outer = ttk.Frame(self, padding=16)
        outer.pack(fill="both", expand=True)

        # Header
        header = ttk.Label(outer, text="Color Data Measurement Processor", font=("Segoe UI", 18, "bold"))
        header.pack(anchor="w", pady=(0, 12))

        # Form grid
        form = ttk.Frame(outer)
        form.pack(fill="x", pady=(0, 12))

        ttk.Label(form, text="Project:").grid(row=0, column=0, sticky="e", padx=(0, 8), pady=6)
        self.project_var = tk.StringVar(value=self._prefs_loaded.get("project", ""))
        ttk.Entry(form, textvariable=self.project_var, width=50).grid(row=0, column=1, sticky="we", padx=(0, 8), pady=6)

        ttk.Label(form, text="Template:").grid(row=1, column=0, sticky="e", padx=(0, 8), pady=6)
        self.template_var = tk.StringVar(value=self._prefs_loaded.get("template", ""))
        ttk.Entry(form, textvariable=self.template_var, width=50).grid(row=1, column=1, sticky="we", padx=(0, 8), pady=6)

        ttk.Label(form, text="Measurement Date (YYYY-MM-DD):").grid(row=2, column=0, sticky="e", padx=(0, 8), pady=6)
        self.mdate_var = tk.StringVar(value=self._prefs_loaded.get("measurement_date", date.today().isoformat()))
        ttk.Entry(form, textvariable=self.mdate_var, width=20).grid(row=2, column=1, sticky="w", padx=(0, 8), pady=6)
        form.columnconfigure(1, weight=1)

        # File selectors
        filebox = ttk.LabelFrame(outer, text="Files", padding=12)
        filebox.pack(fill="x", pady=(0, 12))

        in_row = ttk.Frame(filebox); in_row.pack(fill="x", pady=6)
        ttk.Label(in_row, text="Input CGATS file:").pack(side="left")
        self.input_var = tk.StringVar(value=str(self.input_path) if self.input_path else "(none)")
        self.input_label = ttk.Label(in_row, textvariable=self.input_var, justify="left")
        self.input_label.pack(side="left", padx=8, fill="x", expand=True)
        ttk.Button(in_row, text="Choose...", command=self.choose_input).pack(side="right")

        folder_row = ttk.Frame(filebox); folder_row.pack(fill="x", pady=6)
        ttk.Label(folder_row, text="Output folder:").pack(side="left")
        self.folder_var = tk.StringVar(value=str(self.output_folder))
        self.folder_label = ttk.Label(folder_row, textvariable=self.folder_var, justify="left")
        self.folder_label.pack(side="left", padx=8, fill="x", expand=True)
        ttk.Button(folder_row, text="Choose folder...", command=self.choose_output_folder).pack(side="right")

        fname_row = ttk.Frame(filebox); fname_row.pack(fill="x", pady=6)
        ttk.Label(fname_row, text="Output filename (.json):").pack(side="left")
        self.filename_var = tk.StringVar(value=self.output_filename)
        ttk.Entry(fname_row, textvariable=self.filename_var, width=50).pack(side="left", padx=8, fill="x", expand=True)

        eff_row = ttk.Frame(filebox); eff_row.pack(fill="x", pady=6)
        ttk.Label(eff_row, text="Will save to:").pack(side="left")
        self.eff_path_var = tk.StringVar(value=str(self.output_path))
        self.eff_label = ttk.Label(eff_row, textvariable=self.eff_path_var, justify="left")
        self.eff_label.pack(side="left", padx=8, fill="x", expand=True)

        def update_wraplength(event=None):
            pad = 24
            for row, label in [(in_row, self.input_label), (folder_row, self.folder_label), (eff_row, self.eff_label)]:
                try:
                    w = max(100, row.winfo_width() - pad)
                    label.configure(wraplength=w)
                except Exception:
                    pass
            self.eff_path_var.set(str(self.output_path))

        in_row.bind("<Configure>", update_wraplength)
        folder_row.bind("<Configure>", update_wraplength)
        eff_row.bind("<Configure>", update_wraplength)
        self.bind("<Configure>", update_wraplength)

        # Options
        opts = ttk.LabelFrame(outer, text="Options", padding=12)
        opts.pack(fill="x", pady=(0, 12))
        self.compact_var = tk.BooleanVar(value=bool(self._prefs_loaded.get("compact", False)))
        ttk.Checkbutton(opts, text="Compact JSON", variable=self.compact_var).pack(anchor="w")
        self.replace_all_var = tk.BooleanVar(value=False)  # session-only
        ttk.Checkbutton(opts, text="Replace existing files without prompting (this session)", variable=self.replace_all_var).pack(anchor="w")

        # Actions
        actions = ttk.Frame(outer); actions.pack(fill="x", pady=(8, 0))
        ttk.Button(actions, text="Process", command=self.process).pack(side="right")
        ttk.Button(actions, text="Quit", command=self._on_close).pack(side="right", padx=(0, 8))

        def on_filename_change(*args):
            self.output_filename = self.filename_var.get().strip()
            clean, modified, reason = sanitize_filename(self.output_filename)
            if modified:
                self.output_filename = clean
                self.filename_var.set(clean)
            self.eff_path_var.set(str(self.output_path))
        self.filename_var.trace_add("write", lambda *a: on_filename_change())

    # File dialogs
    def choose_input(self):
        initial = self.output_folder if self.output_folder.exists() else self.startup_dir
        path = filedialog.askopenfilename(
            title="Select CGATS File",
            initialdir=str(initial),
            filetypes=[("CGATS / Text", "*.txt *.cgats *.cie *.cxf *.txf *.it8 *.itx *.sp"), ("All files", "*.*")],
        )
        if path:
            self.input_path = Path(path)
            self.input_var.set(str(self.input_path))
            today_str = date.today().isoformat()
            suggested = f"{self.input_path.stem}_{today_str}.json"
            clean, modified, _ = sanitize_filename(suggested)
            self.output_filename = clean
            self.filename_var.set(clean)
            if str(self.output_folder) == str(self.startup_dir):
                self.output_folder = self.input_path.parent
                self.folder_var.set(str(self.output_folder))
            self.eff_path_var.set(str(self.output_path))

    def choose_output_folder(self):
        initial = self.output_folder if self.output_folder.exists() else self.startup_dir
        path = filedialog.askdirectory(
            title="Choose Output Folder",
            initialdir=str(initial),
            mustexist=True,
        )
        if path:
            self.output_folder = Path(path)
            self.folder_var.set(str(self.output_folder))
            self.eff_path_var.set(str(self.output_path))

    # Processing
    def process(self):
        if not self.input_path or not self.input_path.exists():
            messagebox.showerror("Missing input", "Please choose a valid CGATS input file.")
            return

        mdate = self.mdate_var.get().strip()
        if mdate:
            try:
                datetime.strptime(mdate, "%Y-%m-%d")
            except ValueError:
                messagebox.showerror("Invalid date", "Please enter Measurement Date as YYYY-MM-DD.")
                return
        else:
            mdate = date.today().isoformat()

        name = self.filename_var.get().strip()
        clean, modified, reason = sanitize_filename(name)
        if modified:
            self.filename_var.set(clean)
            self.output_filename = clean
            messagebox.showinfo("Filename adjusted", f"Your filename was adjusted to:\n{clean}\n\nReason: {reason}")

        target_path = self.output_path

        # Overwrite handling
        if target_path.exists():
            if self.replace_all_var.get():
                # proceed without prompting
                pass
            else:
                overwrite = messagebox.askyesno(
                    "File exists",
                    f"The file already exists:\n{target_path}\n\nDo you want to overwrite it?",
                    icon="warning"
                )
                if not overwrite:
                    new_path = next_available_path(target_path)
                    # Update filename field in the UI
                    self.output_filename = new_path.name
                    self.filename_var.set(self.output_filename)
                    target_path = new_path
                    self.eff_path_var.set(str(target_path))

        # Read & parse
        try:
            text = self.input_path.read_text(encoding="utf-8", errors="replace")
        except Exception as e:
            messagebox.showerror("Read error", f"Could not read input file:\n{e}")
            return

        logger = logging.getLogger("cgats2json.gui")
        logger.setLevel(logging.INFO)

        try:
            result = parse_cgats_text(text, logger=logger)
        except Exception as e:
            messagebox.showerror("Parse error", f"Failed to parse CGATS file:\n{e}")
            return

        measurement_uuid = str(uuid.uuid4())
        desc = result.descriptive
        desc.setdefault("MEASUREMENT_ID", measurement_uuid)
        desc.setdefault("PARSED_DATE", date.today().isoformat())
        project = (self.project_var.get() or "NONE").strip()
        template = (self.template_var.get() or "NONE").strip()
        desc.setdefault("PROJECT", project if project else "NONE")
        desc.setdefault("TEMPLATE", template if template else "NONE")
        desc["MEASUREMENT_DATE"] = mdate

        payload = to_json(desc, result.rows, measurement_uuid)
        indent = None if self.compact_var.get() else 2

        try:
            target_path.parent.mkdir(parents=True, exist_ok=True)
            target_path.write_text(json.dumps(payload, indent=indent, ensure_ascii=False), encoding="utf-8")
        except Exception as e:
            messagebox.showerror("Write error", f"Could not write output file:\n{e}")
            return

        # Save prefs after successful process
        prefs = {
            "project": self.project_var.get(),
            "template": self.template_var.get(),
            "measurement_date": self.mdate_var.get(),
            "last_input_path": str(self.input_path) if self.input_path else "",
            "output_folder": str(self.output_folder),
            "output_filename": self.output_filename,
            "compact": bool(self.compact_var.get()),
        }
        save_prefs_json(self.prefs_path, prefs)

        messagebox.showinfo("Done", f"Wrote JSON to:\n{target_path}")

    def _on_close(self):
        # Attempt to save current UI state as prefs on exit (best-effort)
        prefs = {
            "project": self.project_var.get(),
            "template": self.template_var.get(),
            "measurement_date": self.mdate_var.get(),
            "last_input_path": str(self.input_path) if self.input_path else "",
            "output_folder": str(self.output_folder),
            "output_filename": self.output_filename,
            "compact": bool(self.compact_var.get()),
        }
        save_prefs_json(self.prefs_path, prefs)
        self.destroy()


def main():
    try:
        import tkinter.ttk as _ttk
        root = App()
        try:
            style = _ttk.Style()
            style.theme_use("clam")
        except Exception:
            pass
        root.mainloop()
    except Exception as e:
        print("Failed to start GUI:", e, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
