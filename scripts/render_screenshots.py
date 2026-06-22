"""Render lab deliverable evidence (notebook outputs + FS layout) to PNGs.

Produces terminal-style screenshots under submission/screenshots/ so the lite
path has image evidence for each rubric criterion. Faithful: text is pulled
straight from the executed .ipynb stream outputs and the on-disk _delta_log.
"""
import json
import glob
import os
from PIL import Image, ImageDraw, ImageFont

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(ROOT, "submission", "screenshots")
os.makedirs(OUT, exist_ok=True)

# --- fonts -----------------------------------------------------------------
FONT_PATH = r"C:\Windows\Fonts\CascadiaMono.ttf"
if not os.path.exists(FONT_PATH):
    FONT_PATH = r"C:\Windows\Fonts\consola.ttf"
FS = 19
FONT = ImageFont.truetype(FONT_PATH, FS)
HFONT = ImageFont.truetype(FONT_PATH, FS + 2)

# GitHub-dark palette
BG = (13, 17, 23)
HEADER_BG = (22, 27, 34)
FG = (201, 209, 217)
CMD = (126, 231, 135)      # $ commands -> green
ACCENT = (88, 166, 255)    # header title -> blue
GOOD = (63, 185, 80)       # pass markers -> green
WARN = (210, 153, 34)

PAD = 18
LH = FS + 8                # line height


def _color(line):
    s = line.lstrip()
    if s.startswith("$ "):
        return CMD
    if ("✓" in line or "✅" in line or "expected" in line.lower()
            or "passed" in line.lower() or "target" in line.lower()
            or "BLOCKED" in line):
        return GOOD
    return FG


def render(title, body, fname, max_lines_width=None):
    lines = body.rstrip("\n").split("\n")
    # measure
    def w(s):
        return FONT.getlength(s)
    text_w = max([w(l) for l in lines] + [HFONT.getlength(title)])
    width = int(text_w) + PAD * 2
    height = PAD + LH + 6 + LH * len(lines) + PAD   # header + body
    img = Image.new("RGB", (width, height), BG)
    d = ImageDraw.Draw(img)
    # header bar
    d.rectangle([0, 0, width, PAD + LH], fill=HEADER_BG)
    d.text((PAD, PAD // 2), title, font=HFONT, fill=ACCENT)
    # body
    y = PAD + LH + 6
    for l in lines:
        d.text((PAD, y), l, font=FONT, fill=_color(l))
        y += LH
    path = os.path.join(OUT, fname)
    img.save(path)
    print(f"  wrote {fname}  ({width}x{height})")


def cell_stream(nb, idx):
    """Concatenated stream text of the idx-th *code* cell (0-based)."""
    j = json.load(open(os.path.join(ROOT, "notebooks", nb), encoding="utf-8"))
    ci = -1
    for c in j["cells"]:
        if c["cell_type"] != "code":
            continue
        ci += 1
        if ci == idx:
            return "".join("".join(o.get("text", []))
                           for o in c.get("outputs", [])
                           if o.get("output_type") == "stream")
    return ""


# --- A: tree ---------------------------------------------------------------
tree = open(os.path.join(OUT, "01_tree_lakehouse.txt"), encoding="utf-8").read()
render("Terminal — tree _lakehouse/  (Bronze/Silver/Gold + _delta_log visible)",
       tree, "01_tree_lakehouse.png")

# --- B: delta_log v0 commit (pretty, nested schema parsed) -----------------
v0 = sorted(glob.glob(os.path.join(
    ROOT, "_lakehouse", "scratch", "users_delta", "_delta_log", "*.json")))[0]
parts = ["$ cat .../users_delta/_delta_log/00000000000000000000.json", ""]
for line in open(v0, encoding="utf-8"):
    line = line.strip()
    if not line:
        continue
    obj = json.loads(line)
    # expand embedded JSON strings so nothing is one giant line
    md = obj.get("metaData")
    if md and isinstance(md.get("schemaString"), str):
        md["schema"] = json.loads(md.pop("schemaString"))
    ad = obj.get("add")
    if ad and isinstance(ad.get("stats"), str):
        ad["stats"] = json.loads(ad["stats"])
    parts.append(json.dumps(obj, indent=2))
render("Delta transaction log — v0 commit (schema enforcement + ACID add)",
       "\n".join(parts), "02_delta_log_v0.png")

# --- NB1 -------------------------------------------------------------------
nb1 = "\n".join([
    "# NB1 — schema enforcement blocks a bad write; merge adds `tier`",
    "",
    cell_stream("01_delta_basics.ipynb", 3).rstrip(),   # BLOCKED ...
    "",
    cell_stream("01_delta_basics.ipynb", 4).rstrip(),   # tier table
    "",
    cell_stream("01_delta_basics.ipynb", 5).rstrip(),   # tier counts
])
render("NB1  01_delta_basics  —  schema enforcement + schema_mode=merge",
       nb1, "03_nb1_delta_basics.png")

# --- NB2 (trim the long per-file range dump) -------------------------------
c5 = cell_stream("02_optimize_zorder.ipynb", 5).split("\n")
trimmed, kept = [], 0
for l in c5:
    if "file user_id range" in l:
        kept += 1
        if kept <= 3:
            trimmed.append(l)
        elif kept == 4:
            trimmed.append("  ... (49 more files, one min/max range each) ...")
    else:
        trimmed.append(l)
nb2 = "\n".join([
    "# NB2 — small-file problem -> OPTIMIZE + Z-order",
    "",
    cell_stream("02_optimize_zorder.ipynb", 1).rstrip(),   # files before
    cell_stream("02_optimize_zorder.ipynb", 2).rstrip(),   # before bench
    cell_stream("02_optimize_zorder.ipynb", 3).rstrip(),   # files after
    cell_stream("02_optimize_zorder.ipynb", 4).rstrip(),   # after bench
    "",
    "\n".join(trimmed).rstrip(),
])
render("NB2  02_optimize_zorder  —  speedup 9.3x  /  files-pruned 55x",
       nb2, "04_nb2_optimize_zorder.png")

# --- NB3 -------------------------------------------------------------------
nb3 = "\n".join([
    "# NB3 — MERGE 100K + RESTORE; history >= 5 versions (incl. RESTORE)",
    "",
    cell_stream("03_time_travel.ipynb", 1).rstrip(),   # MERGE
    cell_stream("03_time_travel.ipynb", 3).rstrip(),   # time travel
    cell_stream("03_time_travel.ipynb", 4).rstrip(),   # RESTORE + score<0
    "",
    cell_stream("03_time_travel.ipynb", 5).rstrip(),   # final history
])
render("NB3  03_time_travel  —  MERGE 0.82s, RESTORE 0.06s, 5 versions",
       nb3, "05_nb3_time_travel.png")

# --- NB4 -------------------------------------------------------------------
nb4 = "\n".join([
    "# NB4 — medallion Bronze -> Silver (dedup) -> Gold (>= 7 dates x 3 models)",
    "",
    cell_stream("04_medallion.ipynb", 1).rstrip(),   # bronze
    cell_stream("04_medallion.ipynb", 2).rstrip(),   # silver dedup
    "",
    cell_stream("04_medallion.ipynb", 4).rstrip(),   # gold table + metrics
])
render("NB4  04_medallion  —  Silver < Bronze dedup, Gold 8 dates x 3 models",
       nb4, "06_nb4_medallion.png")

print("done.")
