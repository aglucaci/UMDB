"""Write the UMDB logo as SVG: the mark, and horizontal lockups for light and dark backgrounds.

Text is converted to outlines (DejaVu Sans, Bitstream Vera licence, shipped with matplotlib) so the
logo renders identically everywhere, including GitHub READMEs where SVGs cannot load fonts.
"""
import os, sys
from matplotlib.font_manager import FontProperties
from matplotlib.textpath import TextPath

OUT = sys.argv[1]
TEAL, TEAL_DARK, AMBER, BLUE = "#14a085", "#0d7a66", "#f2a33a", "#5b8def"

def _d(tp):
    verts, codes, i, seg = tp.vertices, tp.codes, 0, []
    while codes is not None and i < len(codes):
        c = codes[i]
        if c == 1:   seg.append(f"M{verts[i][0]:.2f} {-verts[i][1]:.2f}"); i += 1
        elif c == 2: seg.append(f"L{verts[i][0]:.2f} {-verts[i][1]:.2f}"); i += 1
        elif c == 3: seg.append(f"Q{verts[i][0]:.2f} {-verts[i][1]:.2f} {verts[i+1][0]:.2f} {-verts[i+1][1]:.2f}"); i += 2
        elif c == 4: seg.append(f"C{verts[i][0]:.2f} {-verts[i][1]:.2f} {verts[i+1][0]:.2f} {-verts[i+1][1]:.2f} {verts[i+2][0]:.2f} {-verts[i+2][1]:.2f}"); i += 3
        elif c == 79: seg.append("Z"); i += 1
        else: i += 1
    return "".join(seg)

def text_path(s, size, weight="bold", tracking=0.0):
    """SVG path data for a string (baseline at y=0) and its width. With tracking, letters are spaced
    individually (no spaces allowed); without, the string is set as one run with the font's kerning."""
    fp = FontProperties(family="DejaVu Sans", weight=weight)
    if not tracking:
        tp = TextPath((0, 0), s, size=size, prop=fp)
        return _d(tp), tp.get_extents().x1
    out, x = [], 0.0
    for ch in s:
        tp = TextPath((x, 0), ch, size=size, prop=fp)
        out.append(_d(tp))
        x = tp.get_extents().x1 + tracking
    return "".join(out), x - tracking

def mark(ring="#14a085", dish="#e8f5f1"):
    """The emblem in a 120x120 box: a culture-dish ring around a city skyline, with microbes
    (cocci and a rod) linked like records in a database above it."""
    return f"""
  <defs><clipPath id="dish"><circle cx="60" cy="60" r="47"/></clipPath></defs>
  <circle cx="60" cy="60" r="50" fill="{dish}"/>
  <circle cx="60" cy="60" r="53" fill="none" stroke="{ring}" stroke-width="7"/>
  <g clip-path="url(#dish)">
    <g fill="{TEAL}">
      <rect x="17" y="70" width="11" height="40"/>
      <rect x="30" y="58" width="13" height="52"/>
      <rect x="45" y="44" width="15" height="66"/>
      <rect x="62" y="54" width="12" height="56"/>
      <rect x="76" y="64" width="14" height="46"/>
      <rect x="92" y="74" width="12" height="36"/>
    </g>
    <rect x="49.5" y="38" width="6" height="6" fill="{TEAL}"/>
    <g fill="#ffffff" fill-opacity=".9">
      <rect x="49" y="52" width="3" height="3"/><rect x="54" y="52" width="3" height="3"/>
      <rect x="49" y="59" width="3" height="3"/><rect x="54" y="66" width="3" height="3"/>
      <rect x="34" y="65" width="3" height="3"/><rect x="66" y="61" width="3" height="3"/>
      <rect x="80" y="71" width="3" height="3"/>
    </g>
    <rect x="0" y="94" width="120" height="30" fill="{TEAL_DARK}"/>
    <path d="M10 101 q 8 -5 16 0 t 16 0 t 16 0 t 16 0 t 16 0 t 16 0 t 16 0" fill="none" stroke="#ffffff" stroke-opacity=".55" stroke-width="2"/>
  </g>
  <g stroke="{AMBER}" stroke-width="2" stroke-linecap="round">
    <line x1="33" y1="35" x2="47" y2="25"/><line x1="47" y1="25" x2="66" y2="27"/><line x1="66" y1="27" x2="83" y2="37"/>
  </g>
  <g fill="{AMBER}">
    <circle cx="33" cy="35" r="5.5"/><circle cx="47" cy="25" r="4.5"/><circle cx="83" cy="37" r="5.5"/>
    <rect x="58" y="22" width="18" height="9" rx="4.5" transform="rotate(10 67 26.5)"/>
  </g>"""

def write(name, body, w, h, title):
    with open(os.path.join(OUT, name), "w") as fh:
        fh.write(f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {w} {h}" width="{w}" height="{h}" role="img" aria-label="{title}">\n'
                 f'  <title>{title}</title>{body}\n</svg>\n')

# the mark alone (transparent), and an app-icon tile
write("umdb-mark.svg", mark(), 120, 120, "UMDB")
write("umdb-icon.svg", f'\n  <rect width="120" height="120" rx="26" fill="#ffffff"/>\n  <g transform="translate(9 9) scale(.85)">{mark()}</g>', 120, 120, "UMDB")

# horizontal lockups
word, ww = text_path("UMDB", 64, "bold", tracking=2)
sub, sw = text_path("Urban Microbiome Database", 20.5, "normal")
W = int(140 + max(ww, sw) + 8)
for variant, ink, muted, ring, dish in (("light", "#14212b", "#4b5a66", TEAL, "#e8f5f1"),
                                       ("dark", "#eef3f6", "#a9b8c3", "#2fc4a3", "#12302a")):
    body = (f'\n  <g transform="translate(4 4)">{mark(ring, dish)}</g>'
            f'\n  <path transform="translate(140 79)" fill="{ink}" d="{word}"/>'
            f'\n  <path transform="translate(141 110)" fill="{muted}" d="{sub}"/>')
    write(f"umdb-logo-{variant}.svg", body, W, 128, "UMDB: Urban Microbiome Database")
    # compact: emblem and "UMDB" only, for small placements such as a site header
    cw = int(140 + ww + 6)
    body = (f'\n  <g transform="translate(4 4)">{mark(ring, dish)}</g>'
            f'\n  <path transform="translate(140 87)" fill="{ink}" d="{word}"/>')
    write(f"umdb-logo-compact-{variant}.svg", body, cw, 128, "UMDB: Urban Microbiome Database")
print("wrote", sorted(os.listdir(OUT)), "width", W)
