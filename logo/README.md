# UMDB logo

| File | Use |
| --- | --- |
| `umdb-logo-light.svg` | Horizontal logo for light backgrounds (README, documents, slides) |
| `umdb-logo-dark.svg` | The same for dark backgrounds |
| `umdb-mark.svg` | The emblem alone, transparent background |
| `umdb-icon.svg` | The emblem on a white rounded tile (avatars, app icons) |

The emblem is a culture dish around a city skyline, with microbes (cocci and a rod) linked like records in a database above it. Colours: teal `#14a085` (dark `#0d7a66`), amber `#f2a33a`.

`make_logo.py` writes all four files (`python3 logo/make_logo.py logo/`; needs matplotlib). The lettering is drawn as outlines from DejaVu Sans (Bitstream Vera licence), so it looks the same everywhere, including GitHub, where SVG images cannot load fonts.

The older raster files here (`umdb_logo.png`, `thumbnail_*.png`, `favicon_*.png`) are the previous artwork.
