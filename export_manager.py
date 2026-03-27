# coding: utf-8
"""export_manager.py
===================
Orchestrates export modes (SHP / GDB / both) for ConvertMapGIS.

Public API
----------
export_to_gdb(reader, out_dir, layer_key, log_fn=None) -> str
    Write a single MapGisReader's geodataframe into the batch FileGDB that
    lives in `out_dir`.  Returns the path of the feature class created.

The FileGDB is named  ``output.gdb``  inside ``out_dir`` and is shared
across all layers converted in one session.  Call this function once per
MapGisReader; they all accumulate into the same GDB.

Design notes
------------
* Pure-Python path: geopandas .to_file with driver='OpenFileGDB' (GDAL ≥ 3.6,
  bundled with pyogrio/geopandas in the PyInstaller package).  No osgeo import.
* Geometry normalisation applied before write to avoid "Unsupported geometry type":
    - Polygon → MultiPolygon (face layers always promoted to Multi)
    - LineString → MultiLineString
    - GeometryCollection → extract polygons/lines/points only
    - Null / empty / invalid geometries filtered out (buffer(0) attempted first)
* Feature-class naming convention (ASCII only):
    pt_<8-char hash>  –  points
    ln_<8-char hash>  –  lines / polylines
    pg_<8-char hash>  –  polygons / regions
  The manifest table maps hash → human-readable source filename.
* System tables written inside the GDB directory:
    __layers_manifest.json  –  one row per layer
    __slib_symbols.json     –  full slib symbol detail rows
"""

from __future__ import annotations

import os
import re
import json
import hashlib
from typing import Callable, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    import pymapgis  # noqa: F401 – only for type hints


# ---------------------------------------------------------------------------
# Name helpers
# ---------------------------------------------------------------------------

def _sanitize_fc_name(name: str, max_len: int = 52) -> str:
    """Return a name safe for use as a FileGDB feature-class name.

    Rules: ASCII alphanumeric + underscore only; cannot start with a digit.
    """
    safe = re.sub(r'[^A-Za-z0-9_]', '_', name)
    if safe and safe[0].isdigit():
        safe = '_' + safe
    return safe[:max_len] or '_layer'


def _make_layer_key(filepath: str) -> str:
    """Derive a short, stable, ASCII-only layer key from the source file path.

    Format: <prefix>_<8-char md5>
    where <prefix> is the first 8 ASCII-safe characters of the basename.
    This guarantees uniqueness even when two files share the same basename.

    Example: '研究程度图H.WP' → 'layer_a1b2c3d4'
    """
    abs_path = os.path.abspath(filepath)
    h = hashlib.md5(abs_path.encode('utf-8')).hexdigest()[:8]

    # Try to keep any ASCII letters/digits from the basename as a readable prefix
    base_raw = os.path.splitext(os.path.basename(abs_path))[0]
    ascii_chars = re.sub(r'[^A-Za-z0-9]', '', base_raw)[:8]
    prefix = ascii_chars if ascii_chars else 'layer'
    return f"{prefix}_{h}"


def _shape_prefix(shape_type: str) -> str:
    """Return pt_ / ln_ / pg_ prefix from a MapGIS shape_type string."""
    st = (shape_type or '').lower()
    if 'point' in st or st == 'wt':
        return 'pt_'
    if 'line' in st or st in ('wl',):
        return 'ln_'
    return 'pg_'


# ---------------------------------------------------------------------------
# GDB path helper
# ---------------------------------------------------------------------------

def gdb_path_for_dir(out_dir: str) -> str:
    """Return the shared GDB path for an output directory."""
    return os.path.join(out_dir, 'output.gdb')


# ---------------------------------------------------------------------------
# Geometry normalisation
# ---------------------------------------------------------------------------

def _normalise_geometry(gdf, shape_type: str, log_fn):
    """Return a copy of *gdf* with geometries safe for OpenFileGDB.

    Steps applied:
    1. buffer(0) on invalid geometries (cheap fix for self-intersections).
    2. Drop remaining null / empty / invalid rows (with a warning count).
    3. Promote Polygon → MultiPolygon (and LineString → MultiLineString)
       so the whole layer has a single, uniform geometry type.
    4. Extract only the relevant geometry type from GeometryCollection rows.
    """
    import geopandas as gpd
    from shapely.geometry import (
        MultiPolygon, Polygon,
        MultiLineString, LineString,
        MultiPoint, Point,
        GeometryCollection,
    )
    from shapely.ops import unary_union

    is_polygon = 'pg_' == _shape_prefix(shape_type)
    is_line    = 'ln_' == _shape_prefix(shape_type)
    is_point   = 'pt_' == _shape_prefix(shape_type)

    gdf = gdf.copy()
    geom_col = gdf.geometry.name

    # ── Step 1: attempt buffer(0) on invalid geometries ─────────────────────
    def _fix(g):
        try:
            if g is None or g.is_empty:
                return None
            if not g.is_valid:
                g2 = g.buffer(0)
                return g2 if g2 is not None and not g2.is_empty else None
            return g
        except Exception:
            return None

    gdf[geom_col] = gdf[geom_col].apply(_fix)

    # ── Step 2: drop null / empty ────────────────────────────────────────────
    before = len(gdf)
    gdf = gdf[gdf[geom_col].notna()].copy()
    gdf = gdf[~gdf[geom_col].is_empty].copy()
    dropped = before - len(gdf)
    if dropped:
        log_fn(f"⚠️ 几何归一化：已过滤 {dropped} 个空/无效要素")

    if gdf.empty:
        return gdf

    # ── Step 3 & 4: promote to Multi and extract from GeometryCollection ────
    def _promote(g):
        if isinstance(g, GeometryCollection) and not isinstance(
            g, (MultiPolygon, MultiLineString, MultiPoint)
        ):
            # Extract only the target type from a mixed collection
            if is_polygon:
                parts = [p for p in g.geoms
                         if isinstance(p, (Polygon, MultiPolygon))]
            elif is_line:
                parts = [p for p in g.geoms
                         if isinstance(p, (LineString, MultiLineString))]
            else:
                parts = [p for p in g.geoms
                         if isinstance(p, (Point, MultiPoint))]
            g = unary_union(parts) if parts else None

        if g is None or (hasattr(g, 'is_empty') and g.is_empty):
            return None

        # Promote single → multi
        if is_polygon:
            if isinstance(g, Polygon):
                return MultiPolygon([g])
            if isinstance(g, MultiPolygon):
                return g
            return None  # unexpected type for polygon layer
        if is_line:
            if isinstance(g, LineString):
                return MultiLineString([g])
            if isinstance(g, MultiLineString):
                return g
            return None
        # point: keep as-is (Point and MultiPoint both work)
        return g

    gdf[geom_col] = gdf[geom_col].apply(_promote)
    gdf = gdf[gdf[geom_col].notna()].copy()
    gdf = gdf[~gdf[geom_col].is_empty].copy()

    return gdf


# ---------------------------------------------------------------------------
# Core export function
# ---------------------------------------------------------------------------

def export_to_gdb(
    reader,
    out_dir: str,
    layer_key: Optional[str] = None,
    log_fn: Optional[Callable[[str], None]] = None,
) -> str:
    """Export *reader*'s GeoDataFrame as a feature class inside the shared GDB.

    Parameters
    ----------
    reader    : MapGisReader – already converted (geodataframe populated)
    out_dir   : directory that will contain ``output.gdb``
    layer_key : caller-supplied key; derived from source path if not given.
                NOTE: this value is only used for the manifest / slib tables
                and as the human-readable label.  The actual FC name is always
                an ASCII hash-based name to avoid encoding issues.
    log_fn    : optional callable(str) for progress messages

    Returns
    -------
    str – path of the feature class written  (``<gdb>/<fc_name>``)
    """
    def _log(msg: str):
        if log_fn:
            log_fn(msg)

    gdf = getattr(reader, 'geodataframe', None)
    if gdf is None or len(gdf) == 0:
        raise ValueError("reader.geodataframe is empty or None – nothing to export")

    # ── 1. Stable ASCII key from file path (always used for FC name) ─────────
    filepath = getattr(reader, 'filepath', 'unknown')
    ascii_key = _make_layer_key(filepath)

    # Human-readable label for manifest (can be Chinese, not used in FC name)
    if not layer_key:
        layer_key = ascii_key
    # Inject into GDF for join-back
    reader.set_layer_key(ascii_key)

    # ── 2. Feature-class name (pure ASCII) ───────────────────────────────────
    shape_type = getattr(reader, 'shape_type', '') or ''
    prefix = _shape_prefix(shape_type)
    fc_name = _sanitize_fc_name(prefix + ascii_key)

    # ── 3. GDB path ──────────────────────────────────────────────────────────
    gdb = gdb_path_for_dir(out_dir)
    os.makedirs(out_dir, exist_ok=True)

    # ── 4. Normalise geometry ────────────────────────────────────────────────
    gdf_out = _normalise_geometry(gdf, shape_type, _log)
    if gdf_out.empty:
        raise ValueError("几何归一化后图层为空，跳过 GDB 写入")

    # ── 5. Write feature class ───────────────────────────────────────────────
    fc_path = _write_feature_class(gdf_out, gdb, fc_name, shape_type, _log)

    # ── 6. Write / update manifest ───────────────────────────────────────────
    try:
        _update_manifest(gdb, ascii_key, layer_key, reader, fc_name, gdf_out)
    except Exception as exc:
        _log(f"⚠️ manifest 更新失败（非致命）: {exc}")

    # ── 7. Write / update slib symbols ──────────────────────────────────────
    slib_data = getattr(reader, '_slib_json_data', None)
    if slib_data:
        try:
            _update_slib_table(gdb, ascii_key, slib_data)
        except Exception as exc:
            _log(f"⚠️ slib 符号表更新失败（非致命）: {exc}")

    # ── 8. Optional ArcPy .lyrx ─────────────────────────────────────────────
    try:
        import arcgis_fgdb_helper
        arcgis_fgdb_helper.create_lyrx(
            gdb_path=gdb,
            fc_name=fc_name,
            out_dir=out_dir,
            layer_key=ascii_key,
            shape_type=shape_type,
            log_fn=log_fn,
        )
    except ImportError:
        pass  # ArcGIS Pro not available – silent
    except Exception as exc:
        _log(f"⚠️ .lyrx 生成失败（非致命）: {exc}")

    return fc_path


# ---------------------------------------------------------------------------
# Internal: write feature class
# ---------------------------------------------------------------------------

def _write_feature_class(gdf, gdb: str, fc_name: str, shape_type: str, log_fn) -> str:
    """Write *gdf* into *gdb* as feature class *fc_name* via OpenFileGDB driver.

    Uses pyogrio (bundled with geopandas in the PyInstaller package).
    Passes TARGET_ARCGIS_VERSION to suppress Integer64 warnings.
    """
    fc_path = os.path.join(gdb, fc_name)

    # layer_options for pyogrio's OpenFileGDB driver
    layer_options = [
        'TARGET_ARCGIS_VERSION=ARCGIS_PRO_3_2_OR_LATER',
    ]

    gdf.to_file(
        gdb,
        driver='OpenFileGDB',
        layer=fc_name,
        layer_options=layer_options,
    )
    log_fn(f"✅ GDB 写入成功: {fc_name}")
    return fc_path


# ---------------------------------------------------------------------------
# Internal: manifest table
# ---------------------------------------------------------------------------

def _manifest_path(gdb: str) -> str:
    return os.path.join(gdb, '__layers_manifest.json')


def _update_manifest(gdb: str, ascii_key: str, display_key: str,
                     reader, fc_name: str, gdf):
    """Append / update a JSON manifest file inside the GDB directory."""
    manifest_file = _manifest_path(gdb)
    if os.path.exists(manifest_file):
        with open(manifest_file, 'r', encoding='utf-8') as f:
            manifest = json.load(f)
    else:
        manifest = []

    entry = {
        'layer_key':     ascii_key,
        'display_name':  display_key,
        'fc_name':       fc_name,
        'source_file':   getattr(reader, 'filepath', ''),
        'shape_type':    getattr(reader, 'shape_type', ''),
        'feature_count': len(gdf),
        'crs_wkt':       gdf.crs.to_wkt() if gdf.crs is not None else '',
    }
    manifest = [r for r in manifest if r.get('layer_key') != ascii_key]
    manifest.append(entry)

    with open(manifest_file, 'w', encoding='utf-8') as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# Internal: slib symbols table
# ---------------------------------------------------------------------------

def _slib_table_path(gdb: str) -> str:
    return os.path.join(gdb, '__slib_symbols.json')


def _update_slib_table(gdb: str, ascii_key: str, slib_rows: list):
    """Append symbol rows to the per-GDB slib JSON table."""
    table_file = _slib_table_path(gdb)
    if os.path.exists(table_file):
        with open(table_file, 'r', encoding='utf-8') as f:
            table = json.load(f)
    else:
        table = []

    table = [r for r in table if r.get('layer_key') != ascii_key]
    for i, row in enumerate(slib_rows):
        entry = dict(row)
        entry['layer_key'] = ascii_key
        entry['feat_id']   = i
        table.append(entry)

    with open(table_file, 'w', encoding='utf-8') as f:
        json.dump(table, f, ensure_ascii=False, indent=2)
