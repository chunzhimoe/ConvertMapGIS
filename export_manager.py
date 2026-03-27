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
* System tables written **next to** the GDB directory (not inside it):
    __layers_manifest.json  –  one row per layer
    __slib_symbols.json     –  full slib symbol detail rows
"""

from __future__ import annotations

import os
import re
import json
import hashlib
import glob as _glob
import subprocess
import sys
from typing import Callable, Dict, List, Optional, Tuple, TYPE_CHECKING

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


def _sanitize_gdb_field_names(gdf, log_fn):
    """Rename any GDB-unsafe column names before writing with pyogrio.

    GDB field names must be ASCII alphanumeric + underscore, must not start
    with a digit, and must be ≤ 64 characters.  Any column that already meets
    these rules is left untouched.  Renames are logged so the user can see
    what changed.

    Note: this is intentionally *not* the SHP sanitiser from pymapgis.py,
    which truncates names to 10 chars.  GDB allows up to 64 chars.
    """
    import geopandas as gpd

    geom_col = gdf.geometry.name if hasattr(gdf, 'geometry') else 'geometry'
    rename_map = {}
    used = set()

    for col in gdf.columns:
        if col == geom_col:
            continue

        safe = re.sub(r'[^0-9A-Za-z_]', '_', str(col))
        safe = re.sub(r'_+', '_', safe).strip('_')

        if not safe:
            safe = 'field'
        if safe and safe[0].isdigit():
            safe = f'f_{safe}'

        safe = safe[:64]
        base = safe
        i = 1
        while safe.lower() in used:
            suffix = f'_{i}'
            safe = (base[:64 - len(suffix)] + suffix)[:64]
            i += 1

        used.add(safe.lower())
        if safe != col:
            rename_map[col] = safe

    if rename_map:
        log_fn(f"ℹ️ GDB 字段名已规范化: {rename_map}")

    return gdf.rename(columns=rename_map)


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
# Module-level registry of written feature classes (for post-export ArcPy step)
# ---------------------------------------------------------------------------

# Maps out_dir → list of {'fc_name': str, 'crs_wkid': int|None}
# Populated by export_to_gdb(); consumed by reorganise_gdb_with_arcpy().
_gdb_fc_registry: Dict[str, List[Dict]] = {}


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

    # ── 4b. Sanitise field names for GDB (avoids pyogrio laundering warnings) ─
    gdf_out = _sanitize_gdb_field_names(gdf_out, _log)

    # ── 5. Write feature class ───────────────────────────────────────────────
    fc_path = _write_feature_class(gdf_out, gdb, fc_name, shape_type, _log)

    # ── 5b. Register FC for post-export ArcPy step ───────────────────────────
    crs_wkid = _wkid_from_crs(gdf_out.crs if hasattr(gdf_out, 'crs') else None)
    if out_dir not in _gdb_fc_registry:
        _gdb_fc_registry[out_dir] = []
    # Avoid duplicates (re-running same layer)
    _gdb_fc_registry[out_dir] = [
        r for r in _gdb_fc_registry[out_dir] if r['fc_name'] != fc_name
    ]
    _gdb_fc_registry[out_dir].append({'fc_name': fc_name, 'crs_wkid': crs_wkid})

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


def finalise_gdb(
    out_dir: str,
    log_fn: Optional[Callable[[str], None]] = None,
) -> bool:
    """Post-export step: reorganise output.gdb with ArcPy Feature Datasets.

    Call this **once** after all ``export_to_gdb()`` calls for a session are
    complete.  It reads the module-level FC registry for *out_dir* and calls
    ``reorganise_gdb_with_arcpy()``.

    Returns True if the ArcPy reorganisation succeeded, False otherwise.
    """
    fc_records = _gdb_fc_registry.get(out_dir, [])
    if not fc_records:
        return False

    result = reorganise_gdb_with_arcpy(out_dir, fc_records, log_fn=log_fn)

    # Clear registry for this session so repeated calls are safe
    _gdb_fc_registry.pop(out_dir, None)
    return result


def _downcast_int64_to_int32(gdf, log_fn):
    """Downcast any int64 columns to int32 for ArcMap compatibility.

    ArcMap 10.x (FileGDB) does not support Big Integer (int64) fields.
    This is a belt-and-suspenders guard: upstream code should already emit
    int32, but pandas may infer int64 from Python int lists.
    """
    import numpy as np

    renamed = []
    for col in gdf.columns:
        if col == gdf.geometry.name:
            continue
        if hasattr(gdf[col], 'dtype') and gdf[col].dtype == np.int64:
            gdf[col] = gdf[col].astype(np.int32)
            renamed.append(col)
    if renamed:
        log_fn(f"ℹ️ 已将 int64 字段降级为 int32（ArcMap 兼容）: {renamed}")
    return gdf


def _sanitize_field_types(gdf, log_fn):
    """Convert unsupported field types to ArcMap-safe equivalents.

    ArcMap 10.x FileGDB via OpenFileGDB driver has limited type support:
    - date/datetime/time   → string (ISO format)
    - bool                 → int32 (0 / 1)
    - object (mixed/str)   → string (str cast)
    - complex / other      → string

    This prevents "项目没有定义 / 没有注册类" errors caused by unknown
    field types being written into the GDB.
    """
    import numpy as np
    import pandas as pd

    converted = []
    for col in list(gdf.columns):
        if col == gdf.geometry.name:
            continue
        s = gdf[col]
        dtype = s.dtype

        if dtype == bool or dtype == np.bool_:
            gdf[col] = s.astype(np.int32)
            converted.append((col, 'bool→int32'))
        elif dtype == 'object':
            # object columns may contain datetime objects or mixed types
            gdf[col] = s.apply(
                lambda v: v.isoformat() if hasattr(v, 'isoformat') else (str(v) if v is not None else '')
            )
            converted.append((col, 'object→str'))
        elif hasattr(dtype, 'kind') and dtype.kind == 'M':
            # numpy datetime64 / pandas DatetimeTZDtype
            gdf[col] = s.dt.strftime('%Y-%m-%d %H:%M:%S').fillna('')
            converted.append((col, 'datetime→str'))
        elif hasattr(dtype, 'kind') and dtype.kind == 'm':
            # timedelta
            gdf[col] = s.astype(str)
            converted.append((col, 'timedelta→str'))

    if converted:
        log_fn(f"ℹ️ 字段类型降级（ArcMap 兼容）: {converted}")
    return gdf


# ---------------------------------------------------------------------------
# Internal: write feature class
# ---------------------------------------------------------------------------

def _write_feature_class(gdf, gdb: str, fc_name: str, shape_type: str, log_fn) -> str:
    """Write *gdf* into *gdb* as feature class *fc_name* via OpenFileGDB driver.

    Uses pyogrio (bundled with geopandas in the PyInstaller package).
    TARGET_ARCGIS_VERSION is set to 'ALL' to ensure compatibility with
    ArcMap 10.x (which does not support Big Integer / int64 fields).
    """
    fc_path = os.path.join(gdb, fc_name)

    # Diagnostic: log library versions and field types to aid debugging
    try:
        import geopandas as _gpd
        import pyogrio as _pyogrio
        from osgeo import gdal as _gdal
        log_fn(
            f"ℹ️ 版本诊断 — geopandas={_gpd.__version__} "
            f"pyogrio={_pyogrio.__version__} "
            f"GDAL={_gdal.__version__}"
        )
    except Exception:
        pass
    field_types = {col: str(gdf[col].dtype) for col in gdf.columns if col != gdf.geometry.name}
    log_fn(f"ℹ️ 写入前字段类型: {field_types}")

    # Belt-and-suspenders: downcast any remaining int64 columns to int32
    gdf = _downcast_int64_to_int32(gdf, log_fn)

    # Convert unsupported types (datetime, bool, object) to ArcMap-safe types
    gdf = _sanitize_field_types(gdf, log_fn)

    # layer_options for pyogrio's OpenFileGDB driver (must be a dict)
    # Use 'ALL' (not 'ARCGIS_PRO_3_2_OR_LATER') so the GDB remains compatible
    # with ArcMap 10.x which does not support Big Integer (int64) fields.
    layer_options = {
        'TARGET_ARCGIS_VERSION': 'ALL',
    }

    gdf.to_file(
        gdb,
        driver='OpenFileGDB',
        layer=fc_name,
        layer_options=layer_options,
        promote_to_multi=True,
        engine='pyogrio',
    )
    log_fn(f"✅ GDB 写入成功: {fc_name}")
    return fc_path


# ---------------------------------------------------------------------------
# Internal: manifest table
# ---------------------------------------------------------------------------

def _manifest_path(gdb: str) -> str:
    # Place the manifest next to output.gdb (not inside it) to avoid
    # polluting the FileGDB directory structure.
    return os.path.join(os.path.dirname(gdb), '__layers_manifest.json')


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
    # Place the slib table next to output.gdb (not inside it) to avoid
    # polluting the FileGDB directory structure.
    return os.path.join(os.path.dirname(gdb), '__slib_symbols.json')


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


# ---------------------------------------------------------------------------
# ArcGIS Desktop Python interpreter detection
# ---------------------------------------------------------------------------

def _find_arcgis_desktop_python() -> Optional[str]:
    """Locate the ArcGIS Desktop (ArcMap 10.x) Python 2.7 interpreter.

    Search strategy (Windows only):
    1. Common installation paths for ArcGIS 10.1 – 10.8.
    2. Registry: HKLM\\SOFTWARE\\ESRI\\Desktop<ver>\\PythonDir (via winreg).
    3. Fallback: search C:\\Python27 subdirectories for a python.exe that
       has an 'arcpy' package next to it.

    Returns None on non-Windows or when not found.
    """
    if sys.platform != 'win32':
        return None

    # ── Strategy 1: well-known paths ─────────────────────────────────────────
    candidates: List[str] = []
    for ver in ['10.8', '10.7', '10.6', '10.5', '10.4', '10.3', '10.2', '10.1']:
        for drive in ['C:', 'D:', 'E:']:
            candidates.append(
                os.path.join(drive, os.sep, 'Python27', f'ArcGIS{ver}', 'python.exe')
            )

    for path in candidates:
        if os.path.isfile(path):
            return path

    # ── Strategy 2: registry lookup ──────────────────────────────────────────
    try:
        import winreg
        for ver in ['10.8', '10.7', '10.6', '10.5', '10.4', '10.3', '10.2', '10.1']:
            key_path = f'SOFTWARE\\ESRI\\Desktop{ver}'
            for hive in (winreg.HKEY_LOCAL_MACHINE, winreg.HKEY_CURRENT_USER):
                try:
                    with winreg.OpenKey(hive, key_path) as key:
                        python_dir, _ = winreg.QueryValueEx(key, 'PythonDir')
                        exe = os.path.join(python_dir, 'python.exe')
                        if os.path.isfile(exe):
                            return exe
                except OSError:
                    pass
    except ImportError:
        pass

    # ── Strategy 3: search C:\Python27 subdirs ───────────────────────────────
    base = r'C:\Python27'
    if os.path.isdir(base):
        for sub in os.listdir(base):
            exe = os.path.join(base, sub, 'python.exe')
            arcpy_dir = os.path.join(base, sub, 'Lib', 'site-packages', 'arcpy')
            if os.path.isfile(exe) and os.path.isdir(arcpy_dir):
                return exe

    return None


def _find_arcgis_pro_python() -> Optional[str]:
    """Locate the ArcGIS Pro conda Python interpreter.

    Search strategy:
    1. Common ArcGIS Pro conda env path.
    2. Registry: HKLM\\SOFTWARE\\ESRI\\ArcGIS Pro.
    3. PATH – if 'arcpy' can be imported by the current interpreter.

    Returns None when not found.
    """
    if sys.platform != 'win32':
        return None

    # ── Strategy 1: common paths ─────────────────────────────────────────────
    for drive in ['C:', 'D:']:
        for sub in [
            r'Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe',
            r'ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe',
        ]:
            exe = os.path.join(drive, os.sep, sub)
            if os.path.isfile(exe):
                return exe

    # ── Strategy 2: registry ─────────────────────────────────────────────────
    try:
        import winreg
        key_path = r'SOFTWARE\ESRI\ArcGIS Pro'
        for hive in (winreg.HKEY_LOCAL_MACHINE, winreg.HKEY_CURRENT_USER):
            try:
                with winreg.OpenKey(hive, key_path) as key:
                    install_dir, _ = winreg.QueryValueEx(key, 'InstallDir')
                    exe = os.path.join(
                        install_dir, 'bin', 'Python', 'envs', 'arcgispro-py3', 'python.exe'
                    )
                    if os.path.isfile(exe):
                        return exe
            except OSError:
                pass
    except ImportError:
        pass

    return None


def find_arcgis_python() -> Optional[str]:
    """Return any available ArcGIS Python interpreter (Desktop preferred).

    Preference order: ArcMap 10.x Python 2.7 → ArcGIS Pro Python 3.x.
    Returns None if neither is found.
    """
    return _find_arcgis_desktop_python() or _find_arcgis_pro_python()


# ---------------------------------------------------------------------------
# ArcPy subprocess helper
# ---------------------------------------------------------------------------

def _call_arcpy_helper(
    python_exe: str,
    helper_script: str,
    payload: dict,
    log_fn,
    timeout: int = 300,
) -> dict:
    """Run arcgis_fgdb_helper.py via *python_exe* with *payload* on stdin.

    Parameters
    ----------
    python_exe    : path to the ArcGIS Python interpreter
    helper_script : absolute path to arcgis_fgdb_helper.py
    payload       : dict to serialise as JSON on stdin
    timeout       : seconds to wait for the subprocess

    Returns
    -------
    dict – the JSON result from stdout; {'ok': False, 'error': ...} on failure.
    """
    try:
        proc = subprocess.run(
            [python_exe, helper_script],
            input=json.dumps(payload),
            capture_output=True,
            text=True,
            timeout=timeout,
            encoding='utf-8',
        )
        stdout = proc.stdout.strip()
        stderr = proc.stderr.strip()

        if stderr:
            log_fn(f"ℹ️ ArcPy helper stderr: {stderr[:400]}")

        if not stdout:
            return {'ok': False, 'error': 'arcgis_fgdb_helper returned no output'}

        return json.loads(stdout)
    except subprocess.TimeoutExpired:
        return {'ok': False, 'error': f'arcgis_fgdb_helper timed out after {timeout}s'}
    except Exception as exc:
        return {'ok': False, 'error': str(exc)}


# ---------------------------------------------------------------------------
# CRS grouping helpers
# ---------------------------------------------------------------------------

def _wkid_from_crs(crs) -> Optional[int]:
    """Extract an EPSG WKID integer from a pyproj CRS object.

    Returns None if the CRS is unknown or has no EPSG code.
    """
    if crs is None:
        return None
    try:
        epsg = crs.to_epsg()
        if epsg:
            return int(epsg)
    except Exception:
        pass
    # Fallback: parse from WKT authority node
    try:
        auth, code = crs.to_authority()
        return int(code)
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# Post-export ArcPy GDB reorganisation (called once after all layers written)
# ---------------------------------------------------------------------------

def reorganise_gdb_with_arcpy(
    out_dir: str,
    fc_records: List[Dict],
    log_fn: Optional[Callable[[str], None]] = None,
) -> bool:
    """Reorganise the GDAL-written GDB into ArcPy-native GDB with Feature Datasets.

    This function is called **once** after all feature classes have been
    written to ``output.gdb`` by GDAL/pyogrio.  It:

    1. Detects an ArcGIS Python interpreter on the host.
    2. Creates a new ``output_arcpy.gdb`` via ArcPy (ensures ArcMap-native format).
    3. Groups feature classes by CRS, creates one Feature Dataset per unique CRS.
    4. Copies each feature class from ``output.gdb`` into the correct Dataset.
    5. Renames ``output.gdb`` → ``output_gdal.gdb`` and
              ``output_arcpy.gdb`` → ``output.gdb``
       so downstream code still finds ``output.gdb``.

    Parameters
    ----------
    out_dir    : directory containing ``output.gdb``
    fc_records : list of dicts, each with keys:
                   'fc_name'  – feature class name (ASCII)
                   'crs_wkid' – int EPSG code (may be None)
    log_fn     : optional logger

    Returns
    -------
    bool – True if reorganisation succeeded, False if skipped/failed.
    """
    def _log(msg: str):
        if log_fn:
            log_fn(msg)

    if not fc_records:
        return False

    # ── 1. Find ArcGIS Python ────────────────────────────────────────────────
    python_exe = find_arcgis_python()
    if not python_exe:
        _log("ℹ️ 未找到 ArcGIS Python 解释器，跳过 Feature Dataset 重组（GDB 仍可用，请在 ArcMap 中展开后逐个图层添加）")
        return False

    _log(f"ℹ️ 找到 ArcGIS Python: {python_exe}")

    # ── 2. Locate helper script ───────────────────────────────────────────────
    helper_script = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'arcgis_fgdb_helper.py')
    if not os.path.isfile(helper_script):
        _log(f"⚠️ 找不到 arcgis_fgdb_helper.py: {helper_script}")
        return False

    src_gdb = gdb_path_for_dir(out_dir)           # output.gdb (GDAL-written)
    dst_gdb = os.path.join(out_dir, 'output_arcpy.gdb')

    # ── 3. Build batch payload ───────────────────────────────────────────────
    steps: List[dict] = []

    # 3a. Create destination GDB
    steps.append({'action': 'create_gdb', 'gdb_path': dst_gdb})

    # 3b. Group FC records by WKID (None → 'Unknown_CRS')
    crs_groups: Dict[str, Tuple[int, List[str]]] = {}
    for rec in fc_records:
        wkid = rec.get('crs_wkid')
        if wkid:
            ds_name = f'CRS_{wkid}'
        else:
            ds_name = 'Unknown_CRS'
            wkid = 4326  # fallback SR for unknown CRS dataset

        if ds_name not in crs_groups:
            crs_groups[ds_name] = (wkid, [])
        crs_groups[ds_name][1].append(rec['fc_name'])

    # 3c. Create Feature Datasets
    for ds_name, (wkid, _) in crs_groups.items():
        steps.append({
            'action':   'ensure_feature_dataset',
            'gdb_path': dst_gdb,
            'ds_name':  ds_name,
            'wkid':     wkid,
        })

    # 3d. Copy feature classes
    for ds_name, (_, fc_list) in crs_groups.items():
        for fc_name in fc_list:
            steps.append({
                'action':  'copy_feature_class',
                'src_gdb': src_gdb,
                'fc_name': fc_name,
                'dst_gdb': dst_gdb,
                'ds_name': ds_name,
            })

    batch_payload = {'action': 'batch', 'steps': steps}

    # ── 4. Call helper ───────────────────────────────────────────────────────
    _log(f"⏳ 通过 ArcPy 重组 GDB（共 {len(steps)} 步）...")
    result = _call_arcpy_helper(python_exe, helper_script, batch_payload, _log, timeout=600)

    if not result.get('ok'):
        _log(f"⚠️ ArcPy GDB 重组失败: {result.get('error', '未知错误')}")
        _log("ℹ️ 保留原始 GDAL GDB，请在 ArcMap 中手动展开 output.gdb 逐个添加图层")
        return False

    _log("✅ ArcPy GDB 重组完成")

    # ── 5. Swap GDB names ────────────────────────────────────────────────────
    import shutil
    gdal_gdb = os.path.join(out_dir, 'output_gdal.gdb')
    try:
        if os.path.exists(gdal_gdb):
            shutil.rmtree(gdal_gdb)
        os.rename(src_gdb, gdal_gdb)
        os.rename(dst_gdb, src_gdb)
        _log("✅ 已将 ArcPy GDB 重命名为 output.gdb（原 GDAL GDB 保存为 output_gdal.gdb）")
    except Exception as exc:
        _log(f"⚠️ GDB 重命名失败（非致命）: {exc} — ArcPy GDB 保存在 output_arcpy.gdb")

    return True
