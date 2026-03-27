# coding: utf-8
"""export_manager.py
===================
Orchestrates export modes (SHP / GDB-ArcMap / GDB-Pro) for ConvertMapGIS.

Public API
----------
export_to_gdb(reader, out_dir, layer_key, gdb_profile, log_fn) -> str
    Write a single MapGisReader's GeoDataFrame into a shared FileGDB.
    gdb_profile: 'arcmap' | 'pro'
      'arcmap' -> output_arcmap.gdb  TARGET_ARCGIS_VERSION='ALL'
                  int64->int32, bool->int32, datetime->str, strict field types
      'pro'    -> output_pro.gdb     TARGET_ARCGIS_VERSION='ARCGIS_PRO_3_2_OR_LATER'
                  no field-type downgrade; keeps native types where possible

finalise_gdb(out_dir, gdb_profile, log_fn) -> bool
    Post-export step: called once after all layers are written.
    For 'arcmap': reorganises GDB using ArcMap Python 2.7 ArcPy
                  (create Feature Datasets grouped by CRS).
    For 'pro':    reorganises GDB using ArcGIS Pro Python 3 ArcPy.

Output file names
-----------------
    output_arcmap.gdb   – ArcMap 10.x compatible FileGDB
    output_pro.gdb      – ArcGIS Pro compatible FileGDB

ArcPy subprocess helpers
-------------------------
    arcgis_fgdb_helper_arcmap.py  – Python 2/3 compatible, for ArcMap
    arcgis_fgdb_helper_pro.py     – Python 3 only, for ArcGIS Pro
"""

from __future__ import annotations

import os
import re
import sys
import json
import hashlib
import subprocess
import shutil
from typing import Callable, Dict, List, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    import pymapgis  # noqa: F401


# ---------------------------------------------------------------------------
# GDB profile constants
# ---------------------------------------------------------------------------

PROFILE_ARCMAP = 'arcmap'
PROFILE_PRO    = 'pro'


def gdb_filename(gdb_profile: str) -> str:
    """Return the output GDB directory name for a given profile."""
    if gdb_profile == PROFILE_PRO:
        return 'output_pro.gdb'
    return 'output_arcmap.gdb'


def gdb_path_for_dir(out_dir: str, gdb_profile: str = PROFILE_ARCMAP) -> str:
    """Return the full path to the shared GDB for an output directory."""
    return os.path.join(out_dir, gdb_filename(gdb_profile))


# ---------------------------------------------------------------------------
# Name helpers
# ---------------------------------------------------------------------------

def _sanitize_fc_name(name: str, max_len: int = 52) -> str:
    safe = re.sub(r'[^A-Za-z0-9_]', '_', name)
    if safe and safe[0].isdigit():
        safe = '_' + safe
    return safe[:max_len] or '_layer'


def _make_layer_key(filepath: str) -> str:
    abs_path = os.path.abspath(filepath)
    h = hashlib.md5(abs_path.encode('utf-8')).hexdigest()[:8]
    base_raw = os.path.splitext(os.path.basename(abs_path))[0]
    ascii_chars = re.sub(r'[^A-Za-z0-9]', '', base_raw)[:8]
    prefix = ascii_chars if ascii_chars else 'layer'
    return f"{prefix}_{h}"


def _shape_prefix(shape_type: str) -> str:
    st = (shape_type or '').lower()
    if 'point' in st or st == 'wt':
        return 'pt_'
    if 'line' in st or st in ('wl',):
        return 'ln_'
    return 'pg_'


def _sanitize_gdb_field_names(gdf, log_fn):
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
# Geometry normalisation
# ---------------------------------------------------------------------------

def _normalise_geometry(gdf, shape_type: str, log_fn):
    from shapely.geometry import (
        MultiPolygon, Polygon,
        MultiLineString, LineString,
        MultiPoint, Point,
        GeometryCollection,
    )
    from shapely.ops import unary_union

    is_polygon = _shape_prefix(shape_type) == 'pg_'
    is_line    = _shape_prefix(shape_type) == 'ln_'

    gdf = gdf.copy()
    geom_col = gdf.geometry.name

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

    before = len(gdf)
    gdf = gdf[gdf[geom_col].notna()].copy()
    gdf = gdf[~gdf[geom_col].is_empty].copy()
    dropped = before - len(gdf)
    if dropped:
        log_fn(f"⚠️ 几何归一化：已过滤 {dropped} 个空/无效要素")

    if gdf.empty:
        return gdf

    def _promote(g):
        if isinstance(g, GeometryCollection) and not isinstance(
            g, (MultiPolygon, MultiLineString, MultiPoint)
        ):
            if is_polygon:
                parts = [p for p in g.geoms if isinstance(p, (Polygon, MultiPolygon))]
            elif is_line:
                parts = [p for p in g.geoms if isinstance(p, (LineString, MultiLineString))]
            else:
                parts = [p for p in g.geoms if isinstance(p, (Point, MultiPoint))]
            g = unary_union(parts) if parts else None

        if g is None or (hasattr(g, 'is_empty') and g.is_empty):
            return None

        if is_polygon:
            if isinstance(g, Polygon):
                return MultiPolygon([g])
            return g if isinstance(g, MultiPolygon) else None
        if is_line:
            if isinstance(g, LineString):
                return MultiLineString([g])
            return g if isinstance(g, MultiLineString) else None
        return g

    gdf[geom_col] = gdf[geom_col].apply(_promote)
    gdf = gdf[gdf[geom_col].notna()].copy()
    gdf = gdf[~gdf[geom_col].is_empty].copy()
    return gdf


# ---------------------------------------------------------------------------
# Field type normalisation  (profile-aware)
# ---------------------------------------------------------------------------

def _downcast_int64_to_int32(gdf, log_fn):
    """ArcMap 10.x does not support int64 (Big Integer). Downcast to int32."""
    import numpy as np
    renamed = []
    for col in gdf.columns:
        if col == gdf.geometry.name:
            continue
        if hasattr(gdf[col], 'dtype') and gdf[col].dtype == np.int64:
            gdf[col] = gdf[col].astype(np.int32)
            renamed.append(col)
    if renamed:
        log_fn(f"ℹ️ int64→int32 降级（ArcMap 兼容）: {renamed}")
    return gdf


def _sanitize_field_types_arcmap(gdf, log_fn):
    """Convert types that ArcMap 10.x FileGDB cannot handle.

    Converts: bool→int32, object→str, datetime→str, timedelta→str.
    This prevents the '没有注册类' error in ArcMap.
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
            gdf[col] = s.apply(
                lambda v: v.isoformat() if hasattr(v, 'isoformat')
                else (str(v) if v is not None else '')
            )
            converted.append((col, 'object→str'))
        elif hasattr(dtype, 'kind') and dtype.kind == 'M':
            gdf[col] = s.dt.strftime('%Y-%m-%d %H:%M:%S').fillna('')
            converted.append((col, 'datetime→str'))
        elif hasattr(dtype, 'kind') and dtype.kind == 'm':
            gdf[col] = s.astype(str)
            converted.append((col, 'timedelta→str'))

    if converted:
        log_fn(f"ℹ️ 字段类型降级（ArcMap 兼容）: {converted}")
    return gdf


def _sanitize_field_types_pro(gdf, log_fn):
    """Light field sanitisation for ArcGIS Pro – keep native types where safe.

    Pro supports: int64 (BigInteger), bool (as SmallInt via GDAL), datetime.
    Only convert object columns that are truly mixed-type or contain non-str.
    """
    import numpy as np

    converted = []
    for col in list(gdf.columns):
        if col == gdf.geometry.name:
            continue
        s = gdf[col]
        dtype = s.dtype

        if dtype == 'object':
            # Check if the column is purely strings already
            non_str = s.dropna().apply(lambda v: not isinstance(v, str))
            if non_str.any():
                gdf[col] = s.apply(
                    lambda v: v.isoformat() if hasattr(v, 'isoformat')
                    else (str(v) if v is not None else '')
                )
                converted.append((col, 'mixed-object→str'))

    if converted:
        log_fn(f"ℹ️ 字段类型轻量处理（Pro 兼容）: {converted}")
    return gdf


# ---------------------------------------------------------------------------
# Module-level FC registry (per out_dir + profile)
# ---------------------------------------------------------------------------

# Key: (out_dir, gdb_profile)  Value: list of {'fc_name': str, 'crs_wkid': int|None}
_gdb_fc_registry: Dict[Tuple[str, str], List[Dict]] = {}


# ---------------------------------------------------------------------------
# Core export function
# ---------------------------------------------------------------------------

def export_to_gdb(
    reader,
    out_dir: str,
    layer_key: Optional[str] = None,
    gdb_profile: str = PROFILE_ARCMAP,
    log_fn: Optional[Callable[[str], None]] = None,
) -> str:
    """Export *reader*'s GeoDataFrame as a feature class inside the shared GDB.

    Parameters
    ----------
    reader      : MapGisReader – already converted
    out_dir     : directory that will contain the output GDB
    layer_key   : human-readable label (Chinese OK); FC name derived separately
    gdb_profile : 'arcmap' or 'pro'
    log_fn      : optional progress logger

    Returns
    -------
    str – path of the feature class written (``<gdb>/<fc_name>``)
    """
    def _log(msg: str):
        if log_fn:
            log_fn(msg)

    gdf = getattr(reader, 'geodataframe', None)
    if gdf is None or len(gdf) == 0:
        raise ValueError("reader.geodataframe is empty or None – nothing to export")

    # ── 1. Keys & names ──────────────────────────────────────────────────────
    filepath  = getattr(reader, 'filepath', 'unknown')
    ascii_key = _make_layer_key(filepath)
    if not layer_key:
        layer_key = ascii_key
    reader.set_layer_key(ascii_key)

    shape_type = getattr(reader, 'shape_type', '') or ''
    prefix     = _shape_prefix(shape_type)
    fc_name    = _sanitize_fc_name(prefix + ascii_key)

    # ── 2. GDB path ──────────────────────────────────────────────────────────
    gdb = gdb_path_for_dir(out_dir, gdb_profile)
    os.makedirs(out_dir, exist_ok=True)

    # ── 3. Normalise geometry ────────────────────────────────────────────────
    gdf_out = _normalise_geometry(gdf, shape_type, _log)
    if gdf_out.empty:
        raise ValueError("几何归一化后图层为空，跳过 GDB 写入")

    gdf_out = _sanitize_gdb_field_names(gdf_out, _log)

    # ── 4. Write feature class (profile-specific) ────────────────────────────
    fc_path = _write_feature_class(gdf_out, gdb, fc_name, shape_type,
                                   gdb_profile, _log)

    # ── 5. Register FC for post-export ArcPy step ────────────────────────────
    reg_key = (out_dir, gdb_profile)
    crs_wkid = _wkid_from_crs(gdf_out.crs if hasattr(gdf_out, 'crs') else None)
    if reg_key not in _gdb_fc_registry:
        _gdb_fc_registry[reg_key] = []
    _gdb_fc_registry[reg_key] = [
        r for r in _gdb_fc_registry[reg_key] if r['fc_name'] != fc_name
    ]
    _gdb_fc_registry[reg_key].append({'fc_name': fc_name, 'crs_wkid': crs_wkid})

    # ── 6. Manifest ──────────────────────────────────────────────────────────
    try:
        _update_manifest(gdb, ascii_key, layer_key, reader, fc_name, gdf_out)
    except Exception as exc:
        _log(f"⚠️ manifest 更新失败（非致命）: {exc}")

    # ── 7. slib symbols ──────────────────────────────────────────────────────
    slib_data = getattr(reader, '_slib_json_data', None)
    if slib_data:
        try:
            _update_slib_table(gdb, ascii_key, slib_data)
        except Exception as exc:
            _log(f"⚠️ slib 符号表更新失败（非致命）: {exc}")

    return fc_path


def finalise_gdb(
    out_dir: str,
    gdb_profile: str = PROFILE_ARCMAP,
    log_fn: Optional[Callable[[str], None]] = None,
) -> bool:
    """Post-export ArcPy reorganisation step.

    Reads the FC registry for (out_dir, gdb_profile) and reorganises the GDB
    by creating Feature Datasets grouped by CRS via the appropriate ArcPy
    helper script.

    Returns True if reorganisation succeeded, False if skipped/failed.
    """
    reg_key    = (out_dir, gdb_profile)
    fc_records = _gdb_fc_registry.get(reg_key, [])
    if not fc_records:
        return False

    result = _reorganise_with_arcpy(out_dir, gdb_profile, fc_records, log_fn)
    _gdb_fc_registry.pop(reg_key, None)
    return result


# ---------------------------------------------------------------------------
# Internal: write feature class (profile-aware)
# ---------------------------------------------------------------------------

def _write_feature_class(
    gdf, gdb: str, fc_name: str, shape_type: str,
    gdb_profile: str, log_fn
) -> str:
    import geopandas as _gpd
    try:
        import pyogrio as _pyogrio
        log_fn(f"ℹ️ geopandas={_gpd.__version__} pyogrio={_pyogrio.__version__}")
    except Exception:
        pass

    field_types = {col: str(gdf[col].dtype)
                   for col in gdf.columns if col != gdf.geometry.name}
    log_fn(f"ℹ️ 写入前字段类型: {field_types}")

    if gdb_profile == PROFILE_ARCMAP:
        # ArcMap: strict downgrade
        gdf = _downcast_int64_to_int32(gdf, log_fn)
        gdf = _sanitize_field_types_arcmap(gdf, log_fn)
        layer_options = {'TARGET_ARCGIS_VERSION': 'ALL'}
    else:
        # Pro: light sanitisation only
        gdf = _sanitize_field_types_pro(gdf, log_fn)
        layer_options = {'TARGET_ARCGIS_VERSION': 'ARCGIS_PRO_3_2_OR_LATER'}

    gdf.to_file(
        gdb,
        driver='OpenFileGDB',
        layer=fc_name,
        layer_options=layer_options,
        promote_to_multi=True,
        engine='pyogrio',
    )
    log_fn(f"✅ GDB 写入成功 [{gdb_profile}]: {fc_name}")
    return os.path.join(gdb, fc_name)


# ---------------------------------------------------------------------------
# Internal: manifest
# ---------------------------------------------------------------------------

def _manifest_path(gdb: str) -> str:
    return os.path.join(os.path.dirname(gdb), '__layers_manifest.json')


def _update_manifest(gdb: str, ascii_key: str, display_key: str,
                     reader, fc_name: str, gdf):
    mf = _manifest_path(gdb)
    if os.path.exists(mf):
        with open(mf, 'r', encoding='utf-8') as f:
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

    with open(mf, 'w', encoding='utf-8') as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# Internal: slib symbols
# ---------------------------------------------------------------------------

def _slib_table_path(gdb: str) -> str:
    return os.path.join(os.path.dirname(gdb), '__slib_symbols.json')


def _update_slib_table(gdb: str, ascii_key: str, slib_rows: list):
    tf = _slib_table_path(gdb)
    if os.path.exists(tf):
        with open(tf, 'r', encoding='utf-8') as f:
            table = json.load(f)
    else:
        table = []

    table = [r for r in table if r.get('layer_key') != ascii_key]
    for i, row in enumerate(slib_rows):
        entry = dict(row)
        entry['layer_key'] = ascii_key
        entry['feat_id']   = i
        table.append(entry)

    with open(tf, 'w', encoding='utf-8') as f:
        json.dump(table, f, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# CRS helpers
# ---------------------------------------------------------------------------

def _wkid_from_crs(crs) -> Optional[int]:
    if crs is None:
        return None
    try:
        epsg = crs.to_epsg()
        if epsg:
            return int(epsg)
    except Exception:
        pass
    try:
        auth, code = crs.to_authority()
        return int(code)
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# ArcGIS Python interpreter detection
# ---------------------------------------------------------------------------

def _find_arcgis_desktop_python() -> Optional[str]:
    """Locate ArcMap 10.x Python 2.7 interpreter (Windows only)."""
    if sys.platform != 'win32':
        return None

    # Strategy 1: well-known paths
    for ver in ['10.8', '10.7', '10.6', '10.5', '10.4', '10.3', '10.2', '10.1']:
        for drive in ['C:', 'D:', 'E:']:
            p = os.path.join(drive, os.sep, 'Python27', f'ArcGIS{ver}', 'python.exe')
            if os.path.isfile(p):
                return p

    # Strategy 2: registry
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

    # Strategy 3: scan C:\Python27
    base = r'C:\Python27'
    if os.path.isdir(base):
        for sub in os.listdir(base):
            exe     = os.path.join(base, sub, 'python.exe')
            arcpy_d = os.path.join(base, sub, 'Lib', 'site-packages', 'arcpy')
            if os.path.isfile(exe) and os.path.isdir(arcpy_d):
                return exe

    return None


def _find_arcgis_pro_python() -> Optional[str]:
    """Locate ArcGIS Pro conda Python 3 interpreter (Windows only)."""
    if sys.platform != 'win32':
        return None

    # Strategy 1: common paths
    for drive in ['C:', 'D:']:
        p = os.path.join(
            drive, os.sep,
            'Program Files', 'ArcGIS', 'Pro', 'bin',
            'Python', 'envs', 'arcgispro-py3', 'python.exe'
        )
        if os.path.isfile(p):
            return p

    # Strategy 2: registry
    try:
        import winreg
        key_path = r'SOFTWARE\ESRI\ArcGIS Pro'
        for hive in (winreg.HKEY_LOCAL_MACHINE, winreg.HKEY_CURRENT_USER):
            try:
                with winreg.OpenKey(hive, key_path) as key:
                    install_dir, _ = winreg.QueryValueEx(key, 'InstallDir')
                    exe = os.path.join(
                        install_dir, 'bin', 'Python',
                        'envs', 'arcgispro-py3', 'python.exe'
                    )
                    if os.path.isfile(exe):
                        return exe
            except OSError:
                pass
    except ImportError:
        pass

    return None


def _find_arcgis_python(gdb_profile: str) -> Optional[str]:
    """Return ArcGIS Python for the given profile."""
    if gdb_profile == PROFILE_PRO:
        return _find_arcgis_pro_python()
    return _find_arcgis_desktop_python()


# ---------------------------------------------------------------------------
# Helper-script path resolution (works both in source and PyInstaller EXE)
# ---------------------------------------------------------------------------

def _helper_script_path(gdb_profile: str) -> Optional[str]:
    """Locate the correct helper .py on disk.

    In a PyInstaller one-folder build the helpers are placed next to the EXE
    (added via datas in the .spec).  In source mode they sit next to this file.
    """
    filename = (
        'arcgis_fgdb_helper_pro.py'
        if gdb_profile == PROFILE_PRO
        else 'arcgis_fgdb_helper_arcmap.py'
    )

    # PyInstaller sets sys._MEIPASS for the extraction temp dir,
    # but for onefolder builds the scripts are next to the EXE.
    candidates = []

    # 1. Next to the running EXE / script
    exe_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
    candidates.append(os.path.join(exe_dir, filename))

    # 2. PyInstaller _MEIPASS (onefile builds)
    meipass = getattr(sys, '_MEIPASS', None)
    if meipass:
        candidates.append(os.path.join(meipass, filename))

    # 3. Next to this source file (development)
    candidates.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), filename))

    for p in candidates:
        if os.path.isfile(p):
            return p

    return None


# ---------------------------------------------------------------------------
# ArcPy subprocess call
# ---------------------------------------------------------------------------

def _call_arcpy_helper(
    python_exe: str,
    helper_script: str,
    payload: dict,
    log_fn,
    timeout: int = 600,
) -> dict:
    try:
        proc = subprocess.run(
            [python_exe, helper_script],
            input=json.dumps(payload),
            capture_output=True,
            text=True,
            timeout=timeout,
            encoding='utf-8',
        )
        if proc.stderr.strip():
            log_fn(f"ℹ️ ArcPy stderr: {proc.stderr.strip()[:400]}")
        if not proc.stdout.strip():
            return {'ok': False, 'error': 'helper returned no output'}
        return json.loads(proc.stdout.strip())
    except subprocess.TimeoutExpired:
        return {'ok': False, 'error': f'helper timed out after {timeout}s'}
    except Exception as exc:
        return {'ok': False, 'error': str(exc)}


# ---------------------------------------------------------------------------
# ArcPy GDB reorganisation
# ---------------------------------------------------------------------------

def _reorganise_with_arcpy(
    out_dir: str,
    gdb_profile: str,
    fc_records: List[Dict],
    log_fn: Optional[Callable[[str], None]] = None,
) -> bool:
    """Reorganise GDB using ArcPy: create Feature Datasets grouped by CRS.

    Workflow:
      1. Find the appropriate ArcGIS Python interpreter.
      2. Locate helper script on disk.
      3. Build batch payload (create_gdb → ensure_feature_dataset × N → copy_feature_class × M).
      4. Run helper via subprocess.
      5. Swap GDB names so the result keeps the canonical name.

    The intermediate GDB is named ``output_arcmap_tmp.gdb`` or ``output_pro_tmp.gdb``
    and, on success, is renamed to the final name (``output_arcmap.gdb`` /
    ``output_pro.gdb``).  The original GDAL-written GDB is moved to
    ``output_arcmap_gdal.gdb`` / ``output_pro_gdal.gdb`` as a backup.
    """
    def _log(msg: str):
        if log_fn:
            log_fn(msg)

    # ── 1. Find Python ───────────────────────────────────────────────────────
    python_exe = _find_arcgis_python(gdb_profile)
    if not python_exe:
        label = 'ArcMap 10.x' if gdb_profile == PROFILE_ARCMAP else 'ArcGIS Pro'
        _log(f"ℹ️ 未找到 {label} Python 解释器，跳过 Feature Dataset 重组")
        _log("ℹ️ GDB 仍可用 —— 在 ArcMap/Pro 中展开 GDB 后逐个图层加载即可")
        return False

    _log(f"ℹ️ 找到 ArcGIS Python [{gdb_profile}]: {python_exe}")

    # ── 2. Locate helper ─────────────────────────────────────────────────────
    helper_script = _helper_script_path(gdb_profile)
    if not helper_script:
        _log(f"⚠️ 找不到 helper 脚本（{gdb_profile}），跳过重组")
        return False

    # ── 3. Build batch payload ───────────────────────────────────────────────
    src_gdb = gdb_path_for_dir(out_dir, gdb_profile)   # GDAL-written GDB
    tmp_suffix = '_tmp'
    tmp_gdb = src_gdb[:-4] + tmp_suffix + '.gdb'       # e.g. output_arcmap_tmp.gdb

    steps: List[dict] = [{'action': 'create_gdb', 'gdb_path': tmp_gdb}]

    # Group by CRS
    crs_groups: Dict[str, Tuple[int, List[str]]] = {}
    for rec in fc_records:
        wkid = rec.get('crs_wkid')
        if wkid:
            ds_name = f'CRS_{wkid}'
        else:
            ds_name = 'Unknown_CRS'
            wkid    = 4326
        if ds_name not in crs_groups:
            crs_groups[ds_name] = (wkid, [])
        crs_groups[ds_name][1].append(rec['fc_name'])

    for ds_name, (wkid, _) in crs_groups.items():
        steps.append({
            'action':   'ensure_feature_dataset',
            'gdb_path': tmp_gdb,
            'ds_name':  ds_name,
            'wkid':     wkid,
        })

    for ds_name, (_, fc_list) in crs_groups.items():
        for fc_name in fc_list:
            steps.append({
                'action':  'copy_feature_class',
                'src_gdb': src_gdb,
                'fc_name': fc_name,
                'dst_gdb': tmp_gdb,
                'ds_name': ds_name,
            })

    _log(f"⏳ ArcPy 重组 GDB [{gdb_profile}]，共 {len(steps)} 步，"
         f"{len(crs_groups)} 个坐标系分组，{len(fc_records)} 个图层…")

    result = _call_arcpy_helper(python_exe, helper_script,
                                {'action': 'batch', 'steps': steps}, _log)

    if not result.get('ok'):
        _log(f"⚠️ ArcPy 重组失败: {result.get('error', '未知错误')}")
        # Clean up tmp GDB if it was partially created
        if os.path.exists(tmp_gdb):
            try:
                shutil.rmtree(tmp_gdb)
            except Exception:
                pass
        return False

    _log(f"✅ ArcPy 重组完成 [{gdb_profile}]")

    # ── 4. Swap GDB names ────────────────────────────────────────────────────
    gdal_backup = src_gdb[:-4] + '_gdal.gdb'
    try:
        if os.path.exists(gdal_backup):
            shutil.rmtree(gdal_backup)
        os.rename(src_gdb, gdal_backup)    # GDAL version → backup
        os.rename(tmp_gdb, src_gdb)        # ArcPy version → canonical name
        _log(f"✅ ArcPy GDB 已就位: {os.path.basename(src_gdb)}"
             f"（原 GDAL 版本备份为 {os.path.basename(gdal_backup)}）")
    except Exception as exc:
        _log(f"⚠️ GDB 重命名失败（非致命）: {exc}"
             f" — ArcPy GDB 保存在 {os.path.basename(tmp_gdb)}")

    return True
