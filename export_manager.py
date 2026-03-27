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
* Pure-Python path: uses Fiona (via geopandas .to_file with driver='OpenFileGDB'
  or 'ESRI Shapefile' + ogr2ogr) as the primary route.
* ArcPy path: if ``arcgis_fgdb_helper`` is importable (ArcGIS Pro present),
  it will be used to generate .lyrx files after the feature class is written.
* The ``layer_key`` is injected into the GeoDataFrame before writing so
  every feature carries a stable reference back to the source MapGIS file.
* Feature-class naming convention:
    pt_<layer_key>  –  points
    ln_<layer_key>  –  lines / polylines
    pg_<layer_key>  –  polygons / regions
* System tables written to the GDB:
    __layers_manifest  –  one row per layer (layer_key, source_path,
                          shape_type, feature_count, crs_wkt)
    __slib_symbols     –  full symbol detail rows from slib_json_data
"""

from __future__ import annotations

import os
import re
import json
import tempfile
import hashlib
from typing import Callable, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    import pymapgis  # noqa: F401 – only for type hints

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sanitize_fc_name(name: str, max_len: int = 52) -> str:
    """Return a name safe for use as a FileGDB feature-class name.

    Rules: alphanumeric + underscore only; cannot start with a digit.
    Truncated to *max_len* characters (GDB limit is 160, but keep shorter).
    """
    safe = re.sub(r'[^A-Za-z0-9_]', '_', name)
    if safe and safe[0].isdigit():
        safe = '_' + safe
    return safe[:max_len] or '_layer'


def _layer_key_from_path(filepath: str) -> str:
    """Derive a short, stable layer_key from the source file path.

    Uses the first 8 hex chars of the MD5 of the absolute path so that
    two files with the same basename in different folders get distinct keys.
    Appended to the sanitised basename for readability.
    """
    abs_path = os.path.abspath(filepath)
    h = hashlib.md5(abs_path.encode('utf-8')).hexdigest()[:8]
    base = os.path.splitext(os.path.basename(abs_path))[0]
    safe_base = _sanitize_fc_name(base, max_len=40)
    return f"{safe_base}_{h}"


def _shape_prefix(shape_type: str) -> str:
    """Return pt_ / ln_ / pg_ prefix from a MapGIS shape_type string."""
    st = (shape_type or '').lower()
    if 'point' in st or st == 'wt':
        return 'pt_'
    if 'line' in st or st in ('wl',):
        return 'ln_'
    return 'pg_'


# ---------------------------------------------------------------------------
# GDB path helpers
# ---------------------------------------------------------------------------

def gdb_path_for_dir(out_dir: str) -> str:
    """Return the shared GDB path for an output directory."""
    return os.path.join(out_dir, 'output.gdb')


# ---------------------------------------------------------------------------
# Core export function
# ---------------------------------------------------------------------------

def export_to_gdb(
    reader,                       # pymapgis.MapGisReader instance
    out_dir: str,
    layer_key: Optional[str] = None,
    log_fn: Optional[Callable[[str], None]] = None,
) -> str:
    """Export *reader*'s GeoDataFrame as a feature class inside the shared GDB.

    Parameters
    ----------
    reader    : MapGisReader – already converted (geodataframe is populated)
    out_dir   : directory that will contain ``output.gdb``
    layer_key : stable string key for this layer; derived from source path
                if not provided
    log_fn    : optional callable(str) for progress messages

    Returns
    -------
    str – path of the feature class written  (``<gdb>/fc_name``)
    """
    def _log(msg: str):
        if log_fn:
            log_fn(msg)

    gdf = getattr(reader, 'geodataframe', None)
    if gdf is None or len(gdf) == 0:
        raise ValueError("reader.geodataframe is empty or None – nothing to export")

    # ── 1. Derive / inject layer_key ────────────────────────────────────────
    if not layer_key:
        layer_key = _layer_key_from_path(getattr(reader, 'filepath', 'unknown'))
    reader.set_layer_key(layer_key)

    # ── 2. Feature-class name ────────────────────────────────────────────────
    shape_type = getattr(reader, 'shape_type', '') or ''
    prefix = _shape_prefix(shape_type)
    fc_name = _sanitize_fc_name(prefix + layer_key)

    # ── 3. GDB path ─────────────────────────────────────────────────────────
    gdb = gdb_path_for_dir(out_dir)
    os.makedirs(out_dir, exist_ok=True)

    # ── 4. Write feature class ───────────────────────────────────────────────
    # Try OpenFileGDB driver (GDAL ≥ 3.6 supports write); fall back to GPKG
    # intermediate then ogr conversion.
    fc_path = _write_feature_class(gdf, gdb, fc_name, log_fn=_log)

    # ── 5. Write / update manifest table ────────────────────────────────────
    try:
        _update_manifest(gdb, layer_key, reader, fc_name)
    except Exception as exc:
        _log(f"⚠️ manifest 更新失败（非致命）: {exc}")

    # ── 6. Write / update slib symbols table ────────────────────────────────
    slib_data = getattr(reader, '_slib_json_data', None)
    if slib_data:
        try:
            _update_slib_table(gdb, layer_key, slib_data)
        except Exception as exc:
            _log(f"⚠️ slib 符号表更新失败（非致命）: {exc}")

    # ── 7. Optionally call ArcPy helper for .lyrx generation ────────────────
    try:
        import arcgis_fgdb_helper
        arcgis_fgdb_helper.create_lyrx(
            gdb_path=gdb,
            fc_name=fc_name,
            out_dir=out_dir,
            layer_key=layer_key,
            shape_type=shape_type,
            log_fn=log_fn,
        )
    except ImportError:
        pass  # ArcGIS Pro not available – skip silently
    except Exception as exc:
        _log(f"⚠️ .lyrx 生成失败（非致命）: {exc}")

    return fc_path


# ---------------------------------------------------------------------------
# Internal: write feature class
# ---------------------------------------------------------------------------

def _write_feature_class(gdf, gdb: str, fc_name: str, log_fn) -> str:
    """Write *gdf* into *gdb* as feature class *fc_name*.

    Strategy:
    1. Try geopandas .to_file with driver='OpenFileGDB' (GDAL ≥ 3.6).
    2. On failure, fall back to writing a GeoPackage then converting with
       osgeo.ogr (comes with GDAL/Fiona).
    """
    import geopandas as gpd

    fc_path = os.path.join(gdb, fc_name)
    _exc1_msg = ''

    # --- attempt 1: OpenFileGDB driver (fastest, no extra deps) -------------
    try:
        gdf.to_file(gdb, driver='OpenFileGDB', layer=fc_name)
        log_fn(f"✅ OpenFileGDB 写入成功: {fc_name}")
        return fc_path
    except Exception as exc1:
        _exc1_msg = str(exc1)
        log_fn(f"ℹ️ OpenFileGDB 驱动写入失败，尝试备用方案: {_exc1_msg}")

    # --- attempt 2: osgeo.ogr into GDB ----------------------------------------
    try:
        from osgeo import ogr, osr
        _write_via_ogr(gdf, gdb, fc_name, ogr, osr, log_fn)
        log_fn(f"✅ OGR 写入成功: {fc_name}")
        return fc_path
    except Exception as exc2:
        raise RuntimeError(
            f"无法写入 FileGDB（OpenFileGDB: {_exc1_msg}; OGR: {exc2}）"
        ) from exc2


def _write_via_ogr(gdf, gdb: str, fc_name: str, ogr, osr, log_fn):
    """Write via osgeo.ogr: create GDB if needed, then add layer."""
    # Driver
    drv = ogr.GetDriverByName('OpenFileGDB')
    if drv is None:
        raise RuntimeError("osgeo.ogr: OpenFileGDB 驱动不可用")

    # Open or create GDB
    if os.path.exists(gdb):
        ds = drv.Open(gdb, update=1)
        if ds is None:
            raise RuntimeError(f"无法打开已有 GDB: {gdb}")
    else:
        ds = drv.CreateDataSource(gdb)
        if ds is None:
            raise RuntimeError(f"无法创建 GDB: {gdb}")

    # SRS
    srs = None
    if gdf.crs is not None:
        srs = osr.SpatialReference()
        srs.ImportFromWkt(gdf.crs.to_wkt())

    # Geometry type mapping
    import shapely.geometry as sg
    geom_type_map = {
        'Point':           ogr.wkbPoint,
        'MultiPoint':      ogr.wkbMultiPoint,
        'LineString':      ogr.wkbLineString,
        'MultiLineString': ogr.wkbMultiLineString,
        'Polygon':         ogr.wkbPolygon,
        'MultiPolygon':    ogr.wkbMultiPolygon,
    }
    sample_geom = gdf.geometry.dropna().iloc[0] if not gdf.geometry.dropna().empty else None
    geom_type = ogr.wkbUnknown
    if sample_geom is not None:
        geom_type = geom_type_map.get(sample_geom.geom_type, ogr.wkbUnknown)

    # Delete existing layer if present (overwrite)
    lyr_idx = ds.GetLayerByName(fc_name)
    if lyr_idx is not None:
        ds.DeleteLayer(fc_name)

    lyr = ds.CreateLayer(fc_name, srs=srs, geom_type=geom_type)
    if lyr is None:
        raise RuntimeError(f"无法在 GDB 中创建图层: {fc_name}")

    # Add attribute fields
    attr_cols = [c for c in gdf.columns if c != gdf.geometry.name]
    for col in attr_cols:
        dtype = gdf[col].dtype
        if dtype.kind in ('i', 'u'):
            fd = ogr.FieldDefn(col[:10], ogr.OFTInteger64)
        elif dtype.kind == 'f':
            fd = ogr.FieldDefn(col[:10], ogr.OFTReal)
        else:
            fd = ogr.FieldDefn(col[:10], ogr.OFTString)
            fd.SetWidth(254)
        lyr.CreateField(fd)

    # Write features
    feat_defn = lyr.GetLayerDefn()
    for _, row in gdf.iterrows():
        feat = ogr.Feature(feat_defn)
        geom = row[gdf.geometry.name]
        if geom is not None and not geom.is_empty:
            feat.SetGeometry(ogr.CreateGeometryFromWkb(geom.wkb))
        for col in attr_cols:
            val = row[col]
            try:
                feat.SetField(col[:10], val)
            except Exception:
                feat.SetField(col[:10], str(val))
        lyr.CreateFeature(feat)
        feat = None

    ds.FlushCache()
    ds = None


# ---------------------------------------------------------------------------
# Internal: manifest table
# ---------------------------------------------------------------------------

def _manifest_path(gdb: str) -> str:
    return os.path.join(gdb, '__layers_manifest.json')


def _update_manifest(gdb: str, layer_key: str, reader, fc_name: str):
    """Append / update a JSON manifest file inside the GDB directory."""
    manifest_file = _manifest_path(gdb)
    if os.path.exists(manifest_file):
        with open(manifest_file, 'r', encoding='utf-8') as f:
            manifest = json.load(f)
    else:
        manifest = []

    gdf = reader.geodataframe
    entry = {
        'layer_key':     layer_key,
        'fc_name':       fc_name,
        'source_file':   getattr(reader, 'filepath', ''),
        'shape_type':    getattr(reader, 'shape_type', ''),
        'feature_count': len(gdf),
        'crs_wkt':       gdf.crs.to_wkt() if gdf.crs is not None else '',
    }
    # Replace existing entry for same layer_key
    manifest = [r for r in manifest if r.get('layer_key') != layer_key]
    manifest.append(entry)

    with open(manifest_file, 'w', encoding='utf-8') as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# Internal: slib symbols table
# ---------------------------------------------------------------------------

def _slib_table_path(gdb: str) -> str:
    return os.path.join(gdb, '__slib_symbols.json')


def _update_slib_table(gdb: str, layer_key: str, slib_rows: list):
    """Append symbol rows to the per-GDB slib JSON table."""
    table_file = _slib_table_path(gdb)
    if os.path.exists(table_file):
        with open(table_file, 'r', encoding='utf-8') as f:
            table = json.load(f)
    else:
        table = []

    # Remove existing rows for this layer_key then re-add
    table = [r for r in table if r.get('layer_key') != layer_key]
    for i, row in enumerate(slib_rows):
        entry = dict(row)
        entry['layer_key'] = layer_key
        entry['feat_id']   = i
        table.append(entry)

    with open(table_file, 'w', encoding='utf-8') as f:
        json.dump(table, f, ensure_ascii=False, indent=2)
