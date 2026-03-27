# coding: utf-8
"""arcgis_fgdb_helper.py
========================
ArcPy-dependent operations for FileGDB post-processing.

This module is **optional**.  It is only imported when ArcGIS Pro / ArcPy
is available on the host machine.  ``export_manager`` attempts
``import arcgis_fgdb_helper`` inside a try/except; if the import fails
(e.g., on macOS or a machine without ArcGIS Pro), the GDB is still
written via pure GDAL – only the .lyrx generation is skipped.

Functions
---------
create_lyrx(gdb_path, fc_name, out_dir, layer_key, shape_type, log_fn=None)
    Generate a .lyrx file for the given feature class using ArcPy.
    Applies basic labeling if the feature class contains a 'text' field
    (which MapGIS point layers often carry).

apply_symbology_from_slib(gdb_path, fc_name, slib_rows, log_fn=None)
    (V2 – not yet implemented)  Translate MapGIS symbol info into CIM
    symbology and apply it to the feature class layer.

Usage (called from export_manager)
-----------------------------------
    import arcgis_fgdb_helper
    arcgis_fgdb_helper.create_lyrx(
        gdb_path=r"C:\\out\\output.gdb",
        fc_name="pt_myLayer_abc12345",
        out_dir=r"C:\\out",
        layer_key="myLayer_abc12345",
        shape_type="wt",
        log_fn=print,
    )

Running as a standalone script (subprocess mode)
-------------------------------------------------
When this file is executed directly (``python arcgis_fgdb_helper.py``),
it reads a JSON payload from stdin describing the operation and writes a
JSON result to stdout.  This lets the main PyInstaller process call ArcPy
operations without importing arcpy into its own process space (which would
break packaging).

Stdin payload schema:
    {
        "action":     "create_lyrx",
        "gdb_path":   "...",
        "fc_name":    "...",
        "out_dir":    "...",
        "layer_key":  "...",
        "shape_type": "...",
        "lyrx_path":  "..."   // optional override
    }

Stdout:
    {"ok": true, "lyrx_path": "..."} | {"ok": false, "error": "..."}
"""

from __future__ import annotations

import os
import json
from typing import Callable, List, Optional


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def create_lyrx(
    gdb_path: str,
    fc_name: str,
    out_dir: str,
    layer_key: str,
    shape_type: str = '',
    log_fn: Optional[Callable[[str], None]] = None,
    lyrx_path: Optional[str] = None,
) -> str:
    """Create a .lyrx file for *fc_name* in *gdb_path* using ArcPy.

    Parameters
    ----------
    gdb_path  : absolute path to the FileGDB
    fc_name   : feature class name inside the GDB
    out_dir   : directory where the .lyrx file will be saved
    layer_key : used as the base name for the .lyrx file
    shape_type: 'wt' / 'wl' / 'wp' (MapGIS type) – used for label heuristics
    log_fn    : optional callable for progress messages
    lyrx_path : explicit output path; derived from out_dir + layer_key if None

    Returns
    -------
    str – path of the written .lyrx file
    """
    import arcpy  # raises ImportError if ArcGIS Pro not installed

    def _log(msg: str):
        if log_fn:
            log_fn(msg)

    fc_path = os.path.join(gdb_path, fc_name)
    if not lyrx_path:
        lyrx_path = os.path.join(out_dir, f"{layer_key}.lyrx")

    # ── 1. Make a feature layer in-memory ────────────────────────────────────
    lyr_name = f"tmp_{fc_name}"
    arcpy.management.MakeFeatureLayer(fc_path, lyr_name)

    # ── 2. Basic labeling: if 'text' field exists, use it for labels ──────────
    lyr_obj = arcpy.mp.ArcGISProject("CURRENT").listMaps()[0].listLayers(lyr_name)[0] \
        if False else _get_standalone_layer(lyr_name, arcpy)
    # Note: we work with the standalone layer object (not tied to a project)
    # for portability.

    _apply_basic_labeling(lyr_obj, shape_type, arcpy, _log)

    # ── 3. Save to .lyrx ─────────────────────────────────────────────────────
    arcpy.management.SaveToLayerFile(lyr_name, lyrx_path, "ABSOLUTE")
    _log(f"📄 .lyrx 已生成: {os.path.basename(lyrx_path)}")

    # ── 4. Clean up in-memory layer ─────────────────────────────────────────
    try:
        arcpy.management.Delete(lyr_name)
    except Exception:
        pass

    return lyrx_path


def apply_symbology_from_slib(
    gdb_path: str,
    fc_name: str,
    slib_rows: List[dict],
    log_fn: Optional[Callable[[str], None]] = None,
):
    """(V2 – placeholder) Apply CIM symbology derived from MapGIS slib data."""
    # TODO: translate slib_rows into CIM UniqueValueRenderer / symbols
    raise NotImplementedError("apply_symbology_from_slib is a V2 feature")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _get_standalone_layer(lyr_name: str, arcpy):
    """Return a Layer object for an in-memory feature layer (no project needed)."""
    # arcpy.mp requires an ArcGIS Pro project context; for standalone scripts
    # we use arcpy.mapping (ArcMap) or just pass the layer name string to
    # SaveToLayerFile directly.  SaveToLayerFile accepts a layer name string.
    return lyr_name  # SaveToLayerFile accepts name string directly


def _apply_basic_labeling(lyr_or_name, shape_type: str, arcpy, log_fn):
    """Enable labels using the 'text' field if present (MapGIS annotation points)."""
    # Detect whether 'text' column exists
    fields = [f.name.lower() for f in arcpy.ListFields(lyr_or_name)]
    label_field = None
    for candidate in ('text', 'txt', 'label', 'name'):
        if candidate in fields:
            label_field = candidate
            break

    if label_field is None:
        return  # nothing to label

    # Build simple label expression
    expr = f'[{label_field}]'
    try:
        arcpy.management.AddLabelClass(
            lyr_or_name,
            "Default",
            where_clause="",
            expression=expr,
            expression_type="VBScript",
            priority="",
            symbol_id="",
        )
    except Exception as exc:
        log_fn(f"ℹ️ 标注设置失败（非致命）: {exc}")


# ---------------------------------------------------------------------------
# Subprocess / standalone entry point
# ---------------------------------------------------------------------------

def _run_from_stdin():
    """Read a JSON payload from stdin, execute the requested action, print result."""
    import sys
    payload = json.load(sys.stdin)
    action = payload.get('action', '')
    try:
        if action == 'create_lyrx':
            result_path = create_lyrx(
                gdb_path=payload['gdb_path'],
                fc_name=payload['fc_name'],
                out_dir=payload['out_dir'],
                layer_key=payload['layer_key'],
                shape_type=payload.get('shape_type', ''),
                lyrx_path=payload.get('lyrx_path'),
            )
            print(json.dumps({'ok': True, 'lyrx_path': result_path}))
        else:
            print(json.dumps({'ok': False, 'error': f'Unknown action: {action}'}))
    except Exception as exc:
        import traceback
        print(json.dumps({
            'ok': False,
            'error': str(exc),
            'traceback': traceback.format_exc(),
        }))


if __name__ == '__main__':
    _run_from_stdin()
