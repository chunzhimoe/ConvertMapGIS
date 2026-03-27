# coding: utf-8
"""arcgis_fgdb_helper_arcmap.py
================================
ArcPy helper for ArcMap 10.x (Desktop).  Designed to run under the ArcMap
bundled Python 2.7 interpreter (C:\\Python27\\ArcGIS10.x\\python.exe).

IMPORTANT: This file must remain Python 2/3 compatible because ArcMap ships
with Python 2.7.  Do NOT use:
  - f-strings  (use .format() or % instead)
  - type annotations
  - walrus operator (:=)
  - print as a function called with parentheses containing complex expressions
    that Python 2 would mis-parse  (simple print("x") is fine in 2 & 3)

Stdin payload schemas
---------------------

  create_gdb:
    {"action": "create_gdb", "gdb_path": "C:\\out\\output_arcmap.gdb"}

  ensure_feature_dataset:
    {"action": "ensure_feature_dataset",
     "gdb_path": "C:\\out\\output_arcmap.gdb",
     "ds_name":  "CRS_4326",
     "wkid":     4326}

  copy_feature_class:
    {"action": "copy_feature_class",
     "src_gdb": "C:\\out\\output_arcmap.gdb",
     "fc_name": "pt_abc12345",
     "dst_gdb": "C:\\out\\output_arcmap.gdb",
     "ds_name": "CRS_4326"}

  batch:
    {"action": "batch", "steps": [ <payload1>, ... ]}

Stdout: {"ok": true, ...} | {"ok": false, "error": "..."}
"""
from __future__ import print_function, unicode_literals

import os
import sys
import json
import traceback


# ---------------------------------------------------------------------------
# Action handlers (all Python 2/3 compatible)
# ---------------------------------------------------------------------------

def create_gdb(gdb_path):
    import arcpy
    parent = os.path.dirname(gdb_path)
    name   = os.path.basename(gdb_path)
    if not os.path.exists(gdb_path):
        arcpy.management.CreateFileGDB(parent, name)
    return gdb_path


def ensure_feature_dataset(gdb_path, ds_name, wkid):
    import arcpy
    ds_path = os.path.join(gdb_path, ds_name)
    if not arcpy.Exists(ds_path):
        sr = arcpy.SpatialReference(int(wkid))
        arcpy.management.CreateFeatureDataset(gdb_path, ds_name, sr)
    return ds_path


def copy_feature_class(src_gdb, fc_name, dst_gdb, ds_name=None):
    import arcpy
    src_fc = os.path.join(src_gdb, fc_name)
    if ds_name:
        out_ws = os.path.join(dst_gdb, ds_name)
    else:
        out_ws = dst_gdb
    dst_fc = os.path.join(out_ws, fc_name)
    if not arcpy.Exists(dst_fc):
        arcpy.conversion.FeatureClassToFeatureClass(src_fc, out_ws, fc_name)
    return dst_fc


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

def _handle(payload):
    action = payload.get("action", "")

    if action == "create_gdb":
        path = create_gdb(payload["gdb_path"])
        return {"ok": True, "gdb_path": path}

    elif action == "ensure_feature_dataset":
        path = ensure_feature_dataset(
            payload["gdb_path"], payload["ds_name"], payload["wkid"])
        return {"ok": True, "ds_path": path}

    elif action == "copy_feature_class":
        path = copy_feature_class(
            payload["src_gdb"], payload["fc_name"],
            payload["dst_gdb"], payload.get("ds_name"))
        return {"ok": True, "dst_fc": path}

    elif action == "batch":
        results = []
        for step in payload.get("steps", []):
            try:
                r = _handle(step)
            except Exception as exc:
                r = {"ok": False, "error": str(exc),
                     "traceback": traceback.format_exc()}
            results.append(r)
            if not r.get("ok"):
                return {"ok": False,
                        "error": r.get("error", "batch step failed"),
                        "results": results}
        return {"ok": True, "results": results}

    else:
        return {"ok": False, "error": "Unknown action: {}".format(action)}


def _main():
    raw = sys.stdin.read()
    payload = json.loads(raw)
    try:
        result = _handle(payload)
    except Exception as exc:
        result = {"ok": False, "error": str(exc),
                  "traceback": traceback.format_exc()}
    # Use sys.stdout.write to avoid Python 2 print-statement ambiguity
    sys.stdout.write(json.dumps(result))
    sys.stdout.write("\n")
    sys.stdout.flush()


if __name__ == "__main__":
    _main()
