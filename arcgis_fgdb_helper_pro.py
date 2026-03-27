# coding: utf-8
"""arcgis_fgdb_helper_pro.py
==============================
ArcPy helper for ArcGIS Pro (Python 3).

Run via the ArcGIS Pro conda interpreter:
  C:\\Program Files\\ArcGIS\\Pro\\bin\\Python\\envs\\arcgispro-py3\\python.exe

Actions supported
-----------------
  create_gdb, ensure_feature_dataset, copy_feature_class, batch
  (same schema as arcgis_fgdb_helper_arcmap.py)

Python 3 only – may use f-strings, type hints, etc.
"""
from __future__ import annotations

import os
import sys
import json
import traceback
from typing import Optional


# ---------------------------------------------------------------------------
# Action handlers
# ---------------------------------------------------------------------------

def create_gdb(gdb_path: str) -> str:
    import arcpy
    parent = os.path.dirname(gdb_path)
    name   = os.path.basename(gdb_path)
    if not os.path.exists(gdb_path):
        arcpy.management.CreateFileGDB(parent, name)
    return gdb_path


def ensure_feature_dataset(gdb_path: str, ds_name: str, wkid: int) -> str:
    import arcpy
    ds_path = os.path.join(gdb_path, ds_name)
    if not arcpy.Exists(ds_path):
        sr = arcpy.SpatialReference(int(wkid))
        arcpy.management.CreateFeatureDataset(gdb_path, ds_name, sr)
    return ds_path


def copy_feature_class(
    src_gdb: str,
    fc_name: str,
    dst_gdb: str,
    ds_name: Optional[str] = None,
) -> str:
    import arcpy
    src_fc = os.path.join(src_gdb, fc_name)
    out_ws = os.path.join(dst_gdb, ds_name) if ds_name else dst_gdb
    dst_fc = os.path.join(out_ws, fc_name)
    if not arcpy.Exists(dst_fc):
        arcpy.conversion.FeatureClassToFeatureClass(src_fc, out_ws, fc_name)
    return dst_fc


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

def _handle(payload: dict) -> dict:
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
        return {"ok": False, "error": f"Unknown action: {action}"}


def _main():
    raw = sys.stdin.read()
    payload = json.loads(raw)
    try:
        result = _handle(payload)
    except Exception as exc:
        result = {"ok": False, "error": str(exc),
                  "traceback": traceback.format_exc()}
    sys.stdout.write(json.dumps(result) + "\n")
    sys.stdout.flush()


if __name__ == "__main__":
    _main()
