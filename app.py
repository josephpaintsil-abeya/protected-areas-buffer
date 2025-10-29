# app.py
# FastAPI service for buffer + intersection using shapely/pyproj.
# Endpoints:
#   GET  /                               -> health
#   POST /buffer-intersect-files         -> multipart/form-data; two GeoJSON files (coop, protected) + buffer_km
#   POST /buffer-intersect-batch         -> JSON body; { items: [ {json: {...}}, ... ], buffer_km?: number }

from fastapi import FastAPI, UploadFile, File, Form
from typing import Any, Dict, List
import json
import shapely
from shapely.geometry import shape, mapping, GeometryCollection
from shapely.ops import unary_union, transform
from shapely.validation import make_valid
from pyproj import CRS, Transformer

app = FastAPI(title="Geo Buffer/Intersect Service", version="1.0.1")

# ---- projections & transformers ----
crs_wgs = CRS.from_epsg(4326)
crs_m   = CRS.from_epsg(3857)
to_m    = Transformer.from_crs(crs_wgs, crs_m, always_xy=True).transform
to_geo  = Transformer.from_crs(crs_m, crs_wgs, always_xy=True).transform


# ---- helpers ----
def _to_2d(g):
    """Force 2D geometry (drop Z values)."""
    try:
        return shapely.force_2d(g)
    except Exception:
        return transform(lambda x, y, z=None: (x, y), g)

def _fix_valid(g):
    """Ensure geometry is valid."""
    try:
        g2 = make_valid(g)
    except Exception:
        g2 = g.buffer(0)
    if not g2.is_valid:
        try:
            g2 = g2.buffer(0)
        except Exception:
            return None
    return g2


def union_from_fc(fc: Dict[str, Any]):
    """Dissolve a FeatureCollection into one valid 2D geometry."""
    geoms = []
    for f in (fc or {}).get("features", []):
        geom = (f or {}).get("geometry")
        if not geom:
            continue
        try:
            g = shape(geom)
            if getattr(g, "has_z", False):
                g = _to_2d(g)
            g = _fix_valid(g)
            if g and not g.is_empty:
                geoms.append(g)
        except Exception:
            continue

    if not geoms:
        return GeometryCollection()

    try:
        return unary_union(geoms)
    except Exception:
        # fallback: merge stepwise
        u = geoms[0]
        for g in geoms[1:]:
            try:
                u = u.union(g)
            except Exception:
                pass
        return u


def pick_pair(j: Dict[str, Any]):
    if "coop" in j and "protected" in j:
        return (
            (j["coop"] or {}).get("name"),
            (j["coop"] or {}).get("geojson"),
            (j["protected"] or {}).get("name"),
            (j["protected"] or {}).get("geojson"),
        )

    a = {"kind": j.get("kind_1"), "name": j.get("name_1"), "geojson": j.get("geojson_1")}
    b = {"kind": j.get("kind_2"), "name": j.get("name_2"), "geojson": j.get("geojson_2")}

    if not a["kind"] and not b["kind"]:
        return a["name"], a["geojson"], b["name"], b["geojson"]

    coop = a if ((a["kind"] or "").lower().startswith("coop")) else b
    prot = b if coop is a else a
    return coop["name"], coop["geojson"], prot["name"], prot["geojson"]


def process_one(j: Dict[str, Any], buffer_m: int = 10_000) -> Dict[str, Any]:
    coop_name, coop_fc, prot_name, prot_fc = pick_pair(j)
    coop_name = (coop_name or "coop").replace(".geojson", "")
    prot_name = (prot_name or "protected").replace(".geojson", "")

    coop_union = union_from_fc(coop_fc or {"type": "FeatureCollection", "features": []})
    prot_union = union_from_fc(prot_fc or {"type": "FeatureCollection", "features": []})

    if coop_union.is_empty:
        coop_buffer = GeometryCollection()
    else:
        coop_m = transform(to_m, coop_union)
        coop_buffer_m = coop_m.buffer(buffer_m)
        coop_buffer = transform(to_geo, coop_buffer_m)

    if coop_buffer.is_empty or prot_union.is_empty:
        inter_features: List[Dict[str, Any]] = []
        inter_count = 0
        inter_area_km2 = 0.0
    else:
        inter = coop_buffer.intersection(prot_union)
        pieces = getattr(inter, "geoms", [inter])
        inter_features = []
        area_m2 = 0.0
        for g in pieces:
            if g.is_empty:
                continue
            area_m2 += transform(to_m, g).area
            inter_features.append({
                "type": "Feature",
                "properties": {
                    "coop": coop_name,
                    "protected": prot_name,
                    "buffer_km": round(buffer_m / 1000),
                },
                "geometry": mapping(g)
            })
        inter_count = len(inter_features)
        inter_area_km2 = round(area_m2 / 1_000_000.0, 6)

    overlap_fc = {"type": "FeatureCollection", "features": inter_features}
    buffer_fc = {
        "type": "FeatureCollection",
        "features": [] if coop_buffer.is_empty else [ {
            "type": "Feature",
            "properties": {"coop": coop_name, "buffer_km": round(buffer_m / 1000)},
            "geometry": mapping(coop_buffer)
        }]
    }

    return {
        "json": {
            "overlapFile": f"{coop_name}__x__{prot_name}__overlap_{round(buffer_m/1000)}km.geojson",
            "bufferFile":  f"{coop_name}__buffer_{round(buffer_m/1000)}km.geojson",
            "overlap_geojson": overlap_fc,
            "buffer_geojson":  buffer_fc,
            "coop": coop_name,
            "protected": prot_name,
            "buffer_km": round(buffer_m / 1000),
            "overlap_feature_count": inter_count,
            "overlap_area_km2": inter_area_km2
        }
    }


# ---- routes ----
@app.get("/")
def health():
    return {"ok": True, "service": "geo-buffer-intersect"}


@app.post("/buffer-intersect-files")
async def buffer_intersect_files(
    coop: UploadFile = File(...),
    protected: UploadFile = File(...),
    buffer_km: int = Form(10),
):
    coop_fc = json.loads((await coop.read()).decode("utf-8"))
    prot_fc = json.loads((await protected.read()).decode("utf-8"))

    j = {
        "coop": {"name": coop.filename or "coop.geojson", "geojson": coop_fc},
        "protected": {"name": protected.filename or "protected.geojson", "geojson": prot_fc},
    }
    return process_one(j, buffer_m=int(buffer_km) * 1000)


@app.post("/buffer-intersect-batch")
def buffer_intersect_batch(payload: Dict[str, Any]):
    items = payload.get("items")
    if items is None:
        items = [payload]
    buffer_km = int(payload.get("buffer_km", 10))
    buffer_m = buffer_km * 1000

    out: List[Dict[str, Any]] = []
    for it in items:
        try:
            j = it.get("json", it)
            out.append(process_one(j, buffer_m=buffer_m))
        except Exception as e:
            out.append({"json": {"error": str(e)}})
    return out
