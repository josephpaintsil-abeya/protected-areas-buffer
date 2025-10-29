# app.py
# FastAPI service for 10 km buffer + intersection using shapely/pyproj.
# Input: batch payload from n8n (see /buffer-intersect-batch docstring).
# Output: list of {"json": {...}} records ready for n8n.

from fastapi import FastAPI
from typing import Any, Dict, List

from shapely.geometry import shape, mapping, GeometryCollection
from shapely.ops import unary_union, transform
from pyproj import CRS, Transformer

app = FastAPI(title="Geo Buffer/Intersect Batch", version="1.0.0")

# ---- constants & transformers ----
BUFFER_M = 10_000  # 10 km

crs_wgs = CRS.from_epsg(4326)
crs_m   = CRS.from_epsg(3857)
to_m    = Transformer.from_crs(crs_wgs, crs_m, always_xy=True).transform
to_geo  = Transformer.from_crs(crs_m, crs_wgs, always_xy=True).transform


# ---- helpers (your original logic, adapted) ----
def union_from_fc(fc: Dict[str, Any]) -> GeometryCollection:
    """Dissolve a FeatureCollection into one geometry (or empty)."""
    geoms: List = []
    for f in (fc or {}).get("features", []):
        try:
            g = shape(f["geometry"])
            if not g.is_empty:
                geoms.append(g)
        except Exception:
            # ignore malformed features
            pass
    if not geoms:
        return GeometryCollection()
    return unary_union(geoms)


def pick_pair(j: Dict[str, Any]):
    """
    Accept either:
      { coop: {name, geojson}, protected: {name, geojson} }
    or Merge(Combine) outputs that have numbered fields:
      { kind_1, name_1, geojson_1, kind_2, name_2, geojson_2 }
    If neither has 'kind', assume _1 is coop and _2 is protected.
    """
    if "coop" in j and "protected" in j:
        return (
            j["coop"].get("name"),
            j["coop"].get("geojson"),
            j["protected"].get("name"),
            j["protected"].get("geojson"),
        )

    a = {"kind": j.get("kind_1"), "name": j.get("name_1"), "geojson": j.get("geojson_1")}
    b = {"kind": j.get("kind_2"), "name": j.get("name_2"), "geojson": j.get("geojson_2")}

    if not a["kind"] and not b["kind"]:
        # default: treat first as coop, second as protected
        return a["name"], a["geojson"], b["name"], b["geojson"]

    coop = a if ((a["kind"] or "").lower().startswith("coop")) else b
    prot = b if coop is a else a
    return coop["name"], coop["geojson"], prot["name"], prot["geojson"]


def process_one(j: Dict[str, Any]) -> Dict[str, Any]:
    coop_name, coop_fc, prot_name, prot_fc = pick_pair(j)
    coop_name = (coop_name or "coop").replace(".geojson", "")
    prot_name = (prot_name or "protected").replace(".geojson", "")

    coop_union = union_from_fc(coop_fc or {"type": "FeatureCollection", "features": []})
    prot_union = union_from_fc(prot_fc or {"type": "FeatureCollection", "features": []})

    # 10 km buffer around coop in projected meters
    if coop_union.is_empty:
        coop_buffer = GeometryCollection()
    else:
        coop_m = transform(to_m, coop_union)
        coop_buffer_m = coop_m.buffer(BUFFER_M)
        coop_buffer = transform(to_geo, coop_buffer_m)

    # intersection + area
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
                    "buffer_km": 10
                },
                "geometry": mapping(g)
            })
        inter_count = len(inter_features)
        inter_area_km2 = round(area_m2 / 1_000_000.0, 6)

    overlap_fc = {"type": "FeatureCollection", "features": inter_features}
    buffer_fc = {
        "type": "FeatureCollection",
        "features": [] if coop_buffer.is_empty else [{
            "type": "Feature",
            "properties": {"coop": coop_name, "buffer_km": 10},
            "geometry": mapping(coop_buffer)
        }]
    }

    # n8n-friendly one-record payload
    return {
        "json": {
            "overlapFile": f"{coop_name}__x__{prot_name}__overlap_10km.geojson",
            "bufferFile":  f"{coop_name}__buffer_10km.geojson",
            "overlap_geojson": overlap_fc,
            "buffer_geojson":  buffer_fc,
            "coop": coop_name,
            "protected": prot_name,
            "buffer_km": 10,
            "overlap_feature_count": inter_count,
            "overlap_area_km2": inter_area_km2
        }
    }


# ---- routes ----
@app.get("/")
def health():
    return {"ok": True, "service": "geo-buffer-intersect-batch"}


@app.post("/buffer-intersect-batch")
def buffer_intersect_batch(payload: Dict[str, Any]):
    """
    Expected (from n8n HTTP Request node):
      {
        "items": [
          { "json": { ...pair1... } },
          { "json": { ...pair2... } }
        ]
      }
    Also accepts a single object (will be wrapped).
    """
    items = payload.get("items")
    if items is None:
        items = [payload]  # accept a single object too

    out: List[Dict[str, Any]] = []
    for it in items:
        try:
            j = it.get("json", it)  # prefer .json if present
            out.append(process_one(j))
        except Exception as e:
            out.append({"json": {"error": str(e)}})

    return out
