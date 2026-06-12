#!/usr/bin/env python3
"""Downsample the 5 day GPX tracks into a compact JS dataset for the website.

Outputs route_data.js at repo root: window.ROUTE_DATA = [{name, dist_km, gain_m,
points: [[lat, lon], ...], elev: [[km, m], ...]}, ...]
"""
import json
import math
import re
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ROUTES = ROOT / "bikepacking-bc-epic" / "routes"
NS = {"gpx": "http://www.topografix.com/GPX/1/1"}

DAYS = [
    "day1_penticton_beaverdell.gpx",
    "day2_beaverdell_grand_forks.gpx",
    "day3_grand_forks_castlegar.gpx",
    "day4_castlegar_gray_creek.gpx",
    "day5_gray_creek_cranbrook.gpx",
]

MAP_POINTS = 400   # points kept per day for the map polyline
ELEV_POINTS = 120  # samples per day for the elevation profile


def haversine(a, b):
    R = 6371.0088
    la1, lo1, la2, lo2 = map(math.radians, (a[0], a[1], b[0], b[1]))
    h = math.sin((la2 - la1) / 2) ** 2 + math.cos(la1) * math.cos(la2) * math.sin((lo2 - lo1) / 2) ** 2
    return 2 * R * math.asin(math.sqrt(h))


def load(path):
    tree = ET.parse(path)
    pts = []
    for tp in tree.iterfind(".//gpx:trkpt", NS):
        ele = tp.find("gpx:ele", NS)
        pts.append((float(tp.get("lat")), float(tp.get("lon")), float(ele.text) if ele is not None else 0.0))
    return pts


def resample(pts, n):
    """Pick n points evenly spaced by cumulative distance."""
    cum = [0.0]
    for i in range(1, len(pts)):
        cum.append(cum[-1] + haversine(pts[i - 1], pts[i]))
    total = cum[-1]
    out, j = [], 0
    for k in range(n):
        target = total * k / (n - 1)
        while j < len(cum) - 1 and cum[j + 1] < target:
            j += 1
        out.append((pts[j], cum[j]))
    return out, total


def smooth_gain(pts, window=9):
    """Elevation gain over a lightly smoothed series (raw GPX is noisy)."""
    el = [p[2] for p in pts]
    sm = [sum(el[max(0, i - window // 2): i + window // 2 + 1]) / len(el[max(0, i - window // 2): i + window // 2 + 1]) for i in range(len(el))]
    return sum(max(0.0, sm[i] - sm[i - 1]) for i in range(1, len(sm)))


def main():
    days = []
    for f in DAYS:
        pts = load(ROUTES / f)
        name = re.sub(r"^day\d+_|\.gpx$", "", f)
        mp, total = resample(pts, MAP_POINTS)
        ep, _ = resample(pts, ELEV_POINTS)
        days.append({
            "file": f,
            "dist_km": round(total, 1),
            "gain_m": int(round(smooth_gain(pts))),
            "points": [[round(p[0], 5), round(p[1], 5)] for p, _ in mp],
            "elev": [[round(d, 2), int(round(p[2]))] for p, d in ep],
        })
        print(f"{f}: {total:.1f} km, +{days[-1]['gain_m']} m, {len(pts)} -> {MAP_POINTS} pts")

    js = "window.ROUTE_DATA=" + json.dumps(days, separators=(",", ":")) + ";"
    out = ROOT / "route_data.js"
    out.write_text(js)
    print(f"wrote {out} ({out.stat().st_size / 1024:.0f} KB)")


if __name__ == "__main__":
    main()
