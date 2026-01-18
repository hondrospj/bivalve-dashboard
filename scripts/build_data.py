import json
import math
import os
from datetime import datetime, timedelta, timezone

import requests

USGS_SITE = "01412150"
PRIMARY_PARAM = "72279"
FALLBACK_PARAM = "00065"

THRESH_MINOR = 4.19
TZ_UTC = timezone.utc

OUT_FORECAST = "data/nyhops_forecast.json"
OUT_INDEX = "data/high_tides_index.json"

# Stevens station requested
STEVENS_STATION = "U238"
STEVENS_PAGE = f"https://hudson.dl.stevens-tech.edu/sfas/d/index.shtml?station={STEVENS_STATION}"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; cupajoe-dashboard/1.0; +https://cupajoe.live/)"
}

def iso_now():
    return datetime.now(TZ_UTC).isoformat()

def usgs_iv(start_utc: datetime, end_utc: datetime, param: str):
    url = "https://waterservices.usgs.gov/nwis/iv/"
    params = {
        "format": "json",
        "sites": USGS_SITE,
        "parameterCd": param,
        "siteStatus": "all",
        "agencyCd": "USGS",
        "startDT": start_utc.isoformat().replace("+00:00", "Z"),
        "endDT": end_utc.isoformat().replace("+00:00", "Z"),
    }
    r = requests.get(url, params=params, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()

def extract_series(j):
    ts = (((j or {}).get("value") or {}).get("timeSeries") or [])
    if not ts:
        return []
    vals = (((ts[0].get("values") or [])[0]).get("value") or [])
    out = []
    for v in vals:
        try:
            ft = float(v.get("value"))
            if math.isfinite(ft):
                out.append({"t": v.get("dateTime"), "ft": ft})
        except Exception:
            pass
    return out

def fetch_usgs_series(start_utc: datetime, end_utc: datetime):
    j = usgs_iv(start_utc, end_utc, PRIMARY_PARAM)
    s = extract_series(j)
    if s:
        return s
    j = usgs_iv(start_utc, end_utc, FALLBACK_PARAM)
    return extract_series(j)

def load_json(path, default):
    if not os.path.exists(path):
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def save_json(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def detect_peaks(series, minor=THRESH_MINOR):
    """
    Converts a 15-min series into event peaks above minor.
    An event starts when ft>=minor and ends when ft<minor.
    We store the peak value and its timestamp.
    """
    if not series:
        return []

    # Ensure sorted by time string (ISO sorts correctly)
    series = sorted(series, key=lambda x: x["t"])

    in_evt = False
    peak_ft = -1e9
    peak_t = None
    peaks = []

    for p in series:
        ft = p["ft"]
        if not in_evt:
            if ft >= minor:
                in_evt = True
                peak_ft = ft
                peak_t = p["t"]
        else:
            if ft > peak_ft:
                peak_ft = ft
                peak_t = p["t"]
            if ft < minor:
                peaks.append({"t": peak_t, "ft": round(float(peak_ft), 4)})
                in_evt = False
                peak_ft = -1e9
                peak_t = None

    if in_evt and peak_t is not None:
        peaks.append({"t": peak_t, "ft": round(float(peak_ft), 4)})

    return peaks

def merge_unique_peaks(existing, new_peaks):
    """
    De-duplicate by timestamp (t). Keep max ft if duplicates.
    """
    m = {p["t"]: p for p in existing}
    for p in new_peaks:
        if p["t"] in m:
            m[p["t"]] = p if p["ft"] > m[p["t"]]["ft"] else m[p["t"]]
        else:
            m[p["t"]] = p
    out = list(m.values())
    out.sort(key=lambda x: x["t"], reverse=True)
    return out

# -------------------------
# NYHOPS/SFAS adapter (set once)
# -------------------------
def fetch_stevens_nyhops_forecast_points():
    """
    This function is intentionally isolated.
    SFAS/NYHOPS often requires a server-side fetch and parsing because the public UI
    is form/image-based. Your job is ONLY to make this return:
      [{"t": "<ISO8601>", "ft": <float>}, ...] for ~72 hours ahead.

    Once you identify the actual Stevens data endpoint behind station U238
    (via browser DevTools > Network), put the request here.
    """
    # Placeholder: return empty if not configured
    # Example shape (DON'T use this example values):
    # return [{"t":"2026-01-18T00:00:00Z","ft":4.12}, ...]
    return []

def main():
    # 1) Update high_tides_index.json using last 60 days of USGS data (fast & reliable)
    #    This builds an all-time index over time (keeps growing).
    end = datetime.now(TZ_UTC)
    start = end - timedelta(days=60)
    series = fetch_usgs_series(start, end)
    peaks = detect_peaks(series, minor=THRESH_MINOR)

    idx = load_json(OUT_INDEX, {"site": USGS_SITE, "updated_utc": None, "peaks": []})
    idx["peaks"] = merge_unique_peaks(idx.get("peaks", []), peaks)
    idx["updated_utc"] = iso_now()
    save_json(OUT_INDEX, idx)

    # 2) Update NYHOPS/SFAS forecast JSON
    fc = load_json(OUT_FORECAST, {"source": "NYHOPS", "updated_utc": None, "points": []})
    points = fetch_stevens_nyhops_forecast_points()
    fc["points"] = points
    fc["updated_utc"] = iso_now()
    fc["source"] = f"Stevens SFAS/NYHOPS station {STEVENS_STATION}"
    fc["station_page"] = STEVENS_PAGE
    save_json(OUT_FORECAST, fc)

if __name__ == "__main__":
    main()

