#!/usr/bin/env python3
import json, os, math, time
from datetime import datetime, timedelta, timezone
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

USGS_SITE = "01412150"
PRIMARY_PARAM = "72279"
FALLBACK_PARAM = "00065"

THRESH_MINOR = 4.19

OUT_DIR = "data"
OUT_INDEX = os.path.join(OUT_DIR, "high_tides_index.json")
OUT_NYHOPS = os.path.join(OUT_DIR, "nyhops_forecast.json")

NYHOPS_STATION = "U238"

def iso_now():
  return datetime.now(timezone.utc).isoformat()

def http_get(url, timeout=30, headers=None):
  headers = headers or {}
  req = Request(url, headers={"User-Agent":"cupajoe-bivalve-dashboard/1.0", **headers})
  with urlopen(req, timeout=timeout) as r:
    return r.read()

def usgs_iv_url(site, param, start_iso, end_iso):
  return (
    "https://waterservices.usgs.gov/nwis/iv/?format=json"
    f"&sites={site}&parameterCd={param}&siteStatus=all&agencyCd=USGS"
    f"&startDT={start_iso}&endDT={end_iso}"
  )

def parse_usgs_series(js):
  ts = (js.get("value", {}).get("timeSeries") or [])
  if not ts: return []
  vals = (ts[0].get("values") or [])
  if not vals: return []
  arr = (vals[0].get("value") or [])
  out = []
  for v in arr:
    try:
      ft = float(v["value"])
      t = v["dateTime"]
      if math.isfinite(ft):
        out.append((t, ft))
    except Exception:
      pass
  return out

def fetch_usgs_chunk(start_dt, end_dt):
  start_iso = start_dt.isoformat().replace("+00:00","Z")
  end_iso = end_dt.isoformat().replace("+00:00","Z")

  for param in (PRIMARY_PARAM, FALLBACK_PARAM):
    url = usgs_iv_url(USGS_SITE, param, start_iso, end_iso)
    try:
      raw = http_get(url)
      js = json.loads(raw.decode("utf-8"))
      pts = parse_usgs_series(js)
      if pts:
        return pts
    except Exception:
      continue
  return []

def event_peaks(points, minor=THRESH_MINOR):
  """
  points: list of (iso, ft) in chronological order
  Event definition: continuous period >= minor. Peak is max ft within the event.
  """
  peaks = []
  in_evt = False
  peak_t = None
  peak_ft = None

  for t, ft in points:
    if not in_evt:
      if ft >= minor:
        in_evt = True
        peak_t, peak_ft = t, ft
    else:
      if ft > peak_ft:
        peak_t, peak_ft = t, ft
      if ft < minor:
        peaks.append((peak_t, peak_ft))
        in_evt = False
        peak_t, peak_ft = None, None

  if in_evt and peak_t is not None:
    peaks.append((peak_t, peak_ft))
  return peaks

def load_existing_index():
  if not os.path.exists(OUT_INDEX):
    return {"generated_at": None, "peaks": []}
  with open(OUT_INDEX, "r", encoding="utf-8") as f:
    return json.load(f)

def dedupe_peaks(peaks):
  # dedupe by timestamp; keep max if collision
  m = {}
  for t, ft in peaks:
    if (t not in m) or (ft > m[t]):
      m[t] = ft
  out = [{"t": t, "ft": m[t]} for t in sorted(m.keys())]
  return out

def build_high_tide_index():
  os.makedirs(OUT_DIR, exist_ok=True)
  existing = load_existing_index()
  existing_peaks = [(p["t"], float(p["ft"])) for p in (existing.get("peaks") or []) if "t" in p and "ft" in p]

  # Decide start date: if we already have peaks, go back 7 days before latest peak to safely rebuild overlaps
  if existing_peaks:
    latest_t = max(existing_peaks, key=lambda x: x[0])[0]
    try:
      latest_dt = datetime.fromisoformat(latest_t.replace("Z","+00:00"))
    except Exception:
      latest_dt = datetime(2000,1,1,tzinfo=timezone.utc)
    start_dt = latest_dt - timedelta(days=7)
  else:
    start_dt = datetime(2000,1,1,tzinfo=timezone.utc)

  end_dt = datetime.now(timezone.utc)

  # Pull USGS data in 30-day chunks (keeps requests manageable)
  all_points = []
  cur = start_dt
  chunk = timedelta(days=30)

  while cur < end_dt:
    nxt = min(end_dt, cur + chunk)
    pts = fetch_usgs_chunk(cur, nxt)
    all_points.extend(pts)
    cur = nxt
    time.sleep(0.15)

  # Sort chronologically
  all_points.sort(key=lambda x: x[0])

  new_peaks = event_peaks(all_points, minor=THRESH_MINOR)
  merged = existing_peaks + new_peaks
  merged_dedup = dedupe_peaks(merged)

  out = {
    "generated_at": iso_now(),
    "site": USGS_SITE,
    "minor_threshold_ft": THRESH_MINOR,
    "peaks": merged_dedup
  }

  with open(OUT_INDEX, "w", encoding="utf-8") as f:
    json.dump(out, f, indent=2)
  print(f"Wrote {OUT_INDEX} with {len(merged_dedup)} peaks")

def try_parse_csv_forecast(raw_bytes):
  """
  Very generic CSV parser:
  expects at least 2 columns: time, value
  """
  txt = raw_bytes.decode("utf-8", errors="ignore").strip()
  lines = [ln.strip() for ln in txt.splitlines() if ln.strip()]
  if len(lines) < 2:
    return []

  # try to detect header
  start_i = 0
  if any(h in lines[0].lower() for h in ["time","date","water","stage","elev","value"]):
    start_i = 1

  pts = []
  for ln in lines[start_i:]:
    parts = [p.strip() for p in ln.split(",")]
    if len(parts) < 2:
      continue
    t = parts[0].replace(" ", "T")
    try:
      ft = float(parts[1])
    except Exception:
      continue

    # try to make ISO-ish
    if "T" in t and "Z" not in t and "+" not in t:
      # assume local/naive; keep as string and let frontend format as best it can
      pass
    pts.append({"t": t, "ft": ft})
  return pts

def fetch_nyhops_forecast():
  """
  Stevens SFAS/NYHOPS pages can be dynamic.
  We try a few common patterns; if none work, return [].
  """
  candidates = [
    # (These are guesses; if you confirm the real endpoint, we’ll replace this cleanly.)
    f"https://hudson.dl.stevens-tech.edu/sfas/d/data/{NYHOPS_STATION}.csv",
    f"https://hudson.dl.stevens-tech.edu/sfas/d/{NYHOPS_STATION}.csv",
    f"https://hudson.dl.stevens-tech.edu/sfas/d/download.php?station={NYHOPS_STATION}",
    f"https://hudson.dl.stevens-tech.edu/sfas/d/index.shtml?station={NYHOPS_STATION}&format=csv",
  ]

  for url in candidates:
    try:
      raw = http_get(url, timeout=25)
      pts = try_parse_csv_forecast(raw)
      if pts:
        return {"source": url, "points": pts}
    except (HTTPError, URLError):
      continue
    except Exception:
      continue

  return {"source": None, "points": []}

def write_nyhops():
  os.makedirs(OUT_DIR, exist_ok=True)
  fc = fetch_nyhops_forecast()
  out = {
    "generated_at": iso_now(),
    "station": NYHOPS_STATION,
    "source": fc["source"],
    "points": fc["points"][:2000]  # keep file reasonable
  }
  with open(OUT_NYHOPS, "w", encoding="utf-8") as f:
    json.dump(out, f, indent=2)
  print(f"Wrote {OUT_NYHOPS} with {len(out['points'])} points (source={out['source']})")

if __name__ == "__main__":
  build_high_tide_index()
  write_nyhops()
