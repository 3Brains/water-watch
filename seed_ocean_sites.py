#!/usr/bin/env python3
"""
seed_ocean_sites.py  (v0.1.0)

One-shot ETL to populate the ocean_sites reference table on the
water-watch backend. Idempotent: uses ON CONFLICT (source, source_id)
DO UPDATE so re-runs refresh existing rows in place without dupes.

Sources pulled:
  1. Hawaii DOH Clean Water Branch beach monitoring stations
     via PacIOOS ERDDAP (cwb_water_quality dataset). The dataset is
     observation-keyed (one row per sample), so we derive the catalog
     of active sites by selecting DISTINCT (latitude, longitude) for
     the last 2 years -- this filters out decommissioned stations.

  2. NOAA NDBC + PacIOOS-owned buoys in Hawaii waters
     via the NDBC activestations.xml feed. Filtered by a Hawaii
     bounding box (wide enough to catch far-field swell-arrival
     buoys 200+ NM offshore: 51000, 51001, 51101).

How to run:
  Render dashboard -> water-watch service -> Shell tab, then:
      python3 seed_ocean_sites.py

  DATABASE_URL is auto-injected by Render from the linked Postgres.

Expected output: roughly 80-120 sites total.
"""
import os
import sys
import json
import urllib.request
import urllib.error
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone

import psycopg2
import psycopg2.extras


# --- Config ---------------------------------------------------------------

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    print(
        "ERROR: DATABASE_URL not set. Run inside Render Shell on the "
        "water-watch service (Render auto-injects DATABASE_URL there).",
        file=sys.stderr,
    )
    sys.exit(1)

# Hawaii bounding box -- wide enough to catch the far-field swell buoys
# 200+ NM offshore (51000, 51001, 51101 are at ~24N, ~162W).
HI_LAT_MIN, HI_LAT_MAX = 16.0, 26.0
HI_LNG_MIN, HI_LNG_MAX = -163.0, -153.0

# DOH/CWB dataset on PacIOOS ERDDAP.
DOH_DATASET = "cwb_water_quality"
DOH_ERDDAP_BASE = (
    "https://pae-paha.pacioos.hawaii.edu/erddap/tabledap/" + DOH_DATASET
)

# NDBC active stations XML -- single endpoint, all stations worldwide.
NDBC_STATIONS_URL = "https://www.ndbc.noaa.gov/activestations.xml"

USER_AGENT = "WaterWatch-Seed-ETL/0.1.0 (3Brains; bspencer413@github)"
HTTP_TIMEOUT = 60  # ERDDAP can be slow on cold cache


# --- HTTP helper ----------------------------------------------------------

def http_get(url):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
        return resp.read()


# --- 1. DOH beach stations from PacIOOS ERDDAP ----------------------------

def fetch_doh_stations():
    """Pull distinct (lat, lng) pairs from the CWB water quality dataset.

    Strategy: the dataset is observation-keyed, not station-keyed, so
    we treat each unique (lat, lng) combination as one monitoring site.
    A 2-year time filter excludes long-decommissioned locations. If
    that returns nothing (e.g. ERDDAP transient), fall back to all-time
    distinct.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(days=730)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    primary_url = (
        DOH_ERDDAP_BASE + ".json"
        "?latitude,longitude"
        "&time%3E=" + cutoff           # %3E = >
        + "&distinct()"
    )
    fallback_url = (
        DOH_ERDDAP_BASE + ".json"
        "?latitude,longitude"
        "&distinct()"
    )

    print("Fetching DOH beach stations (PacIOOS ERDDAP)...")
    payload = None

    for label, url in (("primary (last 2yr)", primary_url),
                       ("fallback (all time)", fallback_url)):
        print("  " + label + ": " + url)
        try:
            raw = http_get(url)
            payload = json.loads(raw.decode("utf-8"))
            break
        except urllib.error.HTTPError as e:
            print("  HTTP " + str(e.code) + " on " + label
                  + (": " + str(e.reason) if e.reason else ""),
                  file=sys.stderr)
        except Exception as e:
            print("  ERROR on " + label + ": " + str(e), file=sys.stderr)

    if not payload:
        print("  Both DOH queries failed -- no beaches will be loaded.",
              file=sys.stderr)
        return []

    table = payload.get("table", {})
    cols = table.get("columnNames", [])
    rows = table.get("rows", [])
    if not rows:
        print("  WARNING: ERDDAP returned 0 rows.")
        return []

    try:
        idx_lat = cols.index("latitude")
        idx_lng = cols.index("longitude")
    except ValueError:
        print("  ERROR: latitude/longitude not in columns. Got: "
              + str(cols), file=sys.stderr)
        return []

    stations = []
    seen = set()
    for row in rows:
        try:
            lat = float(row[idx_lat])
            lng = float(row[idx_lng])
        except (ValueError, TypeError, IndexError):
            continue
        if lat == 0.0 and lng == 0.0:
            continue
        if not (HI_LAT_MIN <= lat <= HI_LAT_MAX
                and HI_LNG_MIN <= lng <= HI_LNG_MAX):
            continue
        # Synthetic stable id from coordinates -- no station_id in the dataset.
        source_id = "doh-" + ("%.5f" % lat) + "_" + ("%.5f" % lng)
        if source_id in seen:
            continue
        seen.add(source_id)
        stations.append({
            "source": "doh_cwb",
            "source_id": source_id,
            "name": "DOH beach " + ("%.4f" % lat) + ", " + ("%.4f" % lng),
            "lat": lat,
            "lng": lng,
            "site_type": "beach",
            "county": _hawaii_county_for(lat, lng),
            "monitoring_tier": None,  # not exposed in distinct() query
            "default_radius_mi": 1.5,
            "raw_metadata": {"dataset": DOH_DATASET},
        })

    print("  Got " + str(len(stations)) + " distinct DOH monitoring sites")
    return stations


def _hawaii_county_for(lat, lng):
    """Best-effort county tagging from coordinates. None when ambiguous.

    Hawaii has 4 counties, mostly island-bounded -- this is approximate
    but good enough for grouping in the UI; precise county tagging can
    come later from a proper polygon lookup.
    """
    # Big Island (Hawaii County)
    if lat < 20.30:
        return "Hawaii"
    # Maui County (Maui, Molokai, Lanai, Kahoolawe)
    if 20.30 <= lat < 21.20 and lng < -156.00:
        return "Maui"
    # Oahu (Honolulu County)
    if 21.20 <= lat <= 21.80 and -158.30 <= lng <= -157.60:
        return "Honolulu"
    # Kauai County (Kauai, Niihau)
    if lat >= 21.85 and lng <= -159.25:
        return "Kauai"
    return None


# --- 2. NDBC Hawaii buoys -------------------------------------------------

def fetch_ndbc_buoys():
    """Pull NDBC active stations XML, filter to Hawaii bbox.

    Tagging:
      - DART buoys (dart='y')      -> 'buoy_dart' (tsunami detection)
      - Far-field (lat>22.5 or lng<-160) -> 'buoy_far_field' (swell early-warning)
      - Otherwise                  -> 'buoy_coastal'

    Source attribution:
      - owner contains 'pacioos'   -> source='pacioos'
      - owner contains 'scripps' or 'cdip' -> source='cdip'
      - else                       -> source='ndbc'
    """
    print("Fetching NDBC active stations XML...")
    print("  URL: " + NDBC_STATIONS_URL)

    try:
        raw = http_get(NDBC_STATIONS_URL)
    except Exception as e:
        print("  ERROR: " + str(e), file=sys.stderr)
        return []

    try:
        root = ET.fromstring(raw)
    except ET.ParseError as e:
        print("  ERROR parsing XML: " + str(e), file=sys.stderr)
        return []

    buoys = []
    for station in root.iter("station"):
        sid = (station.get("id") or "").strip()
        if not sid:
            continue
        try:
            lat = float(station.get("lat") or "")
            lng = float(station.get("lon") or "")
        except (ValueError, TypeError):
            continue
        if not (HI_LAT_MIN <= lat <= HI_LAT_MAX
                and HI_LNG_MIN <= lng <= HI_LNG_MAX):
            continue

        owner = (station.get("owner") or "").strip()
        name = (station.get("name") or "").strip() or ("NDBC " + sid)
        stype = (station.get("type") or "").strip()
        is_dart = (station.get("dart") or "n").lower() == "y"
        is_met = (station.get("met") or "n").lower() == "y"
        is_water = (station.get("waterquality") or "n").lower() == "y"
        is_currents = (station.get("currents") or "n").lower() == "y"

        # Site-type classification
        if is_dart:
            site_type = "buoy_dart"
            default_radius_mi = 250.0
        elif lat > 22.5 or lng < -160.0:
            site_type = "buoy_far_field"
            default_radius_mi = 100.0
        else:
            site_type = "buoy_coastal"
            default_radius_mi = 25.0

        # Source attribution from owner string
        owner_lc = owner.lower()
        if "pacioos" in owner_lc:
            source = "pacioos"
        elif "scripps" in owner_lc or "cdip" in owner_lc:
            source = "cdip"
        else:
            source = "ndbc"

        buoys.append({
            "source": source,
            "source_id": sid,
            "name": name,
            "lat": lat,
            "lng": lng,
            "site_type": site_type,
            "county": _hawaii_county_for(lat, lng),
            "monitoring_tier": None,
            "default_radius_mi": default_radius_mi,
            "raw_metadata": {
                "owner": owner,
                "type": stype,
                "dart": is_dart,
                "met": is_met,
                "waterquality": is_water,
                "currents": is_currents,
            },
        })

    print("  Got " + str(len(buoys)) + " Hawaii-region buoys")
    if buoys:
        sources = {}
        types = {}
        for b in buoys:
            sources[b["source"]] = sources.get(b["source"], 0) + 1
            types[b["site_type"]] = types.get(b["site_type"], 0) + 1
        print("    by source: "
              + ", ".join(k + "=" + str(v) for k, v in sorted(sources.items())))
        print("    by type:   "
              + ", ".join(k + "=" + str(v) for k, v in sorted(types.items())))
    return buoys


# --- 3. Upsert ------------------------------------------------------------

UPSERT_SQL = """
INSERT INTO ocean_sites (
    source, source_id, name, lat, lng, site_type,
    county, monitoring_tier, default_radius_mi, active, raw_metadata
) VALUES (
    %(source)s, %(source_id)s, %(name)s, %(lat)s, %(lng)s, %(site_type)s,
    %(county)s, %(monitoring_tier)s, %(default_radius_mi)s, TRUE, %(raw_metadata)s
)
ON CONFLICT (source, source_id) DO UPDATE SET
    name              = EXCLUDED.name,
    lat               = EXCLUDED.lat,
    lng               = EXCLUDED.lng,
    site_type         = EXCLUDED.site_type,
    county            = EXCLUDED.county,
    monitoring_tier   = EXCLUDED.monitoring_tier,
    default_radius_mi = EXCLUDED.default_radius_mi,
    active            = TRUE,
    raw_metadata      = EXCLUDED.raw_metadata
"""

def upsert_sites(sites):
    if not sites:
        return 0
    # Wrap raw_metadata dicts for psycopg2's JSONB adapter.
    for s in sites:
        meta = s.get("raw_metadata")
        s["raw_metadata"] = (
            psycopg2.extras.Json(meta) if meta is not None else None
        )

    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn:
            with conn.cursor() as c:
                psycopg2.extras.execute_batch(
                    c, UPSERT_SQL, sites, page_size=50
                )
    finally:
        conn.close()
    return len(sites)


# --- 4. Main --------------------------------------------------------------

def main():
    print("=" * 60)
    print("ocean_sites seed ETL  (v0.1.0)")
    print("started: " + datetime.now(timezone.utc).isoformat())
    print("=" * 60)
    print()

    doh = fetch_doh_stations()
    print()
    ndbc = fetch_ndbc_buoys()
    print()

    all_sites = doh + ndbc
    if not all_sites:
        print("ERROR: No sites fetched from either source. Aborting "
              "without touching the database.", file=sys.stderr)
        sys.exit(2)

    print("Upserting " + str(len(all_sites)) + " sites into ocean_sites...")
    n = upsert_sites(all_sites)
    print("  Done. " + str(n) + " rows processed.")
    print()

    # Verify totals from DB
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn.cursor() as c:
            c.execute("""
                SELECT source, COUNT(*) FROM ocean_sites
                WHERE active = TRUE
                GROUP BY source ORDER BY source
            """)
            print("Final ocean_sites counts (active=true):")
            for source, count in c.fetchall():
                print("  " + source.ljust(12) + ": " + str(count))

            c.execute("SELECT COUNT(*) FROM ocean_sites WHERE active = TRUE")
            print("  " + "TOTAL".ljust(12) + ": "
                  + str(c.fetchone()[0]))

            c.execute("""
                SELECT site_type, COUNT(*) FROM ocean_sites
                WHERE active = TRUE
                GROUP BY site_type ORDER BY site_type
            """)
            print()
            print("By site_type:")
            for st, count in c.fetchall():
                print("  " + (st or "(null)").ljust(16) + ": " + str(count))
    finally:
        conn.close()

    print()
    print("Done.")


if __name__ == "__main__":
    main()
