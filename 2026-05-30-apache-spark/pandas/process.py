import os
import time
import pandas as pd

DATA_PATH     = "/data"
LISTINGS_FILE = os.environ.get("LISTINGS_FILE", "listings.csv")
CALENDAR_FILE = os.environ.get("CALENDAR_FILE", "calendar.csv.gz")

def fmt_time(t):
    return f"{t:.3f}s"

timings = {}
total_start = time.perf_counter()

# ── Load ──────────────────────────────────────────────────────────────────────
t = time.perf_counter()
listings_raw = pd.read_csv(f"{DATA_PATH}/{LISTINGS_FILE}", low_memory=False)
calendar_raw = pd.read_csv(f"{DATA_PATH}/{CALENDAR_FILE}", low_memory=False)
n_listings = len(listings_raw)
n_calendar = len(calendar_raw)
timings["load_data"] = time.perf_counter() - t

# ── Clean listings ────────────────────────────────────────────────────────────
t = time.perf_counter()
listings = listings_raw[[
    "id", "neighbourhood_cleansed", "room_type",
    "price", "number_of_reviews", "review_scores_rating", "accommodates",
]].copy()
listings.rename(columns={"id": "listing_id"}, inplace=True)
listings["listing_id"] = pd.to_numeric(listings["listing_id"], errors="coerce")
listings["price"] = (
    listings["price"]
    .astype(str)
    .str.replace(r"[\$,]", "", regex=True)
    .pipe(pd.to_numeric, errors="coerce")
)
listings = listings[listings["price"].notna() & (listings["price"] > 0)]
timings["clean_listings"] = time.perf_counter() - t

# ── Clean calendar ────────────────────────────────────────────────────────────
t = time.perf_counter()
calendar = calendar_raw[["listing_id", "date", "available", "price"]].copy()
calendar["listing_id"] = pd.to_numeric(calendar["listing_id"], errors="coerce")
calendar["is_available"] = (calendar["available"] == "t").astype(int)
calendar["price"] = (
    calendar["price"]
    .astype(str)
    .str.replace(r"[\$,]", "", regex=True)
    .pipe(pd.to_numeric, errors="coerce")
)
timings["clean_calendar"] = time.perf_counter() - t

# ── Aggregate calendar per listing ────────────────────────────────────────────
t = time.perf_counter()
booked_price = (
    calendar[calendar["is_available"] == 0]
    .groupby("listing_id")["price"]
    .mean()
    .rename("avg_booked_price")
)
cal_agg = calendar.groupby("listing_id").agg(
    total_days=("is_available", "count"),
    days_available=("is_available", "sum"),
).join(booked_price)
cal_agg["occupancy_rate"] = (
    (cal_agg["total_days"] - cal_agg["days_available"]) / cal_agg["total_days"]
)
cal_agg = cal_agg.reset_index()
timings["calendar_agg"] = time.perf_counter() - t

# ── Join listings + calendar ───────────────────────────────────────────────────
t = time.perf_counter()
joined = listings.merge(cal_agg, on="listing_id", how="left")
timings["join"] = time.perf_counter() - t

# ── Aggregate: neighbourhood × room_type ──────────────────────────────────────
t = time.perf_counter()
neighbourhood_stats = (
    joined
    .groupby(["neighbourhood_cleansed", "room_type"], dropna=False)
    .agg(
        listing_count=("listing_id", "count"),
        avg_price=("price", "mean"),
        avg_occupancy=("occupancy_rate", "mean"),
        avg_rating=("review_scores_rating", "mean"),
        avg_booked_price=("avg_booked_price", "mean"),
    )
    .sort_values("listing_count", ascending=False)
    .reset_index()
)
top_neighbourhoods = neighbourhood_stats.head(20)
timings["neighbourhood_agg"] = time.perf_counter() - t

# ── Aggregate: room_type summary ──────────────────────────────────────────────
t = time.perf_counter()
room_type_stats = (
    joined
    .groupby("room_type", dropna=False)
    .agg(
        count=("listing_id", "count"),
        avg_price=("price", "mean"),
        avg_occupancy=("occupancy_rate", "mean"),
        avg_rating=("review_scores_rating", "mean"),
    )
    .sort_values("count", ascending=False)
    .reset_index()
)
timings["room_type_agg"] = time.perf_counter() - t

timings["total"] = time.perf_counter() - total_start

# ── Print results ─────────────────────────────────────────────────────────────
print()
print("=" * 70)
print("  PANDAS — AIRBNB DUBLIN ANALYSIS")
print("=" * 70)
print(f"  Dataset         : {LISTINGS_FILE}  |  {CALENDAR_FILE}")
print(f"  Listings loaded : {n_listings:,}")
print(f"  Calendar rows   : {n_calendar:,}")
print()

print(f"  Top 20 Neighbourhood × Room Type (by listing count)")
print(f"  {'Neighbourhood':<32} {'Room Type':<22} {'#':>5} {'Avg €':>8} {'Occup':>7} {'Rating':>7}")
print("  " + "-" * 84)
for _, r in top_neighbourhoods.iterrows():
    occ  = f"{r['avg_occupancy']:.0%}"  if pd.notna(r["avg_occupancy"])  else "N/A"
    rtg  = f"{r['avg_rating']:.2f}"     if pd.notna(r["avg_rating"])     else "N/A"
    pri  = f"{r['avg_price']:.2f}"      if pd.notna(r["avg_price"])      else "N/A"
    nbh  = str(r["neighbourhood_cleansed"] or "")[:31]
    rtyp = str(r["room_type"]              or "")[:21]
    print(f"  {nbh:<32} {rtyp:<22} {int(r['listing_count']):>5} {pri:>8} {occ:>7} {rtg:>7}")

print()
print(f"  Room Type Summary")
print(f"  {'Room Type':<28} {'Count':>7} {'Avg €':>8} {'Occup':>7} {'Rating':>7}")
print("  " + "-" * 60)
for _, r in room_type_stats.iterrows():
    occ = f"{r['avg_occupancy']:.0%}" if pd.notna(r["avg_occupancy"]) else "N/A"
    rtg = f"{r['avg_rating']:.2f}"    if pd.notna(r["avg_rating"])    else "N/A"
    print(f"  {str(r['room_type']):<28} {int(r['count']):>7} {r['avg_price']:>8.2f} {occ:>7} {rtg:>7}")

print()
print("=" * 70)
print("  TIMING BREAKDOWN")
print("=" * 70)
for step, t in timings.items():
    bar = "█" * int(t / timings["total"] * 30)
    print(f"  {step:<25} {fmt_time(t):>8}  {bar}")
print("=" * 70)
print(f"  {'TOTAL':<25} {fmt_time(timings['total']):>8}")
print("=" * 70)
