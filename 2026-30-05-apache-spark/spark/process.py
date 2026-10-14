import os
import socket
import time
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

DATA_PATH     = "/data"
LISTINGS_FILE = os.environ.get("LISTINGS_FILE", "listings.csv")
CALENDAR_FILE = os.environ.get("CALENDAR_FILE", "calendar.csv.gz")
SPARK_MASTER  = os.environ.get("SPARK_MASTER_URL", "local[*]")

def force(df, label):
    """Persist and count a DataFrame to force Spark to materialize it."""
    df.persist(StorageLevel.MEMORY_AND_DISK)
    n = df.count()
    return df, n

def fmt_time(t):
    return f"{t:.3f}s"

timings = {}
total_start = time.perf_counter()

# ── Init ─────────────────────────────────────────────────────────────────────
t = time.perf_counter()
_builder = (
    SparkSession.builder
    .appName("AirbnbBenchmark")
    .master(SPARK_MASTER)
    .config("spark.driver.memory", "1g")
    .config("spark.sql.shuffle.partitions", "8")
)
if not SPARK_MASTER.startswith("local"):
    _builder = (
        _builder
        .config("spark.driver.host", socket.gethostbyname(socket.gethostname()))
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.executor.memory", "512m")
    )
spark = _builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
timings["spark_init"] = time.perf_counter() - t

# ── Load ──────────────────────────────────────────────────────────────────────
t = time.perf_counter()
listings_raw = spark.read.option("multiLine", "true").csv(
    f"{DATA_PATH}/{LISTINGS_FILE}",
    header=True, inferSchema=False, quote='"', escape='"'
)
calendar_raw = spark.read.csv(
    f"{DATA_PATH}/{CALENDAR_FILE}",   # accepts file path or directory of part files
    header=True, inferSchema=False
)
listings_raw, n_listings = force(listings_raw, "listings_raw")
calendar_raw, n_calendar = force(calendar_raw, "calendar_raw")
timings["load_data"] = time.perf_counter() - t

# ── Clean listings ────────────────────────────────────────────────────────────
t = time.perf_counter()
listings = (
    listings_raw
    .select(
        F.col("id").cast("long").alias("listing_id"),
        F.col("neighbourhood_cleansed"),
        F.col("room_type"),
        F.regexp_replace(F.col("price"), r"[\$,]", "").cast("double").alias("price"),
        F.col("number_of_reviews").cast("int"),
        F.col("review_scores_rating").cast("double"),
        F.col("accommodates").cast("int"),
    )
    .filter(F.col("price").isNotNull() & (F.col("price") > 0))
)
listings, _ = force(listings, "listings")
timings["clean_listings"] = time.perf_counter() - t

# ── Clean calendar ────────────────────────────────────────────────────────────
t = time.perf_counter()
calendar = (
    calendar_raw
    .select(
        F.col("listing_id").cast("long"),
        F.col("date"),
        F.when(F.col("available") == "t", 1).otherwise(0).alias("is_available"),
        F.regexp_replace(F.col("price"), r"[\$,]", "").cast("double").alias("price"),
    )
)
calendar, _ = force(calendar, "calendar")
timings["clean_calendar"] = time.perf_counter() - t

# ── Aggregate calendar per listing ────────────────────────────────────────────
t = time.perf_counter()
cal_agg = (
    calendar
    .groupBy("listing_id")
    .agg(
        F.count("*").alias("total_days"),
        F.sum("is_available").alias("days_available"),
        F.avg(
            F.when(F.col("is_available") == 0, F.col("price"))
        ).alias("avg_booked_price"),
    )
    .withColumn(
        "occupancy_rate",
        (F.col("total_days") - F.col("days_available")) / F.col("total_days"),
    )
)
cal_agg, _ = force(cal_agg, "cal_agg")
timings["calendar_agg"] = time.perf_counter() - t

# ── Join listings + calendar ───────────────────────────────────────────────────
t = time.perf_counter()
joined = listings.join(cal_agg, on="listing_id", how="left")
joined, _ = force(joined, "joined")
timings["join"] = time.perf_counter() - t

# ── Aggregate: neighbourhood × room_type ──────────────────────────────────────
t = time.perf_counter()
neighbourhood_stats = (
    joined
    .groupBy("neighbourhood_cleansed", "room_type")
    .agg(
        F.count("*").alias("listing_count"),
        F.avg("price").alias("avg_price"),
        F.avg("occupancy_rate").alias("avg_occupancy"),
        F.avg("review_scores_rating").alias("avg_rating"),
        F.avg("avg_booked_price").alias("avg_booked_price"),
    )
    .orderBy(F.desc("listing_count"))
)
top_neighbourhoods = neighbourhood_stats.limit(20).collect()
timings["neighbourhood_agg"] = time.perf_counter() - t

# ── Aggregate: room_type summary ──────────────────────────────────────────────
t = time.perf_counter()
room_type_stats = (
    joined
    .groupBy("room_type")
    .agg(
        F.count("*").alias("count"),
        F.avg("price").alias("avg_price"),
        F.avg("occupancy_rate").alias("avg_occupancy"),
        F.avg("review_scores_rating").alias("avg_rating"),
    )
    .orderBy(F.desc("count"))
)
room_types = room_type_stats.collect()
timings["room_type_agg"] = time.perf_counter() - t

timings["total"] = time.perf_counter() - total_start

# ── Print results ─────────────────────────────────────────────────────────────
print()
print("=" * 70)
print("  APACHE SPARK — AIRBNB DUBLIN ANALYSIS")
print("=" * 70)
print(f"  Mode            : {SPARK_MASTER}")
print(f"  Dataset         : {LISTINGS_FILE}  |  {CALENDAR_FILE}")
print(f"  Listings loaded : {n_listings:,}")
print(f"  Calendar rows   : {n_calendar:,}")
print()

print(f"  Top 20 Neighbourhood × Room Type (by listing count)")
print(f"  {'Neighbourhood':<32} {'Room Type':<22} {'#':>5} {'Avg €':>8} {'Occup':>7} {'Rating':>7}")
print("  " + "-" * 84)
for r in top_neighbourhoods:
    occ  = f"{r['avg_occupancy']:.0%}"  if r["avg_occupancy"]  is not None else "N/A"
    rtg  = f"{r['avg_rating']:.2f}"     if r["avg_rating"]     is not None else "N/A"
    pri  = f"{r['avg_price']:.2f}"      if r["avg_price"]      is not None else "N/A"
    nbh  = (r["neighbourhood_cleansed"] or "")[:31]
    rtyp = (r["room_type"]              or "")[:21]
    print(f"  {nbh:<32} {rtyp:<22} {r['listing_count']:>5} {pri:>8} {occ:>7} {rtg:>7}")

print()
print(f"  Room Type Summary")
print(f"  {'Room Type':<28} {'Count':>7} {'Avg €':>8} {'Occup':>7} {'Rating':>7}")
print("  " + "-" * 60)
for r in room_types:
    occ = f"{r['avg_occupancy']:.0%}" if r["avg_occupancy"] is not None else "N/A"
    rtg = f"{r['avg_rating']:.2f}"    if r["avg_rating"]    is not None else "N/A"
    print(f"  {(r['room_type'] or ''):<28} {r['count']:>7} {r['avg_price']:>8.2f} {occ:>7} {rtg:>7}")

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

spark.stop()
