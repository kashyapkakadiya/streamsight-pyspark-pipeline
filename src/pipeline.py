import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DateType, IntegerType
)
from pyspark.sql.window import Window

# ──────────────────────────────────────────
# 1. CREATE SPARK SESSION
# ──────────────────────────────────────────
spark = SparkSession.builder \
    .appName("StreamSight PySpark Pipeline") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("\n" + "="*50)
print("   STREAMSIGHT PYSPARK PIPELINE")
print("="*50)

# ──────────────────────────────────────────
# 2. EXTRACT
# ──────────────────────────────────────────
print("\n[EXTRACT] Reading CSV...")

filepath = "/opt/spark/data/Most Streamed Spotify Songs 2024.csv"

df_raw = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("encoding", "UTF-8") \
    .csv(filepath)

print(f"[EXTRACT] Rows: {df_raw.count()} | Columns: {len(df_raw.columns)}")
print(f"[EXTRACT] Columns: {df_raw.columns}")

# ──────────────────────────────────────────
# 3. TRANSFORM
# ──────────────────────────────────────────
print("\n[TRANSFORM] Starting transformations...")

# --- 3a. Normalize column names to snake_case ---
def normalize_col(name):
    return (name.strip()
                .lower()
                .replace(' ', '_')
                .replace('(', '')
                .replace(')', '')
                .replace('-', '_'))

renamed_cols = [normalize_col(c) for c in df_raw.columns]
df = df_raw.toDF(*renamed_cols)
print(f"[TRANSFORM] Columns normalized")

# --- 3b. Drop duplicates ---
before = df.count()
df = df.dropDuplicates()
print(f"[TRANSFORM] Dropped {before - df.count()} duplicate rows")

# --- 3c. Cast numeric columns ---
numeric_cols = [
    'spotify_streams', 'spotify_playlist_count', 'spotify_playlist_reach',
    'youtube_views', 'youtube_likes', 'tiktok_posts', 'tiktok_likes',
    'tiktok_views', 'pandora_streams', 'soundcloud_streams',
    'apple_music_playlist_count', 'deezer_playlist_count',
    'amazon_playlist_count', 'shazam_counts'
]

for col in numeric_cols:
    if col in df.columns:
        df = df.withColumn(
            col,
            F.regexp_replace(F.col(col).cast("string"), ",", "")
             .cast("long")
        )

print(f"[TRANSFORM] Numeric columns cast to long")

# --- 3d. Parse release date ---
if 'release_date' in df.columns:
    df = df.withColumn(
        'release_date',
        F.to_date(F.col('release_date'), 'M/d/yyyy')
    )
    df = df.withColumn(
        'release_year',
        F.year(F.col('release_date')).cast(IntegerType())
    )
    print(f"[TRANSFORM] release_date parsed, release_year extracted")

# --- 3e. Fill nulls in numeric columns ---
fill_map = {col: 0 for col in numeric_cols if col in df.columns}
df = df.fillna(fill_map)
print(f"[TRANSFORM] Nulls filled with 0")

# --- 3f. Drop rows missing critical fields ---
before = df.count()
df = df.dropna(subset=['track', 'artist'])
print(f"[TRANSFORM] Dropped {before - df.count()} rows missing track/artist")

print(f"[TRANSFORM] Done. Final rows: {df.count()}")

# ──────────────────────────────────────────
# 4. ANALYTICS
# ──────────────────────────────────────────
print("\n[ANALYTICS] Running analytics...")

# --- Top 10 most streamed songs ---
top_songs = df.select('track', 'artist', 'spotify_streams') \
    .orderBy(F.col('spotify_streams').desc()) \
    .limit(10)

print("\n[ANALYTICS] Top 10 Most Streamed Songs:")
top_songs.show(truncate=False)

# --- Top 10 artists by total streams ---
top_artists = df.groupBy('artist') \
    .agg(
        F.sum('spotify_streams').alias('total_streams'),
        F.count('*').alias('total_tracks'),
        F.round(F.avg('spotify_streams')).alias('avg_streams_per_track')
    ) \
    .orderBy(F.col('total_streams').desc()) \
    .limit(10)

print("\n[ANALYTICS] Top 10 Artists by Total Streams:")
top_artists.show(truncate=False)

# --- Yearly trends with YoY growth ---
window_spec = Window.orderBy('release_year')

yearly = df.groupBy('release_year') \
    .agg(
        F.count('*').alias('total_tracks'),
        F.countDistinct('artist').alias('unique_artists'),
        F.sum('spotify_streams').alias('total_streams'),
        F.round(F.avg('spotify_streams')).alias('avg_streams')
    ) \
    .filter(F.col('release_year').isNotNull()) \
    .withColumn(
        'prev_year_streams',
        F.lag('total_streams').over(window_spec)
    ) \
    .withColumn(
        'yoy_growth_pct',
        F.round(
            F.when(
                F.col('prev_year_streams').isNotNull() & (F.col('prev_year_streams') != 0),
                100.0 * (F.col('total_streams') - F.col('prev_year_streams'))
                / F.col('prev_year_streams')
            ).otherwise(None)
        , 2)
    ) \
    .orderBy('release_year', ascending=False)

print("\n[ANALYTICS] Yearly Trends with YoY Growth:")
yearly.show(truncate=False)

# ──────────────────────────────────────────
# 5. SAVE OUTPUT
# ──────────────────────────────────────────
print("\n[OUTPUT] Writing results...")

top_songs.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("/opt/spark/output/top_songs")

top_artists.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("/opt/spark/output/top_artists")

yearly.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("/opt/spark/output/yearly_trends")

print("[OUTPUT] Results written to /opt/spark/output/")

# ──────────────────────────────────────────
# 6. SUMMARY
# ──────────────────────────────────────────
print("\n" + "="*50)
print("   PIPELINE COMPLETE")
print(f"   Total records processed : {df.count()}")
print(f"   Output location         : /opt/spark/output/")
print("="*50 + "\n")

spark.stop()