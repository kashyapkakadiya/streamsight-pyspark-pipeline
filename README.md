# StreamSight PySpark Pipeline

A PySpark implementation of the StreamSight ETL pipeline — processes the
"Most Streamed Spotify Songs 2024" dataset using Apache Spark 3.5.1,
fully containerized with Docker.

## Stack

| Layer | Technology |
|---|---|
| Language | Python 3.8 |
| Processing Engine | Apache Spark 3.5.1 (PySpark) |
| Containerization | Docker + Docker Compose |

## Architecture

CSV File (4,600 records)

↓

[Extract]   spark.read.csv → raw DataFrame

↓

[Transform] normalize columns → deduplicate → cast numerics

→ parse dates → fill nulls → drop invalid rows

↓

[Analytics] top songs · top artists · yearly trends + YoY growth

↓

[Output]    CSV files → /output/

## Project Structure

spotify-pyspark/
├── docker-compose.yml       # Spark container
├── src/
│   └── pipeline.py          # full PySpark pipeline
├── data/                    # CSV dataset (git-ignored)
├── output/                  # results written here (git-ignored)
└── .gitignore

## Pipeline Stages

### Extract
Reads raw CSV (4,600 records, 29 columns) into a Spark DataFrame
using `spark.read.csv` with header inference and UTF-8 encoding.

### Transform
Applies 6 transformation steps:
- Normalizes all column names to snake_case
- Drops 2 fully duplicate rows
- Strips commas and casts 14 columns to `LongType`
- Parses release dates using legacy time parser policy
- Extracts release year as a separate integer column
- Fills null numeric values with 0
- Drops 5 rows missing track name or artist name

Final output: **4,593 clean records**

### Analytics
Three analytics outputs computed using PySpark DataFrame API:
- Top 10 most streamed songs
- Top 10 artists by total streams with track count and avg streams
- Yearly trends (30 years) with YoY stream growth % using window functions

### Output
Results written as CSV to `/opt/spark/output/` using `coalesce(1)`
for single-file output per analytics table.

## Key PySpark Concepts Used

- `SparkSession` with local mode and custom configs
- Lazy evaluation — transformations build a plan, actions trigger execution
- `Window.orderBy` + `F.lag()` for year-over-year calculations
- `F.when().otherwise()` for null-safe conditional logic
- `coalesce(1)` for controlled output partitioning

## How to Run

### Prerequisites
- Docker Desktop installed and running

### Steps

```bash
# 1. Clone the repo
git clone https://github.com/kashyapkakadiya/spotify-pyspark-pipeline.git
cd spotify-pyspark-pipeline

# 2. Add your dataset
# Download from Kaggle and place at:
# data/Most Streamed Spotify Songs 2024.csv

# 3. Run the pipeline
docker-compose up
```

Results are written to the `output/` folder as CSV files.

### Stop
```bash
docker-compose down
```

## Sample Output

[ANALYTICS] Top 10 Most Streamed Songs:
+------------------------+-------------+---------------+
|track                   |artist       |spotify_streams|
+------------------------+-------------+---------------+
|Blinding Lights         |The Weeknd   |4281468720     |
|Shape of You            |Ed Sheeran   |3909458734     |
|Someone You Loved       |Lewis Capaldi|3427498835     |
...
[ANALYTICS] Yearly Trends (sample):
+------------+------------+--------------+-------------+--------------+
|release_year|total_tracks|unique_artists|total_streams|yoy_growth_pct|
+------------+------------+--------------+-------------+--------------+
|2023        |1158        |750           |225245837239 |7.56          |
|2022        |693         |491           |209409259280 |10.44         |
|2011        |51          |37            |54241058712  |107.63        |
...

## Dataset

Source: [Most Streamed Spotify Songs 2024](https://www.kaggle.com/datasets/nelgiriyewithana/most-streamed-spotify-songs-2024)
Records: 4,600 | Size: 1.05 MB
