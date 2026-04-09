# UK Land Registry Pipeline

![dbt](https://img.shields.io/badge/dbt-FF694B?style=flat&logo=dbt&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=flat&logo=snowflake&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB-FFF000?style=flat&logo=duckdb&logoColor=black)
![License](https://img.shields.io/badge/Licence-OGL%20v3.0-blue)

An end-to-end big data pipeline processing **10,057,373 property transactions** from HM Land Registry (2016–2026), built with Python, DuckDB, dbt, and Snowflake.

---

## Architecture

```
HM Land Registry (Open Data CSVs)
            │
            ▼
  Python Ingestion Script
  (download → parse → load)
            │
            ▼
  Snowflake RAW Layer
  UK_LAND_REGISTRY.RAW.PRICE_PAID
            │
            ▼
  dbt Transformations
  ┌──────────────────────────────────────┐
  │  Staging      →  stg_price_paid      │
  │  Intermediate →  int_by_area         │
  │  Marts        →  3 analytical tables │
  └──────────────────────────────────────┘
```

---

## Dataset

| Property | Value |
|----------|-------|
| Source | HM Land Registry Price Paid Data |
| Coverage | 2016–2026 (11 years) |
| Volume | 10,057,373 rows |
| File size | ~1.7GB (11 CSV files) |
| Update frequency | Monthly |
| Licence | Open Government Licence v3.0 |

---

## Tech Stack

| Tool | Version | Purpose |
|------|---------|---------|
| Python | 3.13 | Data ingestion and loading |
| DuckDB | 1.1.0 | Local exploration — 10M rows, sub-second queries |
| Snowflake | — | Cloud data warehouse |
| dbt-snowflake | 1.11.3 | Data transformation and testing |
| pandas | 2.2.3 | CSV parsing and type casting |
| snowflake-connector-python | 4.4.0 | Snowflake bulk ingestion |

---

## dbt Models

### Staging

| Model | Materialisation | Description |
|-------|----------------|-------------|
| `stg_price_paid` | View | Cleans raw data, casts types, decodes categorical fields (property type, tenure, new/established) |

### Intermediate

| Model | Materialisation | Description |
|-------|----------------|-------------|
| `int_transactions_by_area` | View | Aggregates transactions by year, county, district, property type and tenure |

### Marts

| Model | Materialisation | Description |
|-------|----------------|-------------|
| `mart_price_by_year` | Table | National average and median price trends by year |
| `mart_property_trends` | Table | Breakdown by property type, tenure and new/established |
| `mart_london_prices` | Table | London borough-level price analysis (4,658 rows) |

---

## dbt Tests

| Test | Column | Status |
|------|--------|--------|
| not_null | transaction_id | ✅ Pass |
| unique | transaction_id | ✅ Pass |
| not_null | price_paid | ✅ Pass |
| not_null | transfer_date | ✅ Pass |
| not_null | property_type | ✅ Pass |
| accepted_values | property_type | ✅ Pass |
| accepted_values | duration | ✅ Pass |

**7/7 tests passing**

---

## Key Findings

| Metric | Value |
|--------|-------|
| London avg price (2016–2026) | £804,478 |
| Rest of England & Wales avg | £317,854 |
| London premium | 2.5x the national average |
| Peak transaction year | 2021 — 1,280,902 sales (COVID bounce) |
| Lowest transaction year | 2020 — 897,078 sales (COVID dip) |

---

## Row Counts by Year

| Year | Transactions |
|------|-------------|
| 2016 | 1,046,273 |
| 2017 | 1,067,484 |
| 2018 | 1,037,539 |
| 2019 | 1,011,985 |
| 2020 | 897,078 |
| 2021 | 1,280,902 |
| 2022 | 1,075,584 |
| 2023 | 859,145 |
| 2024 | 923,729 |
| 2025 | 802,761 |
| 2026 | 54,893 |
| **Total** | **10,057,373** |

---

## Project Structure

```
uk-land-registry-pipeline/
├── ingestion/
│   └── load_to_snowflake.py       # Loads all CSVs into Snowflake RAW
├── models/
│   ├── staging/
│   │   ├── stg_price_paid.sql
│   │   ├── stg_price_paid.yml     # Column tests and documentation
│   │   └── sources.yml            # Snowflake source declaration
│   ├── intermediate/
│   │   └── int_transactions_by_area.sql
│   └── marts/
│       ├── mart_london_prices.sql
│       ├── mart_price_by_year.sql
│       └── mart_property_trends.sql
├── .env.example
├── .gitignore
├── dbt_project.yml
└── README.md
```

---

## Setup

### Prerequisites

- Python 3.10+
- Snowflake account
- dbt-snowflake installed

### 1. Clone the repo

```bash
git clone https://github.com/Daudsaid/uk-land-registry-pipeline.git
cd uk-land-registry-pipeline
```

### 2. Install dependencies

```bash
pip install dbt-snowflake snowflake-connector-python pandas
```

### 3. Download the data

```bash
for year in 2016 2017 2018 2019 2020 2021 2022 2023 2024 2025 2026; do
  curl -o ~/pp-$year.csv \
    "https://price-paid-data.publicdata.landregistry.gov.uk/pp-$year.csv"
done
```

### 4. Set environment variable

```bash
export SNOWFLAKE_PASSWORD=your_password
```

### 5. Create Snowflake database

Run in your Snowflake worksheet:

```sql
CREATE DATABASE UK_LAND_REGISTRY;
CREATE SCHEMA UK_LAND_REGISTRY.RAW;
```

### 6. Load data into Snowflake

```bash
python3 ingestion/load_to_snowflake.py
```

### 7. Configure dbt

Update `~/.dbt/profiles.yml`:

```yaml
uk_land_registry:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: your_account
      user: your_user
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      role: ACCOUNTADMIN
      warehouse: COMPUTE_WH
      database: UK_LAND_REGISTRY
      schema: RAW
      threads: 1
```

### 8. Run dbt

```bash
dbt debug                             # test connection
dbt run                               # build all models
dbt test                              # run data quality tests
dbt docs generate && dbt docs serve   # view lineage graph
```

---

## Environment Variables

Copy `.env.example` and fill in your values:

```bash
cp .env.example .env
```

| Variable | Description |
|----------|-------------|
| `SNOWFLAKE_PASSWORD` | Your Snowflake account password |

---

## Attribution

Contains HM Land Registry data © Crown copyright and database right 2021.
This data is licensed under the [Open Government Licence v3.0](http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/).

---

## Author

**Daud Abdi** — Data Engineer

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0A66C2?style=flat&logo=linkedin&logoColor=white)](https://linkedin.com/in/daudabdi0506)
[![GitHub](https://img.shields.io/badge/GitHub-181717?style=flat&logo=github&logoColor=white)](https://github.com/Daudsaid)
[![Website](https://img.shields.io/badge/Website-000000?style=flat&logo=google-chrome&logoColor=white)](https://daudabdi.com)
