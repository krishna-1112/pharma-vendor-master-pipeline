# Pharmaceutical Procurement Intelligence: Vendor Master Consolidation

## Project Overview
This repository contains the ETL data engineering pipeline for consolidating dirty, multi-source enterprise vendor data. The solution standardizes heterogeneous datasets (ERP vendor master records and manual procurement logs), resolves string formatting anomalies, imputes missing metrics, segregates invalid records into a quarantine layer, and loads cleaned data into a SQLite analytical warehouse for spend auditing.

## Repository Structure
```text
pharma-vendor-master-pipeline/
├── config/
│   └── config.example.ini             # Template settings (copy to config.ini)
├── data/
│   ├── raw/                           # Raw input datasets (CSV)
│   └── output/                        # Processed outputs (ignored by Git)
├── sql/
│   ├── ddl_schema.sql                 # Table definitions
│   └── verification_queries.sql       # Audit verification queries
├── src/
│   └── consolidate_vendor_master.py   # Primary Python ETL engine
├── .gitignore                         # Git exclusion rules
├── requirements.txt                   # Dependency specifications
└── README.md                          # Pipeline documentation
```
A `logs/` folder is created at runtime (one log file per run) and is ignored by Git.

## Setup & Installation

1. Clone the repository locally:
   ```bash
   git clone https://github.com/<your-username>/pharma-vendor-master-pipeline.git
   cd pharma-vendor-master-pipeline
   ```

2. Initialize a virtual environment and install dependencies:
   ```bash
   python -m venv .venv
   source .venv/bin/activate        # On Windows: .venv\Scripts\activate
   pip install -r requirements.txt
   ```

3. Set up the environment configuration:
   ```bash
   cp config/config.example.ini config/config.ini
   ```
   `config/config.ini` holds your local settings and is never committed to Git.

## Pipeline Execution
Run the full ETL pipeline from the project root:
```bash
python src/consolidate_vendor_master.py
```

### Pipeline Stages
| Step | Stage | What happens |
|---|---|---|
| 1 | Configuration | Reads `config/config.ini` and starts a timestamped log file |
| 2 | Ingestion & lineage | Loads both CSVs, tags `source_system` (`ERP` / `MANUAL_LOG`) and combines them |
| 3 | Standardization | Trims whitespace, uppercases `vendor_id`, Title-Cases vendor names, maps country aliases |
| 4 | Parsing | Cleans currency strings (`$`, `,`, `USD`) to numbers; parses mixed date formats to ISO |
| 5 | Quality gate | Routes invalid records to quarantine with a `rejection_reason` |
| 6 | Imputation & dedup | Fills missing lead times with the valid median; keeps the latest record per vendor |
| 7 | Category explode | Splits comma-separated categories into vendor-category pairs with `.explode()` |
| 8 | Database setup | Creates the SQLite database and tables from `sql/ddl_schema.sql` |
| 9 | Load | Loads all three tables in one transaction (commit or full rollback) |
| 10 | Verification | Exports the quarantine report and runs `sql/verification_queries.sql` |
| 11 | Summary | Logs the execution summary and final status |

### Data Quality Rules
| Condition | Rejection reason |
|---|---|
| Missing or blank `vendor_name` | `MISSING_VENDOR_NAME` |
| Unparseable `contract_value_usd` (e.g. `TBD`) | `INVALID_CONTRACT_VALUE` |
| Negative `lead_time_days` | `NEGATIVE_LEAD_TIME` |

## Verification & Output Rules
Upon execution, the pipeline creates these outputs in `data/output/`:
* `pharmaceutical_procurement.db`: SQLite database containing the cleaned tables
  * `gold_vendor_master`: one row per unique vendor (expected: 5 rows)
  * `gold_vendor_categories`: normalized vendor-category bridge table (expected: 8 rows)
  * `err_vendor_quarantine`: rejected records with reasons (expected: 3 rows)
* `quarantine_report.csv`: records flagged for manual review

A log file `logs/consolidate_vendor_master_<YYYYMMDD_HHMMSS>.log` records every step, the verification query results and the final summary (expected total portfolio spend: 623,650.50 USD).

To run the verification queries manually:
```bash
sqlite3 -header -column data/output/pharmaceutical_procurement.db < sql/verification_queries.sql
```
