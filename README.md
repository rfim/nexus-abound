# NEXUS for Abound

An independent Data Engineer (Platform) portfolio by R. Firman Insan Muhammad (Vim), for Abound.

**Live site:** https://rfim.github.io/nexus-abound/

This repository applies the methods I used at Lifepal to data shaped like a consumer lender's: CDC from Aurora with AWS DMS into an S3 data lake of Iceberg tables, applied by Spark (on Glue in production), reconciled to the source row by row, laid out for Athena cost, and turned into the tables model calibration needs. The numbers on the site come from a real run of the PySpark code here, on synthetic data.

## The path

```
Aurora (simulated) → DMS files on "S3" → bronze (every change) → silver (current state) → checks → reload / erasure → layout → gold
    generate.py        full load + CDC     bronze.py              silver.py              checks.py   pipeline.py       layout.py  gold.py
```

- **Capture.** `src/nexus/generate.py` runs a small lender forward a year (customers, applications with a stored score, loans, repayments, Open Banking transactions) and writes what DMS would: a full load on 1 July, then CDC files with `Op`, `cdc_ts` (TimestampColumnName) and `change_seq` (AR_H_CHANGE_SEQ). File modification times are the delivery times, like S3 LastModified.
- **Land.** `bronze.py` lists files by arrival, reads mixed headers apart (a column added mid-stream), adds new columns to the Iceberg table, and MERGEs on (primary key, change_seq): a change re-sent after a task restart is skipped.
- **Apply.** `silver.py` MERGEs the newest change per key, only if its change_seq is newer. Deletes are tombstones with their values nulled, so an older update arriving late can't resurrect the row. A reload is applied as authoritative (`WHEN NOT MATCHED BY SOURCE`). History tables (SCD2) come from the bronze log.
- **Prove.** `checks.py` reconciles silver to the source's own rows on every contract column, runs the quality rules in `contracts/quality.yml`, checks freshness, and checks that an erased customer's PII is in no snapshot the table can time-travel to.
- **Serve.** `layout.py` plans the same queries on three layouts of the transactions table from Iceberg's file statistics (bytes scanned is the Athena bill). `gold.py` builds the month-end loan book, the calibration set and the point-in-time income feature.

## The six planted problems

| Problem | Where | What catches it |
|---|---|---|
| DMS re-sends two hours of changes after a task restart | 12 Aug | bronze keys on (pk, change_seq) |
| A CDC file delivered 15 days late | customers, 3 Sep | listing by arrival; change_seq ordering |
| A delete, then an older update arriving after it | an erased applicant | tombstones in silver |
| `ALTER TABLE loans ADD COLUMN apr_bps` | 1 Aug | schema evolution in bronze, a quality rule in silver |
| Two statements in one transaction, same commit time | late payments | AR_H_CHANGE_SEQ |
| Changes DMS never delivered | repayments, 20 Sep 02:00–04:00 | reconciliation to source, then a reload |

## AWS

- `infra/dms/`: the S3 target settings, table mappings (AR_H_CHANGE_SEQ → `change_seq`) and task settings the pipeline expects.
- `glue/cdc_job.py`: a Glue 5.0 job (Spark 3.5, Iceberg 1.7) that configures the Glue Data Catalog and runs the same bronze, silver and check functions.
- `infra/terraform/main.tf`: KMS, the S3 lake, DMS instance, endpoints, task and a latency alarm, Glue databases, job and schedule, least-privilege IAM.

Written to be read; not applied, as there is no AWS account behind a portfolio.

## Run it

Python 3.11 and Java 17:

```bash
pip install -r requirements.txt
PYTHONPATH=src python -m pytest -q     # generator, CDC semantics on Spark + Iceberg, layout, published data, site
./scripts/build.sh                     # the whole run; rewrites assets/data/site.json (about 3 minutes)
python -m http.server 8000             # then open http://localhost:8000/
```

Locally the tables live in an Iceberg Hadoop catalog named `lake` under `target/warehouse`. On Glue, `NEXUS_CATALOG=glue_catalog` points the same code at the Glue Data Catalog.

## What's real

- **Real:** the Lifepal outcomes (from my CV and production work) and all the code here, which runs end to end.
- **Synthetic:** every customer, loan and transaction. The Abound side comes from the job description and public information; no Abound systems or data were used.
- **Depth:** Delta Lake is what I run in production; this repository is my Iceberg work. DMS, Spark, Glue and Aurora/RDS PostgreSQL and MySQL are production experience; Athena is working level.

This is an independent application. It is not affiliated with or endorsed by Abound.
