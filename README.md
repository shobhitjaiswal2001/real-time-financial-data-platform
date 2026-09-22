\# Real-Time Financial Data Platform



An end-to-end data engineering platform that ingests, processes, and analyzes multi-stock financial data using both \*\*batch\*\* and \*\*streaming\*\* paradigms — built to demonstrate production-style data engineering practices: medallion architecture, orchestration, incremental processing, containerization, and observability.



\## Architecture



Alpha Vantage API → Raw JSON → Bronze (Parquet) → Silver → Gold (PySpark)

→ PostgreSQL → dbt (staging → fact → mart) → Power BI



Kafka Producer (simulated live ticks) → Kafka Topic

→ Spark Structured Streaming (windowed rolling averages)

→ PostgreSQL (live\_ticks)



Orchestration: Apache Airflow (Dockerized, 7-task DAG)

Alerting: Email notifications on pipeline failure





\## What This Project Demonstrates



\- \*\*Medallion architecture\*\* (Bronze → Silver → Gold) using PySpark

\- \*\*Multi-symbol batch pipeline\*\* (IBM, AAPL, MSFT) with correctly partitioned window functions (moving averages, volatility, returns — computed independently per symbol)

\- \*\*Incremental data loading\*\* — only new rows are upserted into PostgreSQL, not a full rewrite every run

\- \*\*dbt\*\* for staging → fact → mart transformations, with automated data quality tests

\- \*\*Airflow orchestration\*\* — the entire pipeline runs as a 7-task DAG inside a custom Docker image (Java + PySpark + dbt), with per-task retries and failure isolation

\- \*\*Email alerting\*\* on pipeline failure via SMTP

\- \*\*Kafka + Spark Structured Streaming\*\* — a real-time layer computing windowed rolling price averages from a simulated live tick stream, written continuously to PostgreSQL with checkpointed fault tolerance



\## Tech Stack



| Layer | Tools |

|---|---|

| Ingestion | Python, Alpha Vantage API |

| Processing | PySpark (batch + Structured Streaming) |

| Streaming | Apache Kafka |

| Storage | Parquet, PostgreSQL |

| Transformation | dbt |

| Orchestration | Apache Airflow (Docker) |

| Containerization | Docker, Docker Compose |



\## Project Structure



├── src/

│ ├── ingestion/ # Alpha Vantage API ingestion

│ ├── transformation/ # Bronze -> Silver -> Gold (PySpark)

│ ├── warehouse/ # PostgreSQL loader (incremental)

│ └── streaming/ # Kafka producer, consumer, Spark Structured Streaming

├── financial\_dbt/ # dbt models: staging -> fact -> mart

├── airflow/ # Airflow DAG, Dockerfile, orchestration config

├── kafka/ # Kafka broker (KRaft mode) Docker Compose

└── run\_pipeline.py # Standalone batch pipeline runner





\## Key Engineering Decisions



\- \*\*Per-symbol window partitioning\*\*: early versions computed rolling metrics without `partitionBy("symbol")`, which would silently blend calculations across different stocks once multiple symbols were added. Fixed by partitioning all window functions by symbol.

\- \*\*Incremental writes over full reprocessing\*\*: Gold-layer recomputation is necessary for correct rolling windows, but database writes are filtered to only new rows per symbol — avoiding unnecessary I/O on unchanged historical data.

\- \*\*LocalExecutor over CeleryExecutor for Airflow\*\*: given single-machine deployment constraints, LocalExecutor removes the Celery/Redis dependency entirely while still providing full DAG-based orchestration, retries, and task isolation.

\- \*\*Windowed streaming aggregation with checkpointing\*\*: the streaming layer uses Spark Structured Streaming's watermarking and checkpointing for fault-tolerant, exactly-once-ish rolling average computation — not just a stateless print loop.



\## Status



Batch pipeline, orchestration, alerting, and streaming layer are complete and tested end-to-end. Power BI dashboard integration in progress.

