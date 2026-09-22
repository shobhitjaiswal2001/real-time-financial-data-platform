"""
run_pipeline.py

Automates the full financial data pipeline:
Alpha Vantage -> Raw JSON -> Bronze Parquet -> Silver -> Gold -> PostgreSQL -> dbt run -> dbt test

Usage:
    python run_pipeline.py
"""

import glob
import logging
import subprocess
import sys
from datetime import datetime
from pathlib import Path

# ============================================================
# PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent
DBT_PROJECT_DIR = PROJECT_ROOT / "financial_dbt"
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

GOLD_DIR = PROJECT_ROOT / "data" / "gold" / "daily_prices"

PYTHON = sys.executable  # uses the currently active .venv interpreter


# ============================================================
# LOGGING
# ============================================================

log_file = LOG_DIR / f"pipeline_{datetime.now():%Y%m%d_%H%M%S}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)

log = logging.getLogger("pipeline")


# ============================================================
# STEP RUNNER
# ============================================================

def run_step(name: str, command: list[str], cwd: Path = PROJECT_ROOT) -> None:
    """
    Runs a subprocess step. Raises RuntimeError with captured output on failure.
    """
    log.info(f"START: {name}")
    log.info(f"Command: {' '.join(command)} (cwd={cwd})")

    result = subprocess.run(
        command,
        cwd=str(cwd),
        capture_output=True,
        text=True,
    )

    if result.stdout:
        log.info(result.stdout.strip())
    if result.stderr:
        # dbt/Spark often print INFO/WARN to stderr, so log as info, not error
        log.info(result.stderr.strip())

    if result.returncode != 0:
        log.error(f"FAILED: {name} (exit code {result.returncode})")
        raise RuntimeError(f"Pipeline step failed: {name}")

    log.info(f"SUCCESS: {name}")


# ============================================================
# STEP 1: INGEST FROM ALPHA VANTAGE
# ============================================================

def step_ingest():
    run_step(
        "Ingestion (Alpha Vantage -> Raw JSON)",
        [PYTHON, "src/ingestion/alpha_vantage.py"],
    )


# ============================================================
# STEP 2: RAW JSON -> BRONZE PARQUET
# ============================================================

def step_bronze():
    run_step(
        "Raw JSON -> Bronze Parquet",
        [PYTHON, "src/transformation/json_to_parquet.py"],
    )


# ============================================================
# STEP 3: BRONZE -> SILVER (Spark)
# ============================================================

def step_silver():
    run_step(
        "Bronze -> Silver (Spark)",
        [PYTHON, "src/transformation/spark_silver.py"],
    )


# ============================================================
# STEP 4: SILVER -> GOLD (Spark)
# ============================================================

def step_gold():
    run_step(
        "Silver -> Gold (Spark)",
        [PYTHON, "src/transformation/spark_gold.py"],
    )


# ============================================================
# STEP 5: GOLD -> POSTGRESQL
# ============================================================

def step_postgres_load():
    if GOLD_DIR.is_file():
        gold_file = str(GOLD_DIR)
    elif GOLD_DIR.is_dir():
        parquet_files = glob.glob(str(GOLD_DIR / "*.parquet"))
        if not parquet_files:
            raise RuntimeError(f"No Gold parquet file found in: {GOLD_DIR}")
        gold_file = max(parquet_files, key=lambda f: Path(f).stat().st_mtime)
    else:
        raise RuntimeError(f"Gold output not found at: {GOLD_DIR}")

    log.info(f"Using Gold file: {gold_file}")

    run_step(
        "Gold -> PostgreSQL",
        [PYTHON, "src/warehouse/postgres_loader.py", gold_file],
    )


# ============================================================
# STEP 6: DBT RUN
# ============================================================

def step_dbt_run():
    run_step(
        "dbt run",
        ["dbt", "run"],
        cwd=DBT_PROJECT_DIR,
    )


# ============================================================
# STEP 7: DBT TEST
# ============================================================

def step_dbt_test():
    run_step(
        "dbt test",
        ["dbt", "test"],
        cwd=DBT_PROJECT_DIR,
    )


# ============================================================
# MAIN PIPELINE
# ============================================================

def main():
    log.info("=" * 60)
    log.info("PIPELINE STARTED")
    log.info("=" * 60)

    steps = [
        step_ingest,
        step_bronze,
        step_silver,
        step_gold,
        step_postgres_load,
        step_dbt_run,
        step_dbt_test,
    ]

    for step_fn in steps:
        try:
            step_fn()
        except Exception as e:
            log.error(f"PIPELINE FAILED at step: {step_fn.__name__}")
            log.error(str(e))
            log.error("=" * 60)
            log.error("PIPELINE STATUS: FAILED")
            log.error("=" * 60)
            sys.exit(1)

    log.info("=" * 60)
    log.info("PIPELINE STATUS: SUCCESS — all steps completed")
    log.info("=" * 60)


if __name__ == "__main__":
    main()