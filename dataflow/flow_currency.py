"""
Flow 1 — Currency Exchange Pipeline
API: frankfurter.app
Output: data/currency_rates.csv

Simulate 2 scenarios:
  - Success: fetch real exchange rates EUR → USD, GBP, BRL, JPY, CHF
  - Forced Error: attempt to fetch invalid currency (XXX) → controlled failure
"""

import requests
import pandas as pd
import logging
import random
from datetime import datetime, timezone
from pathlib import Path
from prefect import flow, task, get_run_logger

# ── Config
BASE_URL = "https://api.frankfurter.app"
BASE_CCY = "EUR"
TARGET_CCY = ["USD", "GBP", "BRL", "JPY", "CHF"]
OUTPUT_DIR = Path("data")
OUTPUT_FILE = OUTPUT_DIR / "currency_rates.csv"

# Probability of injecting error (30% of the times)
ERROR_RATE = 0.3


# ----- Tasks -----

@task(name="fetch_exchange_rates", retries=1, retry_delay_seconds=5)
def fetch_rates(base: str, targets: list[str]) -> dict:
    logger = get_run_logger()
    logger.info("Fetching rates: %s → %s", base, targets)

    symbols = ",".join(targets)
    url = f"{BASE_URL}/latest?from={base}&to={symbols}"

    logger.info("Requesting URL: %s", url)
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        logger.error("Error fetching exchange rates: %s", e)
        raise
    logger.info("Successfully fetched rates! Status code: %d", response.status_code)
    data = response.json()
    logger.info("Rates fetched for date: %s", data.get("date"))
    return data


@task(name="inject_error_simulation")
def maybe_inject_error() -> bool:
    """Simulate error injection with probability ERROR_RATE."""
    logger = get_run_logger()

    if random.random() < ERROR_RATE:
        logger.warning("Error injection triggered fetching invalid currency XXX")
        url = f"{BASE_URL}/latest?from=EUR&to=XXX"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return True

    logger.info("No error injected this run")
    return False


@task(name="transform_to_dataframe")
def transform(data: dict) -> pd.DataFrame:
    logger = get_run_logger()

    rows = []
    for currency, rate in data["rates"].items():
        rows.append({
            "extracted_at": datetime.now(timezone.utc).isoformat(),
            "base_currency": data["base"],
            "target_currency": currency,
            "rate": rate,
            "date": data["date"],
        })

    df = pd.DataFrame(rows)
    logger.info("Transformed %d rows", len(df))
    return df


@task(name="save_to_csv")
def save_csv(df: pd.DataFrame) -> str:
    logger = get_run_logger()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Append to existing file if it exists, otherwise create new
    if OUTPUT_FILE.exists():
        existing = pd.read_csv(OUTPUT_FILE)
        df = pd.concat([existing, df], ignore_index=True)

    df.to_csv(OUTPUT_FILE, index=False)
    logger.info("File saved %d total rows to %s", len(df), OUTPUT_FILE)
    return str(OUTPUT_FILE)


# ── Flow ──────────────────────────────────────────────────────

@flow(
    name="currency-exchange-pipeline",
    description="Fetches EUR exchange rates and saves to CSV. Simulates random errors.",
    log_prints=True,
    flow_run_name=lambda: f"currency-run-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
)
def currency_pipeline():
    logger = get_run_logger()
    logger.info("Starting Currency Exchange Pipeline")

    # Try to inject error (30% chance) before fetching real rates
    maybe_inject_error()

    # Extract
    raw = fetch_rates(base=BASE_CCY, targets=TARGET_CCY)

    # Transform
    df = transform(raw)

    # Save
    path = save_csv(df)

    logger.info("Pipeline complete | output: %s", path)
    return path


# ── Entry point ───────────────────────────────────────────────
if __name__ == "__main__":
    currency_pipeline()