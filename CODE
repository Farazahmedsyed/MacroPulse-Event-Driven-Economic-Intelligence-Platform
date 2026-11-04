import json
import os
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd
import requests

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException


CONFIG_PATH = Path("/opt/airflow/config/series.json")
DATA_ROOT = Path("/opt/airflow/data")
REPORT_ROOT = Path("/opt/airflow/reports")


@dag(
    dag_id="macropulse_economic_pipeline",
    description="FRED economic data pipeline using Bronze, Silver, and Gold layers",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["fred", "economics", "duckdb", "portfolio"],
)
def macropulse_pipeline():

    @task
    def load_configuration():
        """Load and validate the economic-indicator configuration."""

        if not CONFIG_PATH.exists():
            raise AirflowFailException(
                f"Configuration file not found: {CONFIG_PATH}"
            )

        with CONFIG_PATH.open("r", encoding="utf-8") as file:
            indicators = json.load(file)

        if not indicators:
            raise AirflowFailException("No indicators were configured.")

        required_fields = {
            "id",
            "name",
            "category",
            "frequency",
            "units",
            "aggregation_method",
        }

        series_ids = []

        for indicator in indicators:
            missing = required_fields - set(indicator)

            if missing:
                raise AirflowFailException(
                    f"{indicator.get('id', 'Unknown')} is missing: {sorted(missing)}"
                )

            if indicator["aggregation_method"] not in {"avg", "sum", "eop"}:
                raise AirflowFailException(
                    f"Unsupported aggregation method for {indicator['id']}"
                )

            series_ids.append(indicator["id"])

        if len(series_ids) != len(set(series_ids)):
            raise AirflowFailException("Duplicate series IDs were found.")

        print(f"Configured {len(indicators)} economic indicators: {series_ids}")

        return indicators

    @task
    def fetch_fred_series(indicator):
        """Download one economic indicator and save the original response."""

        api_key = os.getenv("FRED_API_KEY")

        if not api_key:
            raise AirflowFailException(
                "FRED_API_KEY is missing from the environment."
            )

        series_id = indicator["id"]

        response = requests.get(
            "https://api.stlouisfed.org/fred/series/observations",
            params={
                "series_id": series_id,
                "api_key": api_key,
                "file_type": "json",
            },
            timeout=60,
        )

        response.raise_for_status()
        payload = response.json()

        payload["macropulse_metadata"] = {
            "configuration": indicator,
            "fetched_at": datetime.utcnow().isoformat(),
        }

        bronze_directory = (
            DATA_ROOT
            / "bronze"
            / "fred"
            / f"series_id={series_id}"
        )

        bronze_directory.mkdir(parents=True, exist_ok=True)

        output_path = bronze_directory / "observations.json"

        with output_path.open("w", encoding="utf-8") as file:
            json.dump(payload, file, indent=2)

        observation_count = len(payload.get("observations", []))

        print(f"Landed {series_id}: {observation_count} observations")

        return str(output_path)

    @task
    def build_silver(bronze_paths):
        """Clean the raw JSON responses and create a Silver Parquet table."""

        records = []

        for bronze_path in bronze_paths:
            with open(bronze_path, "r", encoding="utf-8") as file:
                payload = json.load(file)

            configuration = payload["macropulse_metadata"]["configuration"]

            for observation in payload.get("observations", []):
                raw_value = observation.get("value")

                if raw_value in {None, ".", ""}:
                    continue

                records.append(
                    {
                        "series_id": configuration["id"],
                        "series_name": configuration["name"],
                        "category": configuration["category"],
                        "frequency": configuration["frequency"],
                        "units": configuration["units"],
                        "aggregation_method": configuration[
                            "aggregation_method"
                        ],
                        "obs_date": observation["date"],
                        "value": float(raw_value),
                    }
                )

        dataframe = pd.DataFrame(records)

        if dataframe.empty:
            raise AirflowFailException("The Silver dataset is empty.")

        dataframe["obs_date"] = pd.to_datetime(dataframe["obs_date"])

        dataframe = (
            dataframe
            .drop_duplicates(subset=["series_id", "obs_date"])
            .sort_values(["series_id", "obs_date"])
        )

        silver_directory = DATA_ROOT / "silver"
        silver_directory.mkdir(parents=True, exist_ok=True)

        silver_path = silver_directory / "economic_observations.parquet"

        dataframe.to_parquet(
            silver_path,
            index=False,
        )

        print(
            f"Silver dataset created with {len(dataframe):,} observations "
            f"across {dataframe['series_id'].nunique()} indicators."
        )

        return str(silver_path)

    @task
    def build_gold(silver_path):
        """Create analytics-ready monthly and quarterly DuckDB tables."""

        gold_directory = DATA_ROOT / "gold"
        gold_directory.mkdir(parents=True, exist_ok=True)

        database_path = gold_directory / "macropulse.duckdb"

        safe_silver_path = silver_path.replace("'", "''")

        connection = duckdb.connect(str(database_path))

        connection.execute(
            f"""
            CREATE OR REPLACE TABLE silver_observations AS
            SELECT *
            FROM read_parquet('{safe_silver_path}');
            """
        )

        connection.execute(
            """
            CREATE OR REPLACE TABLE dim_series AS
            SELECT
                row_number() OVER (ORDER BY series_id) AS series_key,
                series_id,
                max(series_name) AS series_name,
                max(category) AS category,
                max(frequency) AS frequency,
                max(units) AS units,
                max(aggregation_method) AS aggregation_method
            FROM silver_observations
            GROUP BY series_id;
            """
        )

        connection.execute(
            """
            CREATE OR REPLACE TABLE fact_monthly_observation AS
            SELECT
                series.series_key,
                observations.series_id,
                observations.obs_date,
                observations.value,
                series.aggregation_method
            FROM silver_observations AS observations
            JOIN dim_series AS series
                USING (series_id);
            """
        )

        connection.execute(
            """
            CREATE OR REPLACE TABLE fact_macro_quarterly AS
            SELECT
                series_key,
                series_id,
                CAST(date_trunc('quarter', obs_date) AS DATE)
                    AS quarter_start,
                CASE
                    WHEN aggregation_method = 'avg'
                        THEN avg(value)
                    WHEN aggregation_method = 'sum'
                        THEN sum(value)
                    WHEN aggregation_method = 'eop'
                        THEN arg_max(value, obs_date)
                END AS quarterly_value,
                count(*) AS observation_count
            FROM fact_monthly_observation
            GROUP BY
                series_key,
                series_id,
                quarter_start,
                aggregation_method
            ORDER BY series_id, quarter_start;
            """
        )

        monthly_count = connection.execute(
            "SELECT count(*) FROM fact_monthly_observation"
        ).fetchone()[0]

        quarterly_count = connection.execute(
            "SELECT count(*) FROM fact_macro_quarterly"
        ).fetchone()[0]

        connection.close()

        print(
            f"Gold warehouse created: {monthly_count:,} monthly rows and "
            f"{quarterly_count:,} quarterly rows."
        )

        return str(database_path)

    @task
    def run_quality_checks(database_path):
        """Run automated warehouse validation checks."""

        connection = duckdb.connect(database_path, read_only=True)

        checks = {
            "warehouse_has_data": connection.execute(
                """
                SELECT count(*) > 0
                FROM fact_monthly_observation
                """
            ).fetchone()[0],

            "every_series_has_data": connection.execute(
                """
                SELECT count(*) = 0
                FROM dim_series AS series
                LEFT JOIN fact_monthly_observation AS facts
                    USING (series_key)
                WHERE facts.series_key IS NULL
                """
            ).fetchone()[0],

            "no_duplicate_observations": connection.execute(
                """
                SELECT count(*) = 0
                FROM (
                    SELECT series_id, obs_date, count(*) AS record_count
                    FROM fact_monthly_observation
                    GROUP BY series_id, obs_date
                    HAVING count(*) > 1
                )
                """
            ).fetchone()[0],

            "no_null_values": connection.execute(
                """
                SELECT count(*) = 0
                FROM fact_monthly_observation
                WHERE value IS NULL
                """
            ).fetchone()[0],
        }

        connection.close()

        failed_checks = [
            name
            for name, passed in checks.items()
            if not passed
        ]

        REPORT_ROOT.mkdir(parents=True, exist_ok=True)

        report_path = REPORT_ROOT / "quality_report.json"

        with report_path.open("w", encoding="utf-8") as file:
            json.dump(checks, file, indent=2)

        for check_name, passed in checks.items():
            status = "PASSED" if passed else "FAILED"
            print(f"{check_name}: {status}")

        if failed_checks:
            raise AirflowFailException(
                f"Quality checks failed: {failed_checks}"
            )

        return checks

    @task
    def publish_summary(database_path, quality_results):
        """Create a summary that can be displayed by a dashboard."""

        connection = duckdb.connect(database_path, read_only=True)

        summary = connection.execute(
            """
            SELECT
                count(DISTINCT series_id) AS indicators,
                count(*) AS monthly_observations,
                min(obs_date) AS earliest_date,
                max(obs_date) AS latest_date
            FROM fact_monthly_observation
            """
        ).fetchone()

        connection.close()

        result = {
            "indicators": summary[0],
            "monthly_observations": summary[1],
            "earliest_date": str(summary[2]),
            "latest_date": str(summary[3]),
            "quality_checks": quality_results,
            "published_at": datetime.utcnow().isoformat(),
        }

        REPORT_ROOT.mkdir(parents=True, exist_ok=True)

        output_path = REPORT_ROOT / "pipeline_summary.json"

        with output_path.open("w", encoding="utf-8") as file:
            json.dump(result, file, indent=2)

        print("MacroPulse pipeline completed successfully.")
        print(json.dumps(result, indent=2))

    configuration = load_configuration()

    bronze_files = fetch_fred_series.expand(
        indicator=configuration
    )

    silver_file = build_silver(bronze_files)
    gold_database = build_gold(silver_file)
    quality_report = run_quality_checks(gold_database)

    publish_summary(
        gold_database,
        quality_report,
    )


macropulse_pipeline()
