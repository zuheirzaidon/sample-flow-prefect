from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from prefect import task
from src.models import SampleRecord, MetadataRecord

import math


@task
def load_samples(file_path: str) -> pd.DataFrame:
    """Load samples records from CSV"""
    df = pd.read_csv(file_path)
    return df


@task
def load_metadata(file_path: str) -> pd.DataFrame:
    """Load metadata records from CSV"""
    df = pd.read_csv(file_path)
    return df


@task
def validate_samples(samples_df):
    validated_rows = []
    validity_flags = []

    for _, row in samples_df.iterrows():
        try:
            SampleRecord(**row.to_dict())
            validated_rows.append(row)
            validity_flags.append(True)
        except Exception:
            validated_rows.append(row)
            validity_flags.append(False)

    result = samples_df.copy()
    result["sample_valid"] = validity_flags
    return result


@task
def validate_metadata(metadata_df):
    validity_flags = []

    for _, row in metadata_df.iterrows():
        try:
            MetadataRecord(**row.to_dict())
            validity_flags.append(True)
        except Exception:
            validity_flags.append(False)

    result = metadata_df.copy()
    result["metadata_valid"] = validity_flags
    return result


@task
def merge_records(samples_df: pd.DataFrame, metadata_df: pd.DataFrame) -> pd.DataFrame:
    """Merge samples with metadata on sample_id."""
    merged = samples_df.merge(
        metadata_df,
        on = "sample_id",
        how = "left",
        validate = "one_to_one"
    )

    merged["has_metadata"] = merged["site"].notna()
    
    return merged


@task
def assign_processing_status(merged_df: pd.DataFrame) -> pd.DataFrame:
    """Assign a processing status to each sample record."""
    processed = merged_df.copy()

    processed["processed_at"] = datetime.now(timezone.utc).isoformat()

    def determine_status(row: pd.Series) -> str:
        if not bool(row["sample_valid"]):
            return "invalid_sample"
        if not bool(row["has_metadata"]):
            return "missing_metadata"
        if not bool(row["metadata_valid"]):
            return "invalid_metadata"
        return "ready"

    processed["processing_status"] = processed.apply(determine_status, axis=1)
    
    return processed


@task
def build_event_log(processed_df: pd.DataFrame) -> pd.DataFrame:
    """Generate a simple event log for each processed sample."""
    required_columns = ["sample_id", "processing_status", "processed_at"]
    missing = [col for col in required_columns if col not in processed_df.columns]
    if missing:
        raise ValueError(
            f"Processed dataframe is missing required columns for event log: {missing}. "
            f"Available columns: {list(processed_df.columns)}"
        )

    event_log = processed_df[required_columns].copy()
    event_log["event_type"] = "sample_processed"
    event_log = event_log[
        ["sample_id", "event_type", "processing_status", "processed_at"]
    ]
    return event_log


def build_summary(processed_df: pd.DataFrame) -> dict:
    """Create a summary dictionary for the processed run."""
    summary = {
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
        "total_samples": int(len(processed_df)),
        "ready_samples": int((processed_df["processing_status"] == "ready").sum()),
        "invalid_samples": int((processed_df["processing_status"] == "invalid_sample").sum()),
        "invalid_metadata": int((processed_df["processing_status"] == "invalid_metadata").sum()),
        "missing_metadata": int((processed_df["processing_status"] == "missing_metadata").sum()),
        "needs_attention": int(processed_df["needs_attention"].sum()) if "needs_attention" in processed_df.columns else 0,
        "high_priority_samples": int(processed_df["is_high_priority"].sum()) if "is_high_priority" in processed_df.columns else 0,
    }
    return summary


@task
def write_outputs(
    processed_df: pd.DataFrame,
    event_log_df: pd.DataFrame,
    summary: dict,
    aggregates: dict[str, pd.DataFrame],
    output_dir: str,
) -> None:
    """Write processed data, event log, summary outputs, and aggregate views."""
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    processed_df.to_csv(output_path / "processed_samples.csv", index=False)
    event_log_df.to_csv(output_path / "event_log.csv", index=False)

    with open(output_path / "summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    for name, df in aggregates.items():
        df.to_csv(output_path / f"{name}.csv", index=False)
        

####RUNTIME FIELDS#######

@task
def derive_runtime_fields(processed_df: pd.DataFrame) -> pd.DataFrame:
    """
    Derive operational runtime fields from processed records.
    """
    derived = processed_df.copy()

    now = datetime.now(timezone.utc).date()

    def parse_days_since_collection(value) -> float | None:
        if pd.isna(value):
            return None
        try:
            collected = pd.to_datetime(value).date()
            return (now - collected).days
        except Exception:
            return None

    derived["days_since_collection"] = derived["collection_date"].apply(parse_days_since_collection)
    derived["is_high_priority"] = derived["priority"].fillna("").str.lower().eq("high")

    def determine_needs_attention(row: pd.Series) -> bool:
        if row["processing_status"] != "ready":
            return True
        if bool(row["is_high_priority"]) and row["days_since_collection"] is not None and row["days_since_collection"] > 7:
            return True
        return False

    derived["needs_attention"] = derived.apply(determine_needs_attention, axis=1)

    def determine_processing_bucket(row: pd.Series) -> str:
        status = row["processing_status"]
        age = row["days_since_collection"]

        if status == "ready":
            return "ready_for_processing"
        if status in {"invalid_sample", "invalid_metadata"}:
            return "data_issue"
        if status == "missing_metadata":
            return "awaiting_metadata"
        if age is not None and age > 14:
            return "stalled"
        return "in_progress"

    derived["processing_bucket"] = derived.apply(determine_processing_bucket, axis=1)

    return derived

###AGGREGATES#####

@task
def build_aggregates(processed_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """
    Build lightweight aggregate operational views.
    """
    status_summary = (
        processed_df.groupby("processing_status", dropna=False)
        .size()
        .reset_index(name="sample_count")
        .sort_values("sample_count", ascending=False)
    )

    site_summary = (
        processed_df.assign(site=processed_df["site"].fillna("UNKNOWN"))
        .groupby(["site", "processing_status"], dropna=False)
        .size()
        .reset_index(name="sample_count")
        .sort_values(["site", "sample_count"], ascending=[True, False])
    )

    priority_summary = (
        processed_df.assign(priority=processed_df["priority"].fillna("UNKNOWN"))
        .groupby(["priority", "needs_attention"], dropna=False)
        .size()
        .reset_index(name="sample_count")
        .sort_values(["priority", "needs_attention"], ascending=[True, False])
    )

    return {
        "status_summary": status_summary,
        "site_summary": site_summary,
        "priority_summary": priority_summary,
    }






