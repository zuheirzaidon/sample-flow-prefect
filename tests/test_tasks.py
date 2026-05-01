import pandas as pd

from src.tasks import (
    validate_samples,
    validate_metadata,
    merge_records,
    assign_processing_status,
    build_summary,
    derive_runtime_fields, 
    build_aggregates
)


def test_validate_samples_adds_sample_valid_column():
    df = pd.DataFrame(
        [
            {
                "sample_id": "S001",
                "species": "Felis catus",
                "collection_date": "2025-01-10",
                "status": "pending",
            }
        ]
    )

    result = validate_samples.fn(df)

    assert "sample_valid" in result.columns
    assert bool(result.loc[0, "sample_valid"]) is True


def test_validate_metadata_adds_metadata_valid_column():
    df = pd.DataFrame(
        [
            {
                "sample_id": "S001",
                "site": "SiteA",
                "assay_type": "RNA-seq",
                "priority": "high",
            }
        ]
    )

    result = validate_metadata.fn(df)

    assert "metadata_valid" in result.columns
    assert bool(result.loc[0, "metadata_valid"]) is True


def test_merge_records_adds_has_metadata():
    samples = pd.DataFrame(
        [
            {
                "sample_id": "S001",
                "species": "Felis catus",
                "collection_date": "2025-01-10",
                "status": "pending",
                "sample_valid": True,
            }
        ]
    )

    metadata = pd.DataFrame(
        [
            {
                "sample_id": "S001",
                "site": "SiteA",
                "assay_type": "RNA-seq",
                "priority": "high",
                "metadata_valid": True,
            }
        ]
    )

    result = merge_records.fn(samples, metadata)

    assert "has_metadata" in result.columns
    assert bool(result.loc[0, "has_metadata"]) is True


def test_assign_processing_status_ready():
    merged = pd.DataFrame(
        [
            {
                "sample_id": "S001",
                "sample_valid": True,
                "metadata_valid": True,
                "has_metadata": True,
            }
        ]
    )

    result = assign_processing_status.fn(merged)

    assert result.loc[0, "processing_status"] == "ready"
    assert "processed_at" in result.columns


def test_assign_processing_status_missing_metadata():
    merged = pd.DataFrame(
        [
            {
                "sample_id": "S002",
                "sample_valid": True,
                "metadata_valid": False,
                "has_metadata": False,
            }
        ]
    )

    result = assign_processing_status.fn(merged)

    assert result.loc[0, "processing_status"] == "missing_metadata"


def test_build_summary_counts_statuses():
    processed = pd.DataFrame(
        [
            {
                "sample_id": "S001",
                "processing_status": "ready",
                "needs_attention": False,
                "is_high_priority": True,
            },
            {
                "sample_id": "S002",
                "processing_status": "missing_metadata",
                "needs_attention": True,
                "is_high_priority": False,
            },
            {
                "sample_id": "S003",
                "processing_status": "invalid_sample",
                "needs_attention": True,
                "is_high_priority": False,
            },
            {
                "sample_id": "S004",
                "processing_status": "invalid_metadata",
                "needs_attention": True,
                "is_high_priority": True,
            },
        ]
    )

    result = build_summary(processed)

    assert result["total_samples"] == 4
    assert result["ready_samples"] == 1
    assert result["missing_metadata"] == 1
    assert result["invalid_samples"] == 1
    assert result["invalid_metadata"] == 1
    assert result["needs_attention"] == 3
    assert result["high_priority_samples"] == 2


def test_derive_runtime_fields_adds_expected_columns():
    processed = pd.DataFrame(
        [
            {
                "sample_id": "S001",
                "collection_date": "2025-01-10",
                "priority": "high",
                "processing_status": "ready",
            },
            {
                "sample_id": "S002",
                "collection_date": None,
                "priority": "medium",
                "processing_status": "missing_metadata",
            },
        ]
    )

    result = derive_runtime_fields.fn(processed)

    assert "days_since_collection" in result.columns
    assert "is_high_priority" in result.columns
    assert "needs_attention" in result.columns
    assert "processing_bucket" in result.columns

    assert bool(result.loc[0, "is_high_priority"]) is True
    assert result.loc[1, "processing_bucket"] == "awaiting_metadata"


def test_build_aggregates_returns_expected_outputs():
    processed = pd.DataFrame(
        [
            {
                "sample_id": "S001",
                "site": "SiteA",
                "priority": "high",
                "processing_status": "ready",
                "needs_attention": False,
            },
            {
                "sample_id": "S002",
                "site": "SiteA",
                "priority": "high",
                "processing_status": "missing_metadata",
                "needs_attention": True,
            },
            {
                "sample_id": "S003",
                "site": "SiteB",
                "priority": "medium",
                "processing_status": "invalid_sample",
                "needs_attention": True,
            },
        ]
    )

    result = build_aggregates.fn(processed)

    assert "status_summary" in result
    assert "site_summary" in result
    assert "priority_summary" in result

    assert not result["status_summary"].empty
    assert not result["site_summary"].empty
    assert not result["priority_summary"].empty