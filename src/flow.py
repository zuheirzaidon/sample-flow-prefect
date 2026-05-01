from __future__ import annotations

from prefect import flow

from src.tasks import (
    assign_processing_status,
    build_aggregates,
    build_event_log,
    build_summary,
    derive_runtime_fields,
    load_metadata,
    load_samples,
    merge_records,
    validate_metadata,
    validate_samples,
    write_outputs,
)

import argparse


@flow(name="sample-flow")
def run_sample_flow(
    samples_path: str = "data/raw/samples.csv",
    metadata_path: str = "data/raw/metadata.csv",
    output_dir: str = "data/output",
) -> None:
    """
    Run the sample flow pipeline:
    load -> validate -> merge -> assign status -> derive runtime fields
    -> event log -> aggregates -> summary -> write outputs
    """
    samples = load_samples(samples_path)
    metadata = load_metadata(metadata_path)

    validated_samples = validate_samples(samples)
    validated_metadata = validate_metadata(metadata)

    merged = merge_records(validated_samples, validated_metadata)
    processed = assign_processing_status(merged)
    processed = derive_runtime_fields(processed)

    event_log = build_event_log(processed)
    aggregates = build_aggregates(processed)
    summary = build_summary(processed)

    write_outputs(processed, event_log, summary, aggregates, output_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run sample flow pipeline")

    parser.add_argument(
        "--input",
        type=str,
        default="data/raw",
        help="Input directory containing samples.csv and metadata.csv",
    )

    parser.add_argument(
        "--output",
        type=str,
        default="data/output",
        help="Output directory",
    )

    args = parser.parse_args()

    samples_path = f"{args.input}/samples.csv"
    metadata_path = f"{args.input}/metadata.csv"

    run_sample_flow(
        samples_path=samples_path,
        metadata_path=metadata_path,
        output_dir=args.output,
    )