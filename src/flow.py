from __future__ import annotations
from pathlib import Path
from prefect import flow

from src.tasks import (
    assign_processing_status,
    build_aggregates,
    build_event_log,
    build_summary,
    derive_runtime_fields,
    load_data,
    merge_records,
    validate_metadata,
    validate_samples,
    write_outputs,
)

import argparse


@flow(name="sample-flow")
def run_sample_flow(
    input_dir: str = "data/raw",
    output_dir: str = "data/output",
) -> None:
    """
    Run the sample flow pipeline:
    load -> validate -> merge -> assign status -> derive runtime fields
    -> event log -> aggregates -> summary -> write outputs
    """

    input_path = Path(input_dir)
    output_path = Path(output_dir)

    samples_path = input_path / "samples.csv"
    metadata_path = input_path / "metadata.csv"

    samples = load_data(samples_path)
    metadata = load_data(metadata_path)

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

    run_sample_flow(
        input_dir = args.input,
        output_dir=args.output,
    )