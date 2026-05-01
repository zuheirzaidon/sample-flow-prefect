# Sample Flow (Prefect)

A lightweight workflow orchestration project built with Prefect that ingests sample and metadata records from multiple sources, validates and merges them, tracks processing state via event logging, and generates summary outputs.

## Why this project exists

This project demonstrates a modular approach to building data workflows that resemble real-world operational pipelines. It focuses on the core patterns often needed in scientific and data-intensive environments:

- ingesting records from multiple sources
- validating schema and completeness
- merging related datasets
- assigning processing states
- generating event logs for traceability
- writing structured outputs for downstream use

The implementation uses mockup that fully resembles standard public data.

## Architecture

The workflow is organised into discrete Prefect tasks:

1. **Load inputs**  
   Read sample and metadata records from CSV files.

2. **Validate records**  
   Check required columns and basic completeness.

3. **Merge datasets**  
   Join records on `sample_id`.

4. **Assign processing status**  
   Categorise records as ready, missing metadata, invalid sample, or invalid metadata.

5. **Build event log**  
   Record a processing event for each sample.

6. **Write outputs**  
   Save processed records, event logs, and a summary report.

## Tech Stack

Prefect for workflow orchestration
Pandas for tabular data processing
Pydantic for typed validation
Pytest for testing
Docker for reproducible local execution

## Inputs

**samples.csv**

Contains core sample records such as:

- sample_id
- species
- collection_date
- status

**metadata.csv**

Contains sample-linked metadata such as:

- sample_id
- site
- assay_type
- priority

## Outputs

The pipeline writes the following outputs to data/output/:

**processed_samples.csv**

Merged and validated sample records with derived processing status.

**event_log.csv**

Event-level log showing how each sample was processed.

**summary.json**

A compact summary of run statistics, including counts by processing status.

## Example statuses

The workflow currently assigns:

- ready
- missing_metadata
- invalid_sample
- invalid_metadata


## Running locally

Create a virtual environment and install dependencies:

```
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run the workflow:

```
python -m src.flow
```

## Testing

Run tests with:

```
pytest
```

## Future improvements

Planned extensions include:

- stronger schema validation with Pydantic models
- containerised execution with Docker
- richer input validation and error handling
- configurable data sources
- CI for automated test execution

## Motivation

This repo is intended as a small but production-minded example of workflow orchestration and modular pipeline design, inspired by patterns common in scientific and operational data systems.

## Run with Docker

### Build the image:

```bash
docker build -t sample-flow-prefect .
```

### Run the workflow:

```
docker run --rm \
  -v "$(pwd)/data/raw:/app/data/raw" \
  -v "$(pwd)/data/output:/app/data/output" \
  sample-flow-prefect
```

## Runtime-derived metrics

In addition to validating and merging records, the workflow derives lightweight operational fields at run time, including:

- `days_since_collection`
- `is_high_priority`
- `needs_attention`
- `processing_bucket`

These fields are then aggregated into simple monitoring views to demonstrate how record-level data can be transformed into operational metrics.

## Aggregate outputs

The workflow also generates lightweight aggregate outputs:

- `status_summary.csv`
- `site_summary.csv`
- `priority_summary.csv`

These provide simple examples of how pipeline outputs can be turned into monitoring and decision-support views.

## Add CLI usage section

```markdown
## CLI usage

Run the pipeline with custom input/output paths:

```bash
python -m src.flow --input data/raw --output data/output
```

## Project structure

```text
sample-flow-prefect/
├── README.md
├── requirements.txt
├── .gitignore
├── data/
│   ├── raw/
│   │   ├── samples.csv
│   │   └── metadata.csv
│   └── output/
├── src/
│   ├── __init__.py
│   ├── flow.py
│   ├── tasks.py
│   ├── models.py
│   ├── utils.py
│   └── config.py
└── tests/
    └── test_tasks.py
```