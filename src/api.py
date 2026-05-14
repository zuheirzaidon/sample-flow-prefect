import pandas as pd
import json

from fastapi import FastAPI
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = BASE_DIR / "data" / "output"

def clean_api_dataframe(df:pd.DataFrame) -> pd.DataFrame:
    df = df.astype(object).where(pd.notna(df), None)
    df.columns = df.columns.str.strip()

    for col in df.columns:
        df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
    return df

app = FastAPI()

@app.get("/")
def root():
    return {"message": "Biological Sample QC Pipeline"}

@app.get("/event_log")
def  get_event_log():
    df = pd.read_csv(OUTPUT_DIR / "event_log.csv")
    df = df.astype(object).where(pd.notna(df), None)
    return df.to_dict(orient="records")

@app.get("/priority_summary")
def get_priority_summary():
    df = pd.read_csv(OUTPUT_DIR / "priority_summary.csv")
    df = df.astype(object).where(pd.notna(df), None)
    return df.to_dict(orient="records")

@app.get("/processed_samples")
def get_processed_samples():
    df = pd.read_csv(OUTPUT_DIR / "processed_samples.csv")
    df = df.astype(object).where(pd.notna(df), None)
    return df.to_dict(orient="records")

@app.get("/priority_summary")
def get_site_summary():
    df = pd.read_csv(OUTPUT_DIR / "site_summary.csv")
    df = df.astype(object).where(pd.notna(df), None)
    return df.to_dict(orient="records")

@app.get("/status_summary")
def get_status_summary():
    df = pd.read_csv(OUTPUT_DIR / "status_summary.csv")
    df = df.astype(object).where(pd.notna(df), None)
    return df.to_dict(orient="records")

@app.get("/summary")
def get_summary():
    with open(OUTPUT_DIR / "summary.json") as f:
        return json.load(f)
    
@app.get("/sample/{sample_id}")
def get_sample_id(sample_id:str):
    df = pd.read_csv(OUTPUT_DIR / "processed_samples.csv")
    df = df.astype(object).where(pd.notna(df), None)

    result = df[df["sample_id"] == sample_id]

    if result.empty:
        return {"error": "sample_id not found"}

    return result.to_dict(orient="records")[0]

