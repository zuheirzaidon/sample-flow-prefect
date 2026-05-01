from pydantic import BaseModel, Field
from typing import Optional


class SampleRecord(BaseModel):
    sample_id: str
    species: str
    collection_date: Optional[str]
    status: str


class MetadataRecord(BaseModel):
    sample_id: str
    site: Optional[str]
    assay_type: Optional[str]
    priority: Optional[str]