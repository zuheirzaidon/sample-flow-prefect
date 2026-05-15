from pydantic import BaseModel, Field
from typing import Optional, Annotated

SAMPLE_ID = Annotated[str, Field(pattern=r"^S\d{3}$")]

class SampleRecord(BaseModel):
    sample_id: SAMPLE_ID
    species: str
    collection_date: Optional[str]
    status: str = Field(
        default = "pending"
    )

class MetadataRecord(BaseModel):
    sample_id: SAMPLE_ID
    site: Optional[str]
    assay_type: Optional[str]
    priority: str = Field(
        default = "medium"
    )