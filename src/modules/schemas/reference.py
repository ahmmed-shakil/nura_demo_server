from typing import Optional
from pydantic import BaseModel, ConfigDict


class PointLocation(BaseModel):
    lat: Optional[float] = 0
    long: Optional[float] = 0


class GisPointInfo(BaseModel):
    gis_point_type: Optional[str] = "None"
    gis_point_name: Optional[str] = "None"
    gis_point_description: Optional[str] = "None"


class GisPoint(GisPointInfo, PointLocation):
    model_config = ConfigDict(from_attributes=True)
