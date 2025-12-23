from typing import Optional
from pydantic import BaseModel, ConfigDict
from src.modules.schemas.vessel import Speed, Position, PositionStatus, WeatherData, Links


class VesselInfo(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    name: Optional[str] = "None"
    company_id: Optional[str] = "None"
    imo: Optional[int] = 0
    flag: Optional[str] = "Malaysia"
    type: Optional[str] = "None"
    family: Optional[str] = "None"
    image_url: Optional[str] = "None"
    dwt: Optional[float] = 0
    service_speed: Optional[float] = 0
    additional_info: Optional[dict] = {}


class VesselKPI(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    name: str
    mmsi: int

    imo: Optional[int]
    company_id: Optional[str] = ""
    fleet_type: Optional[str] = ""
    beaufort: Optional[int] = 0
    daily_est_cons: Optional[float] = 0
    daily_avg_cons: Optional[float] = 0
    speed: Speed = Speed()
    position: Position = Position()
    position_status: PositionStatus = PositionStatus()
    ais_position: Position = Position()
    weather: WeatherData = WeatherData()
    links: Links = Links()


class VesselPosition(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    vessel_id: Optional[int] = 0
    name: Optional[str] = "None"
    imo: Optional[int] = 0
    mmsi: Optional[int] = 0
    fleet_type: Optional[str] = "None"
    position: Position = Position()
    ais_position: Position = Position()
    position_status: Optional[str] = "None"


class VesselLinks(Links):
    vessel_id: Optional[int] = 0
