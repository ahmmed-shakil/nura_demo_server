from datetime import datetime
from typing import Optional
from pydantic import BaseModel, ConfigDict, Field, field_serializer
from src.modules.schemas.engine import BaseModelTimestamp, AEData, MEData
from src.modules.schemas.reference import GisPointInfo, PointLocation


class Position(PointLocation, BaseModelTimestamp):
    direction: Optional[float] = 0


class PositionStatus(BaseModelTimestamp, GisPointInfo):
    status_name: Optional[str] = "None"
    status_description: Optional[str] = "None"
    distance: Optional[float] = 0

    model_config = ConfigDict(from_attributes=True)


class Utilization(BaseModel):
    port: Optional[float] = 0
    anchorage: Optional[float] = 0
    underway: Optional[float] = 0
    awaiting_data: Optional[float] = 0
    offshore_ops: Optional[float] = 0
    offshore_stby: Optional[float] = 0
    other: Optional[float] = 0


class Links(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    online: Optional[float] = 0
    nmea: Optional[float] = 0
    noon: Optional[float] = 0
    canbus: Optional[float] = 0
    position: Optional[float] = 0
    speed: Optional[float] = 0
    # We have at least 1 main engine
    me1: Optional[float] = 0
    me2: Optional[float] = -1
    me3: Optional[float] = -1
    # We have at least 1 generator
    ae1: Optional[float] = 0
    ae2: Optional[float] = -1
    ae3: Optional[float] = -1


class Alert(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    type: int = 0
    vessel_name: str = ""
    active_since: datetime = Field(
        validation_alias="time", default=datetime(1970, 1, 1)
    )
    time: datetime = datetime(1970, 1, 1)
    text: str = ""
    lat: Optional[float] = 0
    long: Optional[float] = 0
    note: str = Field(validation_alias="text", default="")

    # TODO: Define this via DB table.
    _mapping = {1: "critical", 2: "warning", 3: "warning", 4: "info", 5: "info", 0: ""}

    @field_serializer("active_since", "time")
    def serialize_timestamp(self, v: datetime, _info) -> float:
        return v.timestamp()

    @field_serializer("type")
    def serialize_type(self, v: int, _info) -> str:
        if v not in self._mapping:
            raise ValueError(f"type must be one of {list(self._mapping.keys())}")
        return self._mapping[v]


class WeatherData(BaseModelTimestamp):
    model_config = ConfigDict(from_attributes=True)

    current_direction: Optional[float] = 0
    wave_direction: Optional[float] = 0
    wind_direction: Optional[float] = 0
    beaufourt: Optional[int] = 0
    dss: Optional[int] = 0


class WeatherResistance(BaseModelTimestamp):
    model_config = ConfigDict(from_attributes=True)

    vessel_heading: Optional[float]
    wind_speed: Optional[float]
    wind_direction: Optional[float]
    wave_height: Optional[float]
    wave_direction: Optional[float]
    current_speed: Optional[float]
    current_direction: Optional[float]
    added_resistance: Optional[float] = None


class Speed(BaseModelTimestamp):
    model_config = ConfigDict(from_attributes=True)

    sog: Optional[float] = None
    stw: Optional[float] = None


class UnifiedData(BaseModelTimestamp):
    lat: Optional[float] = 0
    long: Optional[float] = 0
    direction: Optional[float] = 0
    sog: Optional[float] = 0
    stw: Optional[float] = 0
    me1: MEData = MEData()
    me2: MEData = MEData()
    me3: MEData = MEData()
    ae1: AEData = AEData()
    ae2: AEData = AEData()


class VesselData(UnifiedData):
    position: Position = Position() # TODO: Dublicate position from unified with another logic
    ais_position: Position = Position()
    weather: WeatherData = WeatherData()
    status: PositionStatus = PositionStatus()

class VesselDataPlayBack(BaseModel):
    engine:Optional[list] = None
    position: Position = Position() # TODO: Dublicate position from unified with another logic
    ais_position: Position = Position()
    weather: WeatherData = WeatherData()
    status: PositionStatus = PositionStatus()


class EngineDetails(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    type: Optional[str] = "None"
    label: Optional[str] = "None"
    additional_info: Optional[dict] = {}


class PropulsionMetrics(BaseModel):
    rob: Optional[float] = 0
    total_fuel_cons: Optional[float] = 0
    daily_avg_cons: Optional[float] = 0
    sfoc_deviation: Optional[float] = 0
    me1_run_hours: Optional[float] = 0
    me2_run_hours: Optional[float] = 0
    me3_run_hours: Optional[float] = 0
    me1_run_hours_timestamp : Optional[datetime] = datetime(1970, 1, 1)
    me2_run_hours_timestamp : Optional[datetime] = datetime(1970, 1, 1)
    me3_run_hours_timestamp : Optional[datetime] = datetime(1970, 1, 1)


class RunHours(BaseModel):
    me1_start: float = 0
    me1_end: float = 0
    me2_start: float = 0
    me2_end: float = 0
    me3_start: float = 0
    me3_end: float = 0
