from datetime import datetime
from enum import Enum
from typing import Optional
from pydantic import BaseModel, field_serializer, computed_field


class BaseModelTimestamp(BaseModel):
    timestamp: datetime = datetime(1970, 1, 1)

    @field_serializer("timestamp")
    def serialize_timestamp(self, timestamp: datetime, _info):
        return timestamp.timestamp()


class ParamName(str, Enum):
    me_rpm = "rpm"
    me_fuel_temp = "fot"
    me_oil_temp = "lot"
    me_fuel_cons = "cons"


class EngineName(str, Enum):
    ME1 = "ME1"
    ME2 = "ME2"
    ME3 = "ME3"
    AE1 = "AE1"
    AE2 = "AE2"
    AE3 = "AE3"


class FuelConsumption(BaseModelTimestamp):
    fuel_cons: Optional[float] = 0


class AEData(FuelConsumption):
    power: Optional[float] = 0


class MEData(FuelConsumption):
    running_hour: Optional[float] = 0
    rpm: Optional[float] = 0
    engine_load: Optional[float] = 0
    fuel_temp: Optional[float] = 0
    exhaust_gas_temp_left: Optional[float] = 0
    exhaust_gas_temp_right: Optional[float] = 0
    oil_temp: Optional[float] = 0
    running_hours_timestamp : Optional[datetime] = datetime(1970, 1, 1)
    
    @field_serializer("running_hours_timestamp")
    def serialize_running_hours_timestamp(self, timestamp: datetime, _info):
        if timestamp == datetime(1970, 1, 1):
            return None
        return timestamp.timestamp()

class MEResponse(BaseModel):
    me1: list[FuelConsumption] = [FuelConsumption()]
    me2: list[FuelConsumption] = [FuelConsumption()]
    me3: list[FuelConsumption] = [FuelConsumption()]
    total: list[FuelConsumption] = [FuelConsumption()]


class AUXResponse(BaseModel):
    ae1: list[AEData] = [AEData()]
    ae2: list[AEData] = [AEData()]
    ae3: list[AEData] = [AEData()]
    total: list[AEData] = [AEData()]


class AERTData(AEData):
    @computed_field
    def is_running(self) -> bool:
        if self.fuel_cons and self.fuel_cons > 0:
            return True
        return False


class MERTData(MEData):
    @computed_field
    def is_running(self) -> bool:
        if self.fuel_cons and self.fuel_cons > 0:
            return True
        return False
