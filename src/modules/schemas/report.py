from datetime import datetime, date
from typing import Optional
from pydantic import BaseModel, ConfigDict


class UnifiedDataset(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    me1_rpm: Optional[float]
    me2_rpm: Optional[float]
    me3_rpm: Optional[float]
    vessel_id: int
    nr_time: datetime
    nr_type: Optional[str]
    longitude: Optional[float]
    latitude: Optional[float]
    distance: Optional[float]
    average_speed_gps: Optional[float]
    distance_through_water: Optional[float]
    course: Optional[float]
    me1_run_hours: Optional[float]
    me1_consumption: Optional[float]
    me1_fuel_type: Optional[float]
    me1_power_at_shaft: Optional[float]
    me2_run_hours: Optional[float]
    me2_consumption: Optional[float]
    me2_fuel_type: Optional[float]
    me2_power_at_shaft: Optional[float]
    me3_run_hours: Optional[float]
    me3_consumption: Optional[float]
    me3_fuel_type: Optional[float]
    me3_power_at_shaft: Optional[float]
    ae1_run_hours: Optional[float]
    ae1_consumption: Optional[float]
    ae1_fuel_type: Optional[float]
    ae1_energy_produced: Optional[float]
    ae2_run_hours: Optional[float]
    ae2_consumption: Optional[float]
    ae2_fuel_type: Optional[float]
    ae2_energy_produced: Optional[float]
    ae3_run_hours: Optional[float]
    ae3_consumption: Optional[float]
    ae3_fuel_type: Optional[float]
    ae3_energy_produced: Optional[float]
    ae4_run_hours: Optional[float]
    ae4_consumption: Optional[float]
    ae4_fuel_type: Optional[float]
    ae4_energy_produced: Optional[float]
    wind_direction: Optional[float]
    wind_strength: Optional[float]
    sea_state: Optional[float]
    sea_state_direction: Optional[float]
    current_direction: Optional[float]
    current_strength: Optional[float]
    ballast_water: Optional[float]
    cargo_tonns: Optional[float]
    draft_forward: Optional[float]
    draft_aft: Optional[float]
    draft_middle: Optional[float]
    destination_port: Optional[float]
    departure_port: Optional[float]
    eta_next_port: Optional[float]
    charter_speed_order: Optional[float]
    boiler_consumption: Optional[float]
    boiler_fuel_type: Optional[float]
    reporting_period: Optional[float]
    update_datetime: Optional[datetime]
    speed_through_water: Optional[float]


class UnifiedDatasetResponse(BaseModel):
    data: list[UnifiedDataset] = []
    page_size: int
    page_number: int
    total_pages: int


class ConsumptionSummary(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    date: date
    avg_sog: Optional[float] = None
    avg_stw: Optional[float] = None
    sum_hours_me1: Optional[float] = None
    sum_hours_me2: Optional[float] = None
    sum_hours_me3: Optional[float] = None
    sum_cons_me1: Optional[float] = None
    sum_cons_me2: Optional[float] = None
    sum_cons_me3: Optional[float] = None
    sum_cons_me_total: Optional[float] = None
    sum_hours_ae1: Optional[float] = None
    sum_hours_ae2: Optional[float] = None
    sum_cons_ae1: Optional[float] = None
    sum_cons_ae2: Optional[float] = None
    sum_cons_ae_total: Optional[float] = None
    wind_scale: Optional[float] = None
    wave_scale: Optional[float] = None
    me1_run_hours : Optional[float] = None
    me2_run_hours : Optional[float] = None
    me3_run_hours : Optional[float] = None
    ae1_run_hours : Optional[float] = None
    ae2_run_hours : Optional[float] = None
    ae3_run_hours : Optional[float] = None
    ae4_run_hours : Optional[float] = None
