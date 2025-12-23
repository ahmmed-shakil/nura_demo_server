from typing import Annotated
from fastapi import APIRouter, HTTPException, Query, Depends
from src.modules.api import CommonParams
import src.modules.schemas.vessel as schema
import src.modules.utils.vessel as proc
from src.modules.demo.vessels import anonymize_bulkset, anonymize_link


router = APIRouter()
router_v1 = APIRouter() # Remove when switch to async

@router.get("/position", response_model=list[schema.Position])
async def get_position(
    commons: Annotated[CommonParams, Depends()],
    latest: Annotated[
        bool, Query(description="Flag to load latest or real time data")
    ] = False,
    source: Annotated[
        str,
        Query(
            description="Sources is internal or ais data",
            max_length=50,
        ),
    ] = "internal",
):
    match source:
        case "internal":
            content = proc.get_position_data(commons.imo, commons.start_date, commons.end_date, latest)
        case "ais":
            content = proc.get_ais_data(commons.imo, commons.start_date, commons.end_date, latest)
        case _:
            raise HTTPException(
                status_code=400,
                detail="Wrong 'source' parameter value.",
            )
    return content


# Remove when switch to async
@router_v1.get("/position", response_model=list[schema.Position])
async def get_async_position(
    commons: Annotated[CommonParams, Depends()],
    latest: Annotated[
        bool, Query(description="Flag to load latest or real time data")
    ] = False,
    source: Annotated[
        str,
        Query(
            description="Sources is internal or ais data",
            max_length=50,
        ),
    ] = "internal",
):
    match source:
        case "internal":
            content = await proc.get_async_position_data(commons.imo, commons.start_date, commons.end_date, latest)
        case "ais":
            content = await proc.get_async_ais_data(commons.imo, commons.start_date, commons.end_date, latest)
        case _:
            raise HTTPException(
                status_code=400,
                detail="Wrong 'source' parameter value.",
            )
    return content


@router.get("/speed", response_model=list[schema.Speed])
async def get_speed(
    commons: Annotated[CommonParams, Depends()],
    interval: Annotated[
        int,
        Query(
            description="Interval of data time series. Accept values in minutes: 1, 30, 60",
        ),
    ] = 60,
):
    # Early exit on wrong param
    if not commons.imo or interval not in [1, 6, 30, 60]:
        raise HTTPException(
            status_code=400,
            detail="Wrong parameter value.",
        )
    content = proc.get_speed_data(commons.imo, interval, commons.start_date, commons.end_date)
    # Demo fix for bulkset ship
    if commons.imo == 9425863:
        content = anonymize_bulkset("sog", commons.imo, content)
    return content


@router.get("/alerts", response_model=list[schema.Alert])
async def get_alerts(imo: Annotated[int, Query(gt=0)],):
    content = proc.get_alerts_data(imo)
    return content


@router.get("/links", response_model=schema.Links)
async def get_links_status(imo: Annotated[int, Query(gt=0)],):
    content = proc.get_links_data(imo)
    # Demo fix
    # content = anonymize_link(imo, content)
    if not content:
        raise HTTPException(
            status_code=404,
            detail="No data available",
        )
    return content

# TODO: Remove when switch to async
@router.get("/real-time-new", response_model=schema.VesselData)
async def get_real_time_new(imo: Annotated[int, Query(gt=0)],):
    content = proc.get_real_time_data_new(imo)
    return content

@router.get("/real-time-new-playback", response_model=schema.VesselDataPlayBack)
async def get_real_time_new(commons: Annotated[CommonParams, Depends()]):
    content = proc.get_real_time_data_new_playback(commons.imo,commons.start_date,commons.end_date)
    return content


@router_v1.get("/real-time", response_model=schema.VesselData)
async def get_real_time(imo: Annotated[int, Query(gt=0)],):
    content = await proc.get_real_time_data(imo)
    return content


@router.get("/utilization", response_model=schema.Utilization)
async def get_utilization(commons: Annotated[CommonParams, Depends()],):
    content = proc.get_utilization_data(commons.imo, commons.start_date, commons.end_date)
    return content


@router.get("/weather/resistance", response_model=schema.WeatherResistance)
async def get_resistance(imo: Annotated[int, Query(gt=0)],):
    content = proc.get_resistance_data(imo)
    if not content:
        raise HTTPException(
            status_code=404,
            detail="No data available",
        )
    return content


@router.get("/details", response_model=list[schema.EngineDetails])
async def get_engine_info(imo: Annotated[int, Query(gt=0)],):
    content = proc.get_engine_details(imo)
    return content


@router.get("/consumption/histogram")
async def get_consumption_histogram(
    imo: Annotated[int, Query(gt=0)],
    start_date: Annotated[
        int,
        Query(
            description="Start date timstamp of the requested period",
            gt=0,
        ),
    ],
    end_date: Annotated[
        int,
        Query(
            description="End date timstamp of the requested period",
            gt=0,
        ),
    ],
):
    # Early exit on wrong param
    if not imo:
        raise HTTPException(
            status_code=400,
            detail="Wrong parameter value.",
        )
    content = proc.get_consumption_stw_intervals(imo, start_date, end_date)
    if not content:
        raise HTTPException(
            status_code=404,
            detail="No data available",
        )
    return content

@router.get("/propulsion/metrics", response_model=schema.PropulsionMetrics)
async def get_propulsion(commons: Annotated[CommonParams, Depends()],):
    content = proc.get_propulsion_metrics(commons.imo, commons.start_date, commons.end_date)
    return content

@router.get("/propulsion/metrics-daily", response_model=schema.PropulsionMetrics)
async def get_propulsion(commons: Annotated[CommonParams, Depends()],):
    content = proc.get_propulsion_metrics_daily(commons.imo, commons.start_date, commons.end_date)
    return content