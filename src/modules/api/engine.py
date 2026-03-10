from typing import Annotated
from fastapi import APIRouter, HTTPException, Query, Depends
from src.modules.api import CommonParams
import src.modules.schemas.engine as schema
import src.modules.utils.engine as proc


router = APIRouter()


def _parse_imo_query_value(imo_raw: str | int | None) -> int | None:
    if imo_raw is None:
        return None
    if isinstance(imo_raw, int):
        return imo_raw if imo_raw > 0 else None
    normalized = imo_raw.strip().lower()
    if normalized in {"", "null", "none", "undefined"}:
        return None
    try:
        imo = int(normalized)
    except ValueError:
        return None
    return imo if imo > 0 else None


@router.get("/ae", response_model=schema.AUXResponse)
async def get_aux(
    commons: Annotated[CommonParams, Depends()],
    interval: Annotated[
        int,
        Query(
            description="Interval of data time series. Accept values in minutes: 1, 30, 60",
        ),
    ] = 60,
):
    # Early exit on wrong param
    if interval not in [1, 6, 30, 60]:
        raise HTTPException(
            status_code=400,
            detail="Wrong parameter value.",
        )
    content = proc.get_aux_data(commons.imo, interval, commons.start_date, commons.end_date)
    return content


@router.get("/me", response_model=schema.MEResponse)
async def get_main(
    commons: Annotated[CommonParams, Depends()],
    interval: Annotated[
        int,
        Query(
            description="Interval of data time series. Accept values in minutes: 1, 30, 60",
        ),
    ] = 60,
):
    # Early exit on wrong param
    if interval not in [1, 6, 30, 60]:
        raise HTTPException(
            status_code=400,
            detail="Wrong parameter value.",
        )
    content = proc.get_main_data(commons.imo, interval, commons.start_date, commons.end_date)
    return content


@router.get("/real-time", response_model=schema.MERTData | schema.AERTData)
async def get_engine_rt(
    engine: Annotated[
        schema.EngineName,
        Query(description="Engine name, accepted values: ME1, ME2, ME3, AE1, AE2",),
    ],
    imo: Annotated[
        str | int | None,
        Query(description="IMO number. Accepts positive integer. Null-like values return empty payload."),
    ] = None,
):
    parsed_imo = _parse_imo_query_value(imo)
    if parsed_imo is None:
        if engine.startswith("AE"):
            return schema.AERTData()
        return schema.MERTData()
    content = proc.get_engine_rt_data(parsed_imo, engine)
    if not content and engine.startswith("AE"):
        content = schema.AERTData()
    if not content and engine.startswith("ME"):
        content = schema.MERTData()
    return content
@router.get("/real-time-playback", )
async def get_engine_rt(
    commons: Annotated[CommonParams, Depends()],
    engine: Annotated[
        schema.EngineName,
        Query(description="Engine name, accepted values: ME1, ME2, ME3, AE1, AE2",),
    ],
):
    content = proc.get_engine_rt_data_for_playback_by_engine(commons.imo,commons.start_date,commons.end_date, engine)
    if not content and engine.startswith("AE"):
        content = schema.AERTData()
    if not content and engine.startswith("ME"):
        content = schema.MERTData()
    return content


@router.get("/data", response_model=dict[str, list[dict[str, float | None]]])
async def get_engine_params(
    commons: Annotated[CommonParams, Depends()],
    engine: Annotated[
        schema.EngineName,
        Query(description="Engine name, accepted values: ME1, ME2, ME3, AE1, AE2",),
    ],
    params: Annotated[
        list[schema.ParamName],
        Query(
            description="List of engine parameters. One or more of: 'rpm', 'fot', 'lot', 'cons'",
        ),
    ],
):
    content = proc.get_engine_data(commons.imo, commons.start_date, commons.end_date, engine, params)
    return content or {}
