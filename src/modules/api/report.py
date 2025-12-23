from datetime import datetime, date, timedelta, timezone
from typing import Annotated
from fastapi import APIRouter, HTTPException, Query, Depends
from fastapi.responses import FileResponse
from src.modules.api import CommonParams
import src.modules.schemas.report as schema
import src.modules.utils.report as proc


router = APIRouter()


@router.get("/noon")
async def get_noon_report(
    imo: Annotated[int, Query(gt=0)],
    report_date: Annotated[
        date,
        Query(
            description="Reporting date",
            default_factory=(
                lambda: datetime.now(timezone.utc).date() - timedelta(days=1)
            ),
        ),
    ],
):
    output_path_name, file_name = proc.generate_noon_report(imo, report_date)
    if output_path_name is None:
        raise HTTPException(
            status_code=404,
            detail="No data available",
        )
    return FileResponse(output_path_name, filename=file_name)


@router.get("/unified", response_model=schema.UnifiedDatasetResponse)
async def get_dataset(
    commons: Annotated[CommonParams, Depends()],
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1),
):
    start_index = (page - 1) * page_size
    content = proc.get_unified_dataset(
        commons.imo, commons.start_date, commons.end_date, start_index, page_size
    )
    return content


@router.get("/download/unified")
async def get_file(commons: Annotated[CommonParams, Depends()]):
    csv_content = proc.get_unified_file(commons.imo, commons.start_date, commons.end_date)
    if csv_content is None:
        raise HTTPException(status_code=404, detail="No data available")
    if not isinstance(csv_content, str):
        raise HTTPException(status_code=400, detail="Error in generating csv content")
    return FileResponse(csv_content)


@router.get("/consumption/summary", response_model=list[schema.ConsumptionSummary])
async def get_cons_summary(commons: Annotated[CommonParams, Depends()]):
    content = proc.get_cons_summary_data(commons.imo, commons.start_date, commons.end_date)
    return content

@router.get("/client/summary", response_model=list[schema.ConsumptionSummary])
async def get_client_summary(commons: Annotated[CommonParams, Depends()]):
    content = proc.get_client_summary_data(commons.imo, commons.start_date, commons.end_date)
    return content
