from typing import Annotated
from fastapi import APIRouter, HTTPException, Query
from src.modules.demo.vessels import anonymize_vessels_kpi, anonymize_vessels, anonymize_vessels_position
from src.modules.schemas.fleet import VesselInfo, VesselKPI, VesselPosition, VesselLinks
import src.modules.utils.fleet as proc


router = APIRouter()


@router.get("/kpis", response_model=list[VesselKPI])
async def get_vessels_kpi(customer: Annotated[str, Query(max_length=50)]):
    # Demo company (hardcode until normal demo data available)
    vessel_owner = customer if (customer != "democompany") else "suryanautika"
    content = proc.get_vessels_kpi_data(vessel_owner)
    if not content:
        raise HTTPException(
            status_code=404,
            detail="No data available",
        )
    # Anonymize IMOs
    if customer == "democompany":
        content = anonymize_vessels_kpi(content)
    return content


@router.get("/vessels", response_model=list[VesselInfo])
async def get_vessels(customer: Annotated[str, Query(max_length=50)]):
    # Demo company (hardcode until normal demo data available)
    vessel_owner = customer if (customer != "democompany1") else "suryanautika"
    content = proc.get_vessels_data(vessel_owner)
    if not content:
        raise HTTPException(
            status_code=404,
            detail="No data available",
        )
    # Anonymize vessels if democustomer
    if customer == "democompany1" :
        content = anonymize_vessels(content)
    return content


@router.get("/position", response_model=list[VesselPosition])
async def get_position(customer: Annotated[str, Query(max_length=50)],):
    # Demo company (hardcode until normal demo data available)
    vessel_owner = customer if (customer != "democompany1") else "suryanautika"
    content = proc.get_latest_position_data(vessel_owner)

    # Anonymize vessels if democustomer
    if customer == "democompany1" :
        content = anonymize_vessels_position(content)
    return content


@router.get("/links", response_model=list[VesselLinks])
async def get_links(customer: Annotated[str, Query(max_length=50)],):
    return proc.get_fleet_links_data(customer)
