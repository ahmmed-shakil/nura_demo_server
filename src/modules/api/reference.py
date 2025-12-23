from typing import Annotated
from fastapi import APIRouter, Query, HTTPException
from fastapi.responses import FileResponse, StreamingResponse
from sqlalchemy import text
from src.modules import logger
from src.modules.schemas.reference import GisPoint
from src.modules.utils.utils import query_data, get_media_file


router = APIRouter()


@router.get("/reference/map/points", response_model=list[GisPoint])
async def get_gis_points(
    gis_point_type: Annotated[list[str] | None, Query()] = None,
):

    logger.info("Getting ports data")
    content = []
    gis_point_query = text(
        """
select gpt.name as gis_point_type, gp.name as gis_point_name,
gp.description as gis_point_description, gp.lat, gp.long
from gis_point gp
inner join gis_point_type gpt on gpt.id = gp.gis_point_type_id
where (gpt.name = any (:gis_point_type) or :gis_point_type is null)
order by gis_point_type_id
"""
    )
    gis_points = query_data(gis_point_query, {"gis_point_type": gis_point_type})
    if gis_points:
        logger.info("%s GIS points found", len(gis_points))
        for gis_point in gis_points:
            content.append(GisPoint.model_validate(gis_point))
        logger.debug("Gis points content: %s", content)
    return content


@router.get("/media/{customer_name}/{entity}/{instance_id}/{file_name}")
async def get_media(
    customer_name: str,
    entity: str,
    instance_id: int,
    file_name: str
    # TODO: Add autorization to get media
    #token_vessel_id: Annotated[int, Depends(validate_access_token)]
    ):
    # if vessel_id != token_vessel_id:
        # logger.error("User is not alowed to get vessel_id=%d data", vessel_id)
        # raise HTTPException(status_code=401, detail="Access denied to the requested vessel")
    aws_file_name = f"{customer_name}/{entity}/{instance_id}/{file_name}"
    file_stream = get_media_file(aws_file_name)
    if file_stream is None:
        raise HTTPException(status_code=404,detail="No data available",)
    # return FileResponse(path, media_type="image/png")
    return StreamingResponse(file_stream, media_type="image/png")
