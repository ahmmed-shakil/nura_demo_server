import time
import traceback
from typing import Annotated, Type, Optional
from urllib.parse import urlencode, parse_qs
from fastapi import FastAPI, Request, Depends, Query, HTTPException ,File,UploadFile
from fastapi.responses import JSONResponse,StreamingResponse
from fastapi.middleware import Middleware
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from src.modules import logger
from src.modules.config import settings
from src.modules.api import fleet, vessel, auth, reference, engine, report
from src.modules.database import db_init, test_db ,engines,SessionLocal
from src.modules.demo.vessels import vessel_mapping as demo_vessels
from src.modules.demo.vessels import anonymize_dataset as demo_anon_dataset
from src.modules.utils.utils import store_images,get_image_capture
import src.modules.model  as models
from sqlalchemy.orm import Session
from pydantic import BaseModel
import os
import uuid
# DB intialization
db_init(2)
# for files 
os.makedirs("files", exist_ok=True)
# API
app = FastAPI()
models.Base.metadata.create_all(bind=engines)

class ImageCaptureFromCCTVBase(BaseModel):
    device_serial : str
    image: Optional[str]

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

app.include_router(auth.router, prefix="/login", tags=["auth"])
app.include_router(
    vessel.router,
    prefix="/vessel",
    dependencies=[Depends(auth.validate_access_token)],
    tags=["vessel"],
)
app.include_router(
    vessel.router_v1,
    prefix="/vessel/v1",
    dependencies=[Depends(auth.validate_access_token)],
    tags=["vessel v1"],
)
app.include_router(
    engine.router,
    prefix="/vessel/engine",
    dependencies=[Depends(auth.validate_access_token)],
    tags=["engine"],
)
app.include_router(
    fleet.router,
    prefix="/fleet",
    dependencies=[Depends(auth.validate_access_token)],
    tags=["fleet"],
)
app.include_router(
    report.router,
    prefix="/report",
    dependencies=[Depends(auth.validate_access_token)],
    tags=["report"],
)
app.include_router(reference.router, tags=["extra"])


app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Authorization", "X-Refresh-Token"],
)
logger.info("Origins settings: %s, type: %s", settings.ORIGINS, type(settings.ORIGINS))
app.add_middleware(GZipMiddleware, minimum_size=5000, compresslevel=5)


@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    logger.info(f"Process-Time for {request.url.path} {process_time}")
    response.headers["X-Process-Time"] = str(process_time)
    return response


def get_app_middleware(my_app: FastAPI, middleware_class: Type) -> Optional[Middleware]:
    middleware_index = None
    for index, middleware in enumerate(my_app.user_middleware):
        if middleware.cls == middleware_class:
            middleware_index = index
    return (
        None if middleware_index is None else my_app.user_middleware[middleware_index]
    )


@app.exception_handler(Exception)
async def internal_exception_handler(request, exc):
    trace = traceback.format_exc()
    logger.error(f"{request.url.path} Internal Server Error \n {trace}.")
    response = JSONResponse("Internal Server Error", status_code=500)
    cors_middleware = get_app_middleware(
        my_app=request.app, middleware_class=CORSMiddleware
    )
    request_origin = request.headers.get("origin", "")
    cors_kwargs = cors_middleware.kwargs if cors_middleware else {}
    allow_origins = cors_kwargs.get("allow_origins", [])
    if "*" in allow_origins:
        response.headers["Access-Control-Allow-Origin"] = "*"
    elif request_origin in allow_origins:
        response.headers["Access-Control-Allow-Origin"] = request_origin
    return response


@app.middleware("http")
async def params_anonymisation(request: Request, call_next):
    # Swap incoming imo in order to load correct data
    incoming_params = parse_qs(request.url.query, keep_blank_values=True)
    # React to anonymous IMO (all query params are lists)
    if "imo" in incoming_params and incoming_params["imo"][0] in demo_vessels:
        incoming_params["imo"][0] = demo_vessels[incoming_params["imo"][0]]
    # Encode the query back while maintaining multiple values correctly.
    request.scope["query_string"] = urlencode(incoming_params, doseq=True).encode(
        "utf-8"
    )
    return await call_next(request)


@app.get("/health", tags=["health"])
def health_check_api() -> str:
    return "OK"


@app.get("/db_health", tags=["health"])
def health_check_db() -> str:
    result = test_db()
    if result:
        return "DB is available"
    else:
        return "DB is not available"


@app.get("/vessel_data", tags=["stats"])
async def get_aux(imo: Annotated[int, Query(gt=50)]) -> JSONResponse:
    headers = {"Access-Control-Allow-Origin": "*"}
    # Return the dataset even for invalid IMOs (no checks atm)
    data = demo_anon_dataset(imo)
    return JSONResponse(data, headers=headers)

@app.post("/save-device-picture",status_code=201)
async def createimagesfromcctv(imageinfo :ImageCaptureFromCCTVBase,db:Session = Depends(get_db)):
    try:   
        instance = models.ImageCaptureFromCCTV(device_serial=imageinfo.device_serial,image=imageinfo.image)
        db.add(instance)
        db.commit()
        db.refresh(instance)
    except:
        raise HTTPException(
            status_code=404,
            detail="Not Save",
        )
    return imageinfo
 

@app.get('/get-device-picture/{device_serial}')
async def getImagesByDeviceSerial(device_serial:str,db:Session = Depends(get_db)):
    results  = db.query(models.ImageCaptureFromCCTV).filter(models.ImageCaptureFromCCTV.device_serial == device_serial).order_by(models.ImageCaptureFromCCTV.create_datetime.desc()).limit(8).all()
    if not results:
        raise HTTPException(
            status_code=404,
            detail="No Images Found",
        )
    return results

@app.post('/upload-device-picture')
async def uploadImages(file:UploadFile = File(...)):
    file.filename = f"{uuid.uuid4()}.jpg"
    filename = await store_images(file)
    print(filename)
    if filename :
        return {"url": f"/files/{filename}"}
    else :
        raise HTTPException(
            status_code=404,
            detail="Not uploaded",
        )

@app.get('/files/{filename}')
async def get_image_capture_view(filename:str):
    file_stream = get_image_capture(filename)
    if file_stream is None:
        raise HTTPException(status_code=404,detail="No data available",)
    return StreamingResponse(file_stream, media_type="image/png")
    



