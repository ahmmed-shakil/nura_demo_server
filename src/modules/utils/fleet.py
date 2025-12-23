import pandas as pd
from numpy import nan
from sqlalchemy import text
from src.modules import Database, logger
from src.modules.utils.utils import query_data
import src.modules.schemas.fleet as schema
from src.modules.utils.vessel import (
    get_position_data,
    get_links_data,
    get_ais_data,
    get_latest_status,
    get_weather_data,
    get_speed_data,
)


def get_vessels_kpi_data(customer: str = "") -> list[schema.VesselKPI]:
    content = []
    vessels_query = text(
        """select v.name, v.imo, v.mmsi
from core.customer c
left join core.vessel v on v.customer_id = c.id
where c.name = :customer"""
    )
    try:
        engine = Database.get_engine()
        with engine.connect() as connection:
            stmt = connection.execute(vessels_query, {"customer": customer})
            vessels = stmt.fetchall()
    except Exception as e:
        logger.error("DB connection error while query vlist of vessels by customer. Error: %s", e)
        raise e
    if not vessels:
        return content
    for vessel in vessels:
        # See if we can get data based on imo or mmsi for given vessel
        id_type = "imo" if vessel[1] else "mmsi"
        id_number = vessel[1] if vessel[1] else vessel[2]
        vessel_data = schema.VesselKPI.model_validate(vessel)
        vessel_data.speed = get_speed_data(vessel[1], latest=True)[0]
        vessel_data.weather = get_weather_data(vessel[1])
        vessel_data.links = get_links_data(vessel[1])
        vessel_data.ais_position = get_ais_data(
            id_number=id_number, latest=True, id_type=id_type
        )[0]
        vessel_data.position_status = get_latest_status(vessel[1])
        vessel_data.position = get_position_data(vessel[1], latest=True)[0]
        content.append(vessel_data)
    return content


def get_vessels_data(customer: str) -> list[schema.VesselInfo]:
    vessels_query = text(
        """
select
v.name, c.name as company_id, v.imo, v.flag, vt.name as type, vt.family,
v.image as image_url, v.dwt, v.service_speed, v.additional_info
from core.customer c
inner join core.vessel v on v.customer_id = c.id
inner join core.vessel_type vt on vt.id = v.vessel_type_id
where c.name = :customer
"""
    )
    try:
        engine = Database.get_engine()
        with engine.connect() as connection:
            stmt = connection.execute(vessels_query, {"customer": customer})
            vessels = stmt.fetchall()
    except Exception as e:
        logger.error("DB connection error while query vessels data by customer. Error: %s", e)
        raise e
    logger.debug("Vessels data: %s", vessels)
    content = [schema.VesselInfo.model_validate(vessel) for vessel in vessels]
    logger.debug("Content: %s", content)
    return content


def get_latest_position_data(customer: str) -> list[schema.VesselPosition]:
    logger.info("Getting latest position data")
    # Get data from DB
    position_sql = text(
        """
with vs as(SELECT v.id, ft.name as fleet_type, v.imo, v.mmsi, v.name from core.vessel v
inner join core.fleet_type ft on v.fleet_type_id =ft.id
inner join core.customer c on c.id = v.customer_id
where :customer is null or c.name = :customer),
t as(
SELECT 'internal' as source, vs.id, max(ud.nr_time) as dt from core.interpolated_data ud
inner join vs on vs.id = ud.vessel_id
where ud.latitude is not null and ud.longitude is not null group by vs.id
union
SELECT 'ais' as source, vs.id, max(ard.report_datetime) as dt from core.ais_report_data ard
inner join vs on vs.id = ard.vessel_id
where ard.latitude is not null and ard.longitude is not null group by vs.id)

select vs.id as vessel_id, vs.fleet_type, vs.imo, vs.mmsi, vs.name, coalesce(t.dt, '1970-01-01') as timestamp, t.source,
case when t.source = 'internal' then coalesce(ud.latitude, 0) when t.source = 'ais' then ard.latitude end as lat,
case when t.source = 'internal' then coalesce(ud.longitude, 0) when t.source = 'ais' then ard.longitude end as long,
case when t.source = 'internal' then coalesce(ud.course, 0) when t.source = 'ais' then ard.course end as direction,
coalesce(ps.name, 'None') as position_status
from vs left outer join t on t.id = vs.id
left outer join core.interpolated_data ud on t.id = ud.vessel_id and t.dt = ud.nr_time
left outer join core.ais_report_data ard on t.id = ard.vessel_id and t.dt = ard.report_datetime
left outer join core.vessel_position_status vps on t.id = vps.vessel_id and t.dt = vps.report_datetime
left outer join core.position_status ps on ps.id = vps.status
order by vessel_id, source desc
"""
    )
    position = query_data(position_sql, {"customer": customer,})
    if not position:
        logger.info("No vessel geolocation data found")
        return [schema.VesselPosition()]
    content = []
    vessel_position = schema.VesselPosition.model_validate(position[0])
    for row in position:
        if vessel_position.vessel_id != row.vessel_id:
            content.append(vessel_position)
            vessel_position = schema.VesselPosition.model_validate(row)
        if row.source == "internal":
            vessel_position.position = schema.Position.model_validate(row, from_attributes=True)
        elif row.source == "ais":
            vessel_position.ais_position = schema.Position.model_validate(row, from_attributes=True)
    content.append(vessel_position)
    # logger.info("%d vesselposition position data: %s", len(content), content)
    return content


def get_fleet_links_data(customer: str) -> list[schema.VesselLinks]:
    logger.info("Getting fleet links data")
    links_sql = text(
        """
with latest_dates as
(select d.vessel_id, d.device_type_id,  e.type as label, s.latest_timestamp
from core.device d
inner join core.vessel v on v.id = d.vessel_id
inner join core.customer c on c.id = v.customer_id
left outer join core.engine e on e.id = d.engine_id
left outer join core.device_status s on d.device_id = s.device_id
where (c.name = :customer or :customer is null)
    and d.is_active = true and d.device_type_id in (1, 2, 3, 4, 6, 7))

select vessel_id,
    case
        when device_type_id in (1, 2, 4, 6, 7) then lower(label)
        when device_type_id = 3 then 'nmea'
    end as label,
    coalesce (latest_timestamp, '1970-01-01') as dt
from latest_dates
union all
select vessel_id, 'canbus' as label, coalesce (max(latest_timestamp), '1970-01-01') as dt
from latest_dates where device_type_id in(1, 4, 6) group by vessel_id
union all
select vessel_id, 'maic' as label, coalesce (max(latest_timestamp), '1970-01-01') as dt
from latest_dates where device_type_id in(2, 7) group by vessel_id
union all
select vessel_id, 'online' as label, coalesce (max(latest_timestamp), '1970-01-01') as dt
from latest_dates group by vessel_id;
"""
    )
    try:
        engine = Database.get_engine()
        links = pd.read_sql_query(
            links_sql,
            engine,
            params={"customer": customer},
        )
    except Exception as e:
        logger.error("DB connection error when getting links data. Error: %s", e)
        raise e
    if links.empty:
        # Generate empty link info for each avalaible vessel.
        logger.info("No vessel links data found for customer %s, generating empty data", customer)
        links_sql = text(
        """
with d (label, dt) as
(values ('nmea', '1970-01-01'),
('canbus', '1970-01-01'),
('online', '1970-01-01'),
('maic', '1970-01-01'))

select v.id as vessel_id, d.label, d.dt
from core.vessel v
inner join core.customer c on c.id = v.customer_id
cross join d
where (c.name = :customer or :customer is null) and v.is_active = True order by vessel_id ASC
"""
        )
        links = pd.read_sql_query(
            links_sql,
            engine,
            params={"customer": customer},
        )
        if links.empty:
            logger.info("No active vessels found for customer %s.", customer)
            return [schema.VesselLinks()]

    links["dt"] = (
        links["dt"].astype("datetime64[ns]").astype("int64") // 1000000000
    )
    links = links.drop_duplicates(subset=["vessel_id", "label"], keep="last")
    pivot_df = links.pivot(index="vessel_id", columns="label", values="dt")
    pivot_df = pivot_df.reset_index().replace(nan, 0)
    links = pivot_df.to_dict(orient="records")
    # logger.debug("Links data: %s", pivot_df)
    # logger.debug("Links data: %s", links)
    content = [schema.VesselLinks.model_validate(row) for row in links]
    logger.debug("%d vessellinks links data", len(content))
    return content
