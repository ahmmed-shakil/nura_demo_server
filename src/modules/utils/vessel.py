import asyncio
from datetime import datetime, timezone
import pandas as pd
from sqlalchemy import text
from src.modules import Database, logger
from src.modules.func.OperationKPIs import added_resis
import src.modules.schemas.vessel as schema
from src.modules.utils.engine import get_engine_rt_data, get_async_engine_rt_data,get_engine_rt_data_for_playback
from src.modules.utils.utils import query_data, query_data_async, query_df_async


def get_position_data(
    imo: int, start_date: int = 0, end_date: int = 0, latest: bool = False
) -> list[schema.Position]:
    logger.info("Getting position data")
    # Get data from DB
    position_sql = text(
        f"""(select
ud.nr_time as timestamp, ud.latitude as lat, ud.longitude as long, ud.course as direction
from core.interpolated_data ud
inner join core.vessel v on v.id = ud.vessel_id
where v.imo = :imo and (ud.latitude is not null or ud.longitude is not null
    {') order by 1 desc limit 1' if latest
     else ' or ud.course is not null) and ud.nr_time between :start_date and :end_date'})
union
(select ud.nr_time as timestamp, null as lat, null as long, ud.course as direction
from core.interpolated_data ud
inner join core.vessel v on v.id = ud.vessel_id
where v.imo = :imo {'' if latest else 'and ud.nr_time < :start_date'} and ud.course is not null
order by 1 desc limit 1)
union
select '1970-01-01' as timestamp, null as lat, null as long, 0 as direction
order by 1 asc"""
    )
    try:
        engine = Database.get_engine()
        position = pd.read_sql_query(
            position_sql,
            engine,
            params={
                "imo": imo,
                "start_date": datetime.fromtimestamp(start_date, tz=timezone.utc),
                "end_date": datetime.fromtimestamp(end_date, tz=timezone.utc),
            },
        )
    except Exception as e:
        logger.error("DB connection error. Error: %s", e)
        raise e
    merged_df = pd.merge_asof(
        position.loc[position["lat"].notna() & position["long"].notna(), ["timestamp", "lat", "long"]],
        position.loc[position["direction"].notna(), ["timestamp", "direction"]],
        on="timestamp",
        direction="nearest",
    )
    if merged_df.empty:
        logger.info("No vessel geolocation data found")
        return [schema.Position()]
    logger.debug("Merged data, first 3 rows: %s", merged_df.head(3))
    points = merged_df.to_dict(orient="records")
    if len(points) == 0:
        return [schema.Position()]
    points = [schema.Position.model_validate(row) for row in points]
    logger.info("%d position data points found", len(points))
    return points


def get_alerts_data(imo):
    alerts = []
    # Data query
    sql_query = text(
        """
select
    alt.severity as type,
    v.name as vessel_name,
    a.alert_datetime as time,
    a.latitude as lat,
    a.longitude as long,
    case when d.label_name is not null
        then concat(alt.description, ' :: ', d.label_name)
        else alt.description end as text
from core.alert a
inner join core.vessel v on v.id = a.vessel_id
inner join core.alert_type alt on alt.id = a.alert_type_id
inner join core.device d on d.device_id = a.device_id
where v.imo = :imo
order by a.alert_datetime desc;
"""
    )
    results = query_data(sql_query, {"imo": imo})
    if not results:
        return [schema.Alert()]
    alerts = [schema.Alert.model_validate(row) for row in results]
    return alerts


def get_real_time_data_new(imo: int) -> schema.VesselData:
    logger.info("Getting vessel real time data")
    start = datetime.now()
    logger.debug("Time start %s", start)
    response = get_engine_rt_data(imo)
    logger.debug("Time vessel_data %s", datetime.now()-start)
    if not isinstance(response, schema.VesselData):
        response = schema.VesselData()
    response.position = get_position_data(imo, latest=True)[0]
    logger.debug("Time position %s", datetime.now()-start)
    response.ais_position = get_ais_data(imo, latest=True)[0]
    logger.debug("Time ais_position %s", datetime.now()-start)
    response.weather = get_weather_data(imo)
    logger.debug("Time weather %s", datetime.now()-start)
    response.status = get_latest_status(imo)
    logger.debug("Time status %s", datetime.now()-start)
    logger.debug("Time Stop %s", datetime.now()-start)
    return response

def get_real_time_data_new_playback(imo: int, start_date: int,end_date: int) -> schema.VesselDataPlayBack:
    logger.info("Getting vessel real time data")
    start = datetime.now()
    response = schema.VesselDataPlayBack()
    logger.debug("Time start %s", start)
    response.engine = get_engine_rt_data_for_playback(imo,start_date=start_date,end_date=end_date)
    logger.debug("Time vessel_data %s", datetime.now()-start)
    if not isinstance(response, schema.VesselDataPlayBack):
        response = schema.VesselDataPlayBack()
    response.position = get_position_data(imo, start_date=start_date,end_date=end_date)
    logger.debug("Time position %s", datetime.now()-start)
    response.ais_position = get_ais_data(imo, start_date=start_date,end_date=end_date)
    logger.debug("Time ais_position %s", datetime.now()-start)
    response.weather = get_weather_data_for_playback(imo,start_date=start_date,end_date=end_date)
    logger.debug("Time weather %s", datetime.now()-start)
    response.status = get_latest_status_for_playback(imo,start_date=start_date,end_date=end_date)
    logger.debug("Time status %s", datetime.now()-start)
    logger.debug("Time Stop %s", datetime.now()-start)
    return response


async def get_real_time_data(imo: int) -> schema.VesselData:
    logger.info("Getting async vessel real time data")
    start = datetime.now()
    logger.debug("Time start %s", start)
    vessel_data = asyncio.create_task(get_async_engine_rt_data(imo))
    position = asyncio.create_task(get_async_position_data(imo, latest=True))
    ais_position = asyncio.create_task(get_async_ais_data(imo, latest=True))
    weather = asyncio.create_task(get_async_weather_data(imo))
    status = asyncio.create_task(get_async_latest_status(imo))
    results = await asyncio.gather(vessel_data, position, ais_position, weather, status)
    logger.debug("Time res %s", datetime.now()-start)
    if isinstance(results[0], schema.VesselData):
        response = results[0]
    else:
        response = schema.VesselData()
    response.position = results[1][0]
    response.ais_position = results[2][0]
    response.weather = results[3]
    response.status = results[4]
    logger.debug("Time Stop %s", datetime.now()-start)
    return response


def get_speed_data(
    imo: int, interval: int = 1, start_date: int = 0, end_date: int = 0, latest: bool = False
) -> list[schema.Speed]:
    logger.info("Getting SOG and STW data")
    if latest:
        sog_query = text(
            f"""select
        date_bin(:interval, d.nr_time , TIMESTAMP '2023-01-01') as timestamp,
        avg(d.average_speed_gps) as sog,
        avg(d.speed_through_water) as stw
    from core.interpolated_data d
    inner join core.vessel v on v.id = d.vessel_id
    where v.imo = :imo and (d.average_speed_gps is not null or d.speed_through_water is not null)
    {"" if latest else "and (d.nr_time between :start_date and :end_date)"}
    group by v.imo, timestamp order by timestamp desc {"limit 1" if latest else ""}
    """
        )
    else:
        sog_query = text("""
with hourly_series as (
    select generate_series(
        TIMESTAMP :start_date,
        TIMESTAMP :end_date,
        interval :interval
    ) as timestamp
),
aggregated_data as (
    select
        date_bin(:interval, d.nr_time, TIMESTAMP '2023-01-01') as timestamp,
        avg(d.average_speed_gps) as sog,
        avg(d.speed_through_water) as stw
    from core.interpolated_data d
    inner join core.vessel v on v.id = d.vessel_id
    where v.imo = :imo
        and (d.average_speed_gps is not null or d.speed_through_water is not null)
        and (d.nr_time between :start_date and :end_date)
    group by date_bin(:interval, d.nr_time, TIMESTAMP '2023-01-01')
)
select
    hs.timestamp,
    coalesce(ad.sog, 0) as sog,
    coalesce(ad.stw, 0) as stw
from hourly_series hs
left join aggregated_data ad on hs.timestamp = ad.timestamp
order by hs.timestamp desc;

                         """)
    data = query_data(
        sog_query,
        {
            "imo": imo,
            "interval": str(interval) + " minutes",
            "start_date": datetime.fromtimestamp(start_date, tz=timezone.utc),
            "end_date": datetime.fromtimestamp(end_date, tz=timezone.utc),
        },
    )
    if not data:
        content = [schema.Speed()]
    else:
        try:
            content = [schema.Speed.model_validate(row) for row in data]
        except ValueError as e:
            logger.error("Speed data validation error. %s", e)
            raise e
    return content


def get_links_data(imo: int) -> schema.Links:
    # Get data from DB
    sql = text(
        """
with latest_dates as
(select
    d.device_type_id,
    e.type as label,
    s.latest_timestamp
from core.device d
inner join core.vessel v on v.id = d.vessel_id
left outer join core.engine e on e.id = d.engine_id
left outer join core.device_status s on d.device_id = s.device_id
where v.imo = :imo
    and d.is_active = true and d.device_type_id in (1, 2, 3, 4, 6, 7))

select
    case
        when device_type_id in (1, 2, 4, 6, 7) then label
        when device_type_id = 3 then 'nmea'
    end as label,
    coalesce (latest_timestamp, '1970-01-01') as dt
from latest_dates
union all
select
    case
        when device_type_id in (1, 4, 6) then 'canbus'
        when device_type_id in (2, 7) then 'maic'
        else '1'
    end as label,
    coalesce (max(latest_timestamp), '1970-01-01') as dt
from latest_dates
where device_type_id in(1, 2, 4, 6, 7)
group by device_type_id
union all
select 'online' as label, coalesce (max(latest_timestamp), '1970-01-01') as dt
from latest_dates
union all
select 'position' as label, coalesce (max(message_datetime), '1970-01-01') as dt from nmea_report_data nrd
inner join core.device d on d.device_id = nrd.device_id
inner join core.vessel v on v.id = d.vessel_id
where imo = :imo and (nrd.latitude is not null or nrd.longitude is not null)
union all
select 'speed' as label, coalesce (max(message_datetime), '1970-01-01') as dt from nmea_report_data nrd
inner join core.device d on d.device_id = nrd.device_id
inner join core.vessel v on v.id = d.vessel_id
where imo = :imo and nrd.speed_over_ground is not null;
"""
    )
    results = query_data(sql, {"imo": imo})
    if not results:
        return schema.Links()
    return schema.Links.model_validate(
        {row[0].lower(): row[1].timestamp() for row in results}
    )




# def get_links_data(imo: int) -> schema.Links:
#     # Get data from DB
#     sql = text(
#         """
# WITH latest_dates AS (
#     SELECT
#         d.device_type_id,
#         e.type AS label,
#         s.latest_timestamp AT TIME ZONE 'UTC' AS latest_timestamp -- Normalize to UTC
#     FROM core.device d
#     INNER JOIN core.vessel v ON v.id = d.vessel_id
#     LEFT OUTER JOIN core.engine e ON e.id = d.engine_id
#     LEFT OUTER JOIN core.device_status s ON d.device_id = s.device_id
#     WHERE v.imo = :imo
#       AND d.is_active = TRUE
#       AND d.device_type_id IN (1, 2, 3, 4, 6, 7)
# )
# SELECT
#     CASE
#         WHEN device_type_id IN (1, 2, 4, 6, 7) THEN label
#         WHEN device_type_id = 3 THEN 'nmea'
#     END AS label,
#     COALESCE(latest_timestamp, '1970-01-01'::timestamp AT TIME ZONE 'UTC') AS dt -- Ensure default is UTC
# FROM latest_dates
# UNION ALL
# SELECT
#     CASE
#         WHEN device_type_id IN (1, 4, 6) THEN 'canbus'
#         WHEN device_type_id IN (2, 7) THEN 'maic'
#         ELSE '1'
#     END AS label,
#     COALESCE(MAX(latest_timestamp), '1970-01-01'::timestamp AT TIME ZONE 'UTC') AS dt
# FROM latest_dates
# WHERE device_type_id IN (1, 2, 4, 6, 7)
# GROUP BY device_type_id
# UNION ALL
# SELECT 'online' AS label, COALESCE(MAX(latest_timestamp), '1970-01-01'::timestamp AT TIME ZONE 'UTC') AS dt
# FROM latest_dates
# UNION ALL
# SELECT 'position' AS label, COALESCE(MAX(message_datetime AT TIME ZONE 'UTC'), '1970-01-01'::timestamp AT TIME ZONE 'UTC') AS dt
# FROM nmea_report_data nrd
# INNER JOIN core.device d ON d.device_id = nrd.device_id
# INNER JOIN core.vessel v ON v.id = d.vessel_id
# WHERE imo = :imo AND (nrd.latitude IS NOT NULL OR nrd.longitude IS NOT NULL)
# UNION ALL
# SELECT 'speed' AS label, COALESCE(MAX(message_datetime AT TIME ZONE 'UTC'), '1970-01-01'::timestamp AT TIME ZONE 'UTC') AS dt
# FROM nmea_report_data nrd
# INNER JOIN core.device d ON d.device_id = nrd.device_id
# INNER JOIN core.vessel v ON v.id = d.vessel_id
# WHERE imo = :imo AND nrd.speed_over_ground IS NOT NULL;
# """
#     )
#     results = query_data(sql, {"imo": imo})
#     if not results:
#         return schema.Links()
#     return schema.Links.model_validate(
#         {row[0].lower(): row[1].timestamp() for row in results}
#     )


def get_ais_data(
    id_number: int, start_date: int = 0, end_date: int = 0, latest: bool = False, id_type = 'imo'
) -> list[schema.Position]:

    logger.info("Getting AIS position data")
    # Get data from DB
    sql = text(
        f"""select
n.report_datetime as timestamp, n.latitude as lat, n.longitude as long, n.course as direction
from core.ais_report_data n
inner join core.vessel v on v.id = n.vessel_id
where v.{id_type} = :id_number {'' if latest else 'and n.report_datetime between :start_date and :end_date'}
	and n.latitude is not null and n.longitude is not null
order by n.report_datetime desc
{'limit 1' if latest else ''}"""
    )
    try:
        engine = Database.get_engine()
        result = pd.read_sql_query(
            sql,
            engine,
            params={
                "id_number": id_number,
                "start_date": datetime.fromtimestamp(start_date, tz=timezone.utc),
                "end_date": datetime.fromtimestamp(end_date, tz=timezone.utc),
            },
        )
    except Exception as err:
        logger.error("DB connection error. Error: %s", err)
        raise err
    logger.debug("%s position data points found", result.shape[0])
    if result.empty:
        return [schema.Position()]
    points = result.to_dict(orient="records")
    points = [schema.Position.model_validate(row) for row in points]
    return points


def get_utilization_data(
    imo: int, start_date: int, end_date: int
) -> schema.Utilization:
    logger.info("Getting utilization data")
    query = text(
        """
select ps.name, count(*) as number
from vessel_position_status vps
inner join position_status ps on ps.id = vps.status
inner join vessel v on v.id = vps.vessel_id
where vps.report_datetime between :start_date and :end_date and :imo = v.imo
group by ps.name
"""
    )
    data = query_data(
        query,
        {
            "imo": imo,
            "start_date": datetime.fromtimestamp(start_date, tz=timezone.utc),
            "end_date": datetime.fromtimestamp(end_date, tz=timezone.utc),
        },
    )
    if not data:
        return schema.Utilization()
    # data = {row[0]: row[1] for row in data}
    # Normalize the keys
    data = {row[0].replace(" ", "_"): row[1] for row in data}
    content = schema.Utilization.model_validate(data)
    total = content.port + content.anchorage + content.underway + content.awaiting_data + content.offshore_ops + content.offshore_stby + content.other
    if total:
        content.port = content.port * 100 / total
        content.anchorage = content.anchorage * 100 / total
        content.underway = content.underway * 100 / total
        content.awaiting_data = content.awaiting_data * 100 / total
        content.offshore_ops = content.offshore_ops * 100 / total
        content.offshore_stby = content.offshore_stby * 100 / total
        content.other = content.other * 100 / total
    logger.info("Utilization content: %s", content)
    return content


def get_latest_status(imo: int) -> schema.PositionStatus:
    logger.info("Getting latest vessel position status info")
    query = text(
        """
select ps.name as status_name, ps.description as status_description,
vps.report_datetime as status_timestamp, vps.distance, gp.name as gis_point_name,
gp.description as gis_point_description, gpt.name as gis_point_type
from vessel_position_status vps
inner join position_status ps on ps.id = vps.status
inner join vessel v on v.id = vps.vessel_id
inner join gis_point gp on gp.id = vps.gis_point_id
inner join gis_point_type gpt on gpt.id = gp.gis_point_type_id
where v.imo = :imo and ps.name <> 'awaiting_data'
order by vps.report_datetime DESC
limit 1
"""
    )
    data = query_data(query, {"imo": imo})
    if not data:
        content = schema.PositionStatus()
    else:
        content = schema.PositionStatus.model_validate(data[0])
    return content
def get_latest_status_for_playback(imo: int,start_date: int, end_date: int) -> schema.PositionStatus:
    logger.info("Getting latest vessel position status info")
    query = text(
        """
select ps.name as status_name, ps.description as status_description,
vps.report_datetime as status_timestamp, vps.distance, gp.name as gis_point_name,
gp.description as gis_point_description, gpt.name as gis_point_type
from vessel_position_status vps
inner join position_status ps on ps.id = vps.status
inner join vessel v on v.id = vps.vessel_id
inner join gis_point gp on gp.id = vps.gis_point_id
inner join gis_point_type gpt on gpt.id = gp.gis_point_type_id
where v.imo = :imo and ps.name <> 'awaiting_data' and vps.report_datetime between :start_date and :end_date
order by vps.report_datetime DESC
"""
    )
    data = query_data(query, {
        "imo": imo,
        "start_date": datetime.fromtimestamp(start_date, tz=timezone.utc),
        "end_date": datetime.fromtimestamp(end_date, tz=timezone.utc),
        })
    if not data:
        content = []
    else:
        content = [schema.PositionStatus.model_validate(row)for row in data]
    return content


def get_weather_data(imo: int) -> schema.WeatherData:
    logger.info("Getting latest vessel position weather data")
    query = text(
        """
select wd.time as timestamp, wd.current_direction, wd.wave_direction,
wd.wind_direction, wd.beaufourt, wd.dss
from unified_data ud
left outer join weather_data wd on wd.time = date_trunc('hour', ud.nr_time)
	and wd.lat = round(ud.latitude, 1) and wd.long = round(ud.longitude, 1)
inner join vessel v on v.id = ud.vessel_id
where v.imo = :imo and wd.time is not null
order by ud.nr_time desc
limit 1
"""
    )
    data = query_data(query, {"imo": imo})
    if not data:
        content = schema.WeatherData()
    else:
        content = schema.WeatherData.model_validate(data[0])
    return content
def get_weather_data_for_playback(imo: int,start_date: int, end_date: int) -> schema.WeatherData:
    logger.info("Getting latest vessel position weather data")
    query = text(
        """
select wd.time as timestamp, wd.current_direction, wd.wave_direction,
wd.wind_direction, wd.beaufourt, wd.dss
from unified_data ud
left outer join weather_data wd on wd.time = date_trunc('hour', ud.nr_time)
	and wd.lat = round(ud.latitude, 1) and wd.long = round(ud.longitude, 1)
inner join vessel v on v.id = ud.vessel_id
where v.imo = :imo and wd.time is not null and ud.nr_time between :start_date and :end_date
order by ud.nr_time desc
"""
    )
    data = query_data(query, {
        "imo": imo,
        "start_date": datetime.fromtimestamp(start_date, tz=timezone.utc),
        "end_date": datetime.fromtimestamp(end_date, tz=timezone.utc),
        })
    if not data:
        content = []
    else:
        content = [schema.WeatherData.model_validate(row)for row in data]
    return content


def get_resistance_data(imo: int) -> schema.WeatherResistance | None:
    logger.info("Fetching resistance data for the vessel %s", imo)
    query = text(
        """
select d.nr_time as timestamp, d.course as vessel_heading, wd.wind_direction,
	wd.wind_speed * 1.9438 as wind_speed, wd.wave_direction, wd.wave_height,
	wd.current_direction, wd.current_speed  * 1.9438 as current_speed,
    wd.beaufourt, wd.dss
from core.interpolated_data d
inner join core.vessel v on v.id = d.vessel_id
inner join core.weather_data wd on wd.time = date_trunc('hour', d.nr_time)
	and wd.lat = round(d.latitude, 1) and wd.long = round(d.longitude, 1)
where v.imo = :imo
order by d.nr_time desc limit 1;
"""
    )
    data = query_data(query, {"imo": imo})
    if not data:
        return None
    data = data[0]
    content = schema.WeatherResistance.model_validate(data)
    if (
        data[9]
        and content.wave_direction
        and data[8]
        and content.wind_direction
        and content.vessel_heading
    ):
        content.added_resistance = added_resis(
            float(data[9]),
            content.wave_direction,
            float(data[8]),
            content.wind_direction,
            content.vessel_heading,
        )
    logger.debug("Resistance data: %s", content)
    return content


def get_engine_details(imo: int) -> list[schema.EngineDetails]:
    logger.info("Fetching engine details for the vessel %s", imo)
    query = text(
        """
select e.type, e.label, e.additional_info
from core.vessel v
inner join core.engine e on v.id = e.vessel_id
where v.imo = :imo
"""
    )
    data = query_data(query, {"imo": imo})
    if not data:
        return [schema.EngineDetails()]
    return [schema.EngineDetails.model_validate(row) for row in data]


def get_consumption_stw_intervals(imo: int, start_date: int, end_date: int) -> list[:] | None:
    logger.info("Loading consumption grouped by STW data for %s given period", imo)
    query = text(
        """
select count(d.nr_time) ,
	sum(coalesce (d.me1_consumption, 0) + coalesce(d.me2_consumption, 0) + coalesce(d.me3_consumption, 0))/60 as consumption,
	case
		when (d.average_speed_gps >= 0 and d.average_speed_gps <= 5) then 1
		when (d.average_speed_gps > 5 and d.average_speed_gps <= 7.5) then 2
		when (d.average_speed_gps > 7.5 and d.average_speed_gps <= 10) then 3
		when (d.average_speed_gps > 10 and d.average_speed_gps <= 12) then 4
		when (d.average_speed_gps > 12 and d.average_speed_gps <= 14) then 5
		when (d.average_speed_gps > 14 and d.average_speed_gps <= 16) then 6
		when (d.average_speed_gps > 16 and d.average_speed_gps <= 18) then 7
		when (d.average_speed_gps > 18 and d.average_speed_gps <= 20) then 8
		when (d.average_speed_gps > 20) then 9
	end as interval
from core.unified_data as d
inner join core.vessel v on d.vessel_id = v.id
where
	v.imo = :imo and d.average_speed_gps is not null
	and nr_time >= :start_date
	and nr_time <= :end_date
group by interval
order by 3
"""
    )
    data = query_data(query, {
        "imo": imo,
        "start_date": datetime.fromtimestamp(start_date, tz=timezone.utc),
        "end_date": datetime.fromtimestamp(end_date, tz=timezone.utc),
    })
    if not data:
        return None
    result = [{'count': row[0], 'consumption': row[1], 'interval': row[2]} for row in data]
    bin_name = {
        1: "0-5",
        2: "5-7.5",
        3: "7.5-10",
        4: "10-12",
        5: "12-14",
        6: "14-16",
        7: "16-18",
        8: "18-20",
        9: "20+"
    }
    for row in result:
        row['interval'] = bin_name[row['interval']]
    return result


async def get_async_latest_status(imo: int) -> schema.PositionStatus:
    logger.info("Getting latest vessel position status info")
    query = text(
        """
select ps.name as status_name, ps.description as status_description,
vps.report_datetime as status_timestamp, vps.distance, gp.name as gis_point_name,
gp.description as gis_point_description, gpt.name as gis_point_type
from vessel_position_status vps
inner join position_status ps on ps.id = vps.status
inner join vessel v on v.id = vps.vessel_id
inner join gis_point gp on gp.id = vps.gis_point_id
inner join gis_point_type gpt on gpt.id = gp.gis_point_type_id
where v.imo = :imo and ps.name <> 'awaiting_data'
order by vps.report_datetime DESC
limit 1
"""
    )
    data = await query_data_async(query, {"imo": imo})
    if not data:
        content = schema.PositionStatus()
    else:
        content = schema.PositionStatus.model_validate(data[0])
    return content


async def get_async_weather_data(imo: int) -> schema.WeatherData:
    logger.info("Getting latest vessel position weather data")
    query = text(
        """
select wd.time as timestamp, wd.current_direction, wd.wave_direction,
wd.wind_direction, wd.beaufourt, wd.dss
from unified_data ud
left outer join weather_data wd on wd.time = date_trunc('hour', ud.nr_time)
	and wd.lat = round(ud.latitude, 1) and wd.long = round(ud.longitude, 1)
inner join vessel v on v.id = ud.vessel_id
where v.imo = :imo and wd.time is not null
order by ud.nr_time desc
limit 1
"""
    )
    data = await query_data_async(query, {"imo": imo})
    if not data:
        content = schema.WeatherData()
    else:
        content = schema.WeatherData.model_validate(data[0])
    return content


async def get_async_position_data(
    imo: int, start_date: int = 0, end_date: int = 0, latest: bool = False
) -> list[schema.Position]:
    logger.info("Getting position data")
    # Get data from DB
    position_sql = text(
        f"""(select
ud.nr_time as timestamp, ud.latitude as lat, ud.longitude as long, ud.course as direction
from core.interpolated_data ud
inner join core.vessel v on v.id = ud.vessel_id
where v.imo = :imo and (ud.latitude is not null or ud.longitude is not null
    {') order by 1 desc limit 1' if latest
     else ' or ud.course is not null) and ud.nr_time between :start_date and :end_date'})
union
(select ud.nr_time as timestamp, null as lat, null as long, ud.course as direction
from core.interpolated_data ud
inner join core.vessel v on v.id = ud.vessel_id
where v.imo = :imo {'' if latest else 'and ud.nr_time < :start_date'} and ud.course is not null
order by 1 desc limit 1)
union
select '1970-01-01' as timestamp, null as lat, null as long, 0 as direction
order by 1 asc"""
    )
    position = await query_df_async(position_sql, {
                "imo": imo,
                "start_date": datetime.fromtimestamp(start_date, tz=timezone.utc),
                "end_date": datetime.fromtimestamp(end_date, tz=timezone.utc),
            })
    merged_df = pd.merge_asof(
        position.loc[position["lat"].notna(), ["timestamp", "lat", "long"]],
        position.loc[position["direction"].notna(), ["timestamp", "direction"]],
        on="timestamp",
        direction="nearest",
    )
    if merged_df.empty:
        logger.info("No vessel geolocation data found")
        return [schema.Position()]
    logger.debug("Merged data, first 3 rows: %s", merged_df.head(3))
    points = merged_df.to_dict(orient="records")
    if len(points) == 0:
        return [schema.Position()]
    points = [schema.Position.model_validate(row) for row in points]
    logger.info("%d position data points found", len(points))
    return points


async def get_async_ais_data(
    id_number: int, start_date: int = 0, end_date: int = 0, latest: bool = False, id_type = 'imo'
) -> list[schema.Position]:

    logger.info("Getting AIS position data")
    # Get data from DB
    sql = text(
        f"""select
n.report_datetime as timestamp, n.latitude as lat, n.longitude as long, n.course as direction
from core.ais_report_data n
inner join core.vessel v on v.id = n.vessel_id
where v.{id_type} = :id_number {'' if latest else 'and n.report_datetime between :start_date and :end_date'}
	and n.latitude is not null and n.longitude is not null
order by n.report_datetime desc
{'limit 1' if latest else ''}"""
    )
    result = await query_df_async(sql, {
                "id_number": id_number,
                "start_date": datetime.fromtimestamp(start_date, tz=timezone.utc),
                "end_date": datetime.fromtimestamp(end_date, tz=timezone.utc),
            })
    logger.debug("%s position data points found", result.shape[0])
    if result.empty:
        return [schema.Position()]
    points = result.to_dict(orient="records")
    points = [schema.Position.model_validate(row) for row in points]
    return points


def get_propulsion_metrics(imo: int, start_date: int, end_date: int) -> schema.PropulsionMetrics:
    logger.info("Getting running hours for propulsion metrics")
    query = text(
        """
with t as
(select nr_time, me1_run_hours, me2_run_hours, me3_run_hours from core.unified_data ud
inner join core.vessel v on v.id = ud.vessel_id
where v.imo = :imo and nr_time between :start_date and :end_date)

(select 'me1_start', me1_run_hours from t
where me1_run_hours is not null order by nr_time limit 1)
union all
(select 'me1_end', me1_run_hours from t
where me1_run_hours is not null order by nr_time desc limit 1)
union all
(select 'me2_start', me2_run_hours from t
where me2_run_hours is not null order by nr_time limit 1)
union all
(select 'me2_end', me2_run_hours from t
where me2_run_hours is not null order by nr_time desc limit 1)
union all
(select 'me3_start', me3_run_hours from t
where me3_run_hours is not null order by nr_time limit 1)
union all
(select 'me3_end', me3_run_hours from t
where me3_run_hours is not null order by nr_time desc limit 1)
"""
    )
    data = query_data(query,
                      {
                          "imo": imo,
                          "start_date": datetime.fromtimestamp(start_date, tz=timezone.utc),
                          "end_date": datetime.fromtimestamp(end_date, tz=timezone.utc),
                    })
    content = schema.PropulsionMetrics()
    if data:
        data = {row[0]: row[1] for row in data}
        run_hours = schema.RunHours.model_validate(data)
        logger.debug("Propulsion metrics data: %s", data)
        content.me1_run_hours = round((run_hours.me1_end - run_hours.me1_start), 2)
        content.me2_run_hours = round((run_hours.me2_end - run_hours.me2_start), 2)
        content.me3_run_hours = round((run_hours.me3_end - run_hours.me3_start), 2)
    return content


def get_propulsion_metrics_daily(imo: int, start_date: int, end_date: int) -> schema.PropulsionMetrics:
    logger.info("Getting running hours for propulsion metrics daily")
    query = text(
        """
with t as
(select nr_time, me1_run_hours, me2_run_hours, me3_run_hours from core.unified_data ud
inner join core.vessel v on v.id = ud.vessel_id
where v.imo = :imo and nr_time between :start_date and :end_date)


(select 'me1_end', me1_run_hours , nr_time from t
where me1_run_hours is not null order by nr_time desc limit 1)
union all

(select 'me2_end', me2_run_hours , nr_time from t
where me2_run_hours is not null order by nr_time desc limit 1)
union all

(select 'me3_end', me3_run_hours , nr_time from t
where me3_run_hours is not null order by nr_time desc limit 1)
"""
    )
    data = query_data(query,
                      {
                          "imo": imo,
                          "start_date": datetime.fromtimestamp(start_date, tz=timezone.utc),
                          "end_date": datetime.fromtimestamp(end_date, tz=timezone.utc),
                    })
    content = schema.PropulsionMetrics()
    if data:
        for row in data:
            engine_key = row[0] 
            run_hours_value = row[1]
            timestamp_value = row[2].timestamp() if row[2] else None

            engine_name = engine_key.replace('_end', '')

            if run_hours_value is not None:
                setattr(content, f'{engine_name}_run_hours', round(run_hours_value, 2))
                setattr(content, f'{engine_name}_run_hours_timestamp', timestamp_value)
    return content
