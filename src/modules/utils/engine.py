from datetime import datetime, timezone, timedelta
import pandas as pd
from numpy import nan
from sqlalchemy import text
from src.modules import Database, logger
import src.modules.schemas.engine as schema
from src.modules.schemas.vessel import VesselData
from src.modules.utils.utils import query_data, query_df_async

def get_aux_data(imo: int, interval: int, start_date: int, end_date: int):
    data_sql = text(
        """
with hourly_series as (
    select generate_series(
        TIMESTAMP :start_date,
        TIMESTAMP :end_date,
        interval :interval
    ) as dt
),
aggregated_data as (
    select
        date_bin(:interval, u.nr_time, TIMESTAMP '2023-01-01') as dt,
        sum(coalesce(u.ae1_energy_produced, 0)) as ae1_energy,
        sum(coalesce(u.ae1_consumption, 0)) as ae1_cons,
        sum(coalesce(u.ae2_energy_produced, 0)) as ae2_energy,
        sum(coalesce(u.ae2_consumption, 0)) as ae2_cons,
        sum(coalesce(u.ae3_energy_produced, 0)) as ae3_energy,
        sum(coalesce(u.ae3_consumption, 0)) as ae3_cons,
        sum(coalesce(u.ae1_energy_produced, 0) + coalesce(u.ae2_energy_produced, 0) + coalesce(u.ae3_energy_produced, 0)) as total_energy,
        sum(coalesce(u.ae1_consumption, 0) + coalesce(u.ae2_consumption, 0) + coalesce(u.ae3_consumption, 0)) as total_cons
    from core.interpolated_data u
    inner join core.vessel v on u.vessel_id = v.id
    where
        v.imo = :imo
        and (u.ae1_consumption is not null or u.ae2_consumption is not null or u.ae3_consumption is not null)
        and u.nr_time between :start_date and :end_date
    group by date_bin(:interval, u.nr_time, TIMESTAMP '2023-01-01')
)
select
    hs.dt,
    coalesce(ad.ae1_energy, 0) as ae1_energy,
    coalesce(ad.ae1_cons, 0) as ae1_cons,
    coalesce(ad.ae2_energy, 0) as ae2_energy,
    coalesce(ad.ae2_cons, 0) as ae2_cons,
    coalesce(ad.ae3_energy, 0) as ae3_energy,
    coalesce(ad.ae3_cons, 0) as ae3_cons,
    coalesce(ad.total_energy, 0) as total_energy,
    coalesce(ad.total_cons, 0) as total_cons
from hourly_series hs
left join aggregated_data ad on hs.dt = ad.dt
order by hs.dt;

"""
    )
    # Get data from DB
    result = query_data(
        data_sql,
        {
            "imo": imo,
            "interval": str(interval) + " minutes",
            "start_date": datetime.fromtimestamp(start_date, tz=timezone.utc),
            "end_date": datetime.fromtimestamp(end_date, tz=timezone.utc),
        },
    )
    # Generate data for response
    if not result:
        logger.info("No AE data found for requested time range")
        return schema.AUXResponse()
    logger.debug("Fetched AE data: %s", result)
    ae = {}
    # Refactor this for a variable number of AE (currently 3 is max)
    for i in range(0, 4):
        ae[i] = [
            schema.AEData(
                timestamp=row[0] + timedelta(minutes=interval / 2),
                fuel_cons=float(row[i * 2 + 2]) / interval,
                power=float(row[i * 2 + 1]) / interval,
            )
            for row in result
        ]
    content = schema.AUXResponse(
        ae1=ae[0],
        ae2=ae[1],
        ae3=ae[2],
        total=ae[3],
    )
    logger.debug("AE response data: %s", content)
    return content


def get_main_data(imo: int, interval: int, start_date: int, end_date: int):
    data_sql = text(
            """
with hourly_series as (
    select generate_series(
        TIMESTAMP :start_date,
        TIMESTAMP :end_date,
        interval :interval
    ) as dt
),
aggregated_data as (
    select
        date_bin(:interval, u.nr_time, TIMESTAMP '2023-01-01') as dt,
        sum(coalesce(u.me1_consumption, 0)) as me1_cons,
        sum(coalesce(u.me2_consumption, 0)) as me2_cons,
        sum(coalesce(u.me3_consumption, 0)) as me3_cons,
        sum(coalesce(u.me1_consumption, 0) + coalesce(u.me2_consumption, 0) + coalesce(u.me3_consumption, 0)) as total_cons
    from core.interpolated_data u
    inner join core.vessel v on u.vessel_id = v.id
    where
        v.imo = :imo
        and (u.me1_consumption is not null or u.me2_consumption is not null or u.me3_consumption is not null)
        and u.nr_time between :start_date and :end_date
    group by date_bin(:interval, u.nr_time, TIMESTAMP '2023-01-01')
)
select
    hs.dt,
    coalesce(ad.me1_cons, 0) as me1_cons,
    coalesce(ad.me2_cons, 0) as me2_cons,
    coalesce(ad.me3_cons, 0) as me3_cons,
    coalesce(ad.total_cons, 0) as total_cons
from hourly_series hs
left join aggregated_data ad on hs.dt = ad.dt
order by hs.dt;
  """
        )
    # Get data from DB
    result = query_data(
        data_sql,
        {
            "imo": imo,
            "interval": str(interval) + " minutes",
            "start_date": datetime.fromtimestamp(start_date, tz=timezone.utc),
            "end_date": datetime.fromtimestamp(end_date, tz=timezone.utc),
        },
    )
    # Generate data for response
    if not result:
        return schema.MEResponse()
    logger.debug("Fetched ME data: %s", result)
    me = {}
    for i in range(1, 5):
        me[i] = [
            schema.FuelConsumption(
                timestamp=row[0] + timedelta(minutes=interval / 2),
                fuel_cons=float(row[i]) / interval,
            )
            for row in result
        ]
    content = schema.MEResponse(
        me1=me[1],
        me2=me[2],
        me3=me[3],
        total=me[4],
    )
    logger.debug("ME response data: %s", content)
    return content


def get_engine_rt_data(imo: int, engine: str | None = None):
    logger.info("Getting engine real time data")
    # Setting delta to 15 minutes
    fresh_delta = "15 minutes"
    # Get data from DB
    sql = text(
        """select nr_time as timestamp, longitude as long, latitude as lat,
    course as direction, average_speed_gps as sog, speed_through_water as stw,
    --me1_run_hours as me1_running_hour, me2_run_hours as me2_running_hour, me3_run_hours as me3_running_hour,
    me1_consumption as me1_fuel_cons,
    me2_consumption as me2_fuel_cons,
    me3_consumption as me3_fuel_cons,
    ae1_consumption as ae1_fuel_cons, ae1_energy_produced as ae1_power,
    ae2_consumption as ae2_fuel_cons, ae2_energy_produced as ae2_power,
    ae3_consumption as ae3_fuel_cons, ae3_energy_produced as ae3_power
from interpolated_data d
inner join vessel v on v.id = d.vessel_id
where v.imo = :imo and nr_time > CURRENT_TIMESTAMP - interval :fresh_delta
order by nr_time desc"""
    )
    try:
        db_engine = Database.get_engine()
        results = pd.read_sql_query(
            sql,
            db_engine,
            params={"imo": imo, "fresh_delta": fresh_delta,},
        )
    except Exception as e:
        logger.error("DB connection error. Error: %s", e)
        raise e
    if results.empty:
        if engine is not None:
            if engine.lower().startswith("me"):
                
                engine_column = f"{engine.lower()}_run_hours"
                update_sql = text(
                f"""
                        SELECT d.{engine_column} as running_hour,
                        d.nr_time as running_hour_timestamp 
                        FROM core.unified_data as d
                        inner join core.vessel v on d.vessel_id = v.id
                        where v.imo = :imo AND  d.{engine_column} IS NOT NULL AND d.{engine_column} > 0
                        ORDER BY d.nr_time DESC LIMIT 1;
                """
               )
                try:
                    update_results = pd.read_sql_query(
                        update_sql,
                        db_engine,
                        params={"imo": imo, "label": engine},
                    )
                except Exception as e:
                    logger.error("DB connection error. Error: %s", e)
                    raise e
                result = {}
                result['running_hour'] = update_results.iloc[0]["running_hour"] if not update_results.empty else 0
                result['running_hours_timestamp'] = update_results.iloc[0]["running_hour_timestamp"].timestamp() if not update_results.empty else 0
                return schema.MERTData.model_validate(result)
        return None
    # Select the most complete data from fresh data period
    completeness = pd.DataFrame(
        [
            results[["me1_fuel_cons", "me2_fuel_cons", "me3_fuel_cons"]].any(axis=1),
            results[["ae1_fuel_cons", "ae2_fuel_cons", "ae3_fuel_cons"]].any(axis=1),
            results[["lat", "long", "sog", "direction"]].any(axis=1),
        ]
    ).sum(axis=0)
    logger.debug("Completetness: %s", completeness)
    idx = completeness.idxmax()
    if not isinstance(idx, int):
        return None
    result = results.iloc[idx].replace({nan: None}).to_dict()
    if engine:
        engine = engine.lower()
        engines = (engine,)
    else:
        engines = ("me", "ae")
    engines_data = {}
    for key in result.keys():
        if key.startswith(engines):
            prefix, suffix = key.split("_", 1)
            if prefix not in engines_data:
                engines_data[prefix] = {"timestamp": result["timestamp"]}
            engines_data[prefix][suffix] = result[key]
    add_sql = text(
        """
with t as (select * from canbus_report_data crd where message_datetime = :dt)

select d.label_name, t.me_rpm as rpm, t.me_engine_load as engine_load,
t.me_fuel_temp as fuel_temp, t.me_exhaust_gas_temp_left as exhaust_gas_temp_left,
t.me_exhaust_gas_temp_right as exhaust_gas_temp_right, e.running_hours as running_hour
from device d
inner join engine e on d.engine_id = e.id
inner join vessel v on v.id = d.vessel_id
left outer join t on d.device_id = t.device_id
where v.imo = :imo and d.device_type_id in (1,4,6) and d.is_active = True
and ((:label)::text is null or d.label_name = upper(:label)::text)"""
    )
    try:
        add_results = pd.read_sql_query(
            add_sql,
            db_engine,
            params={"imo": imo, "dt": result["timestamp"], "label": engine},
        )
    except Exception as e:
        logger.error("DB connection error. Error: %s", e)
        raise e
    if not add_results.empty:
        for i in range(0, add_results.shape[0]):
            lablel = str(add_results.at[i, "label_name"]).lower()
            if lablel not in engines_data:
                engines_data[lablel] = {"timestamp": result["timestamp"]}
            engines_data[lablel].update(add_results.iloc[i].replace({nan: None}).to_dict())
    result.update(engines_data)
    logger.debug("engine: %s", engine)
    logger.debug("Result: %s", result)
    if engine is None:
        logger.debug("engine: %s", engines_data)
        return VesselData.model_validate(result)
    elif engine.startswith("me"):
        if not result[engine]['running_hour'] or result[engine]['running_hour'] <= 0:
            engine_column = f"{engine.lower()}_run_hours"
            update_sql = text(
                f"""
                        SELECT d.{engine_column} as running_hour,
                        d.nr_time as running_hour_timestamp 
                        FROM core.unified_data as d
                        inner join core.vessel v on d.vessel_id = v.id
                        where v.imo = :imo AND  d.{engine_column} IS NOT NULL AND d.{engine_column} > 0
                        ORDER BY d.nr_time DESC LIMIT 1;
                """
        )
            try:
                update_results = pd.read_sql_query(
                    update_sql,
                    db_engine,
                    params={"imo": imo, "label": engine},
                )
            except Exception as e:
                logger.error("DB connection error. Error: %s", e)
                raise e
            result[engine]['running_hour'] = update_results.iloc[0]["running_hour"] if not update_results.empty else 0
            result[engine]['running_hours_timestamp'] = update_results.iloc[0]["running_hour_timestamp"].timestamp() if not update_results.empty else 0
        return schema.MERTData.model_validate(result[engine])
    elif engine.startswith("ae"):
        logger.debug("engine: %s", engine)
        return schema.AERTData.model_validate(result[engine])
def get_engine_rt_data_for_playback(imo: int,start_date: int,end_date: int, engine: str | None = None):
    logger.info("Getting engine real time data for playback")
    # Get data from DB
    if engine is not None:
        return None
    sql = text(
        """select nr_time as timestamp, longitude as long, latitude as lat,
    course as direction, average_speed_gps as sog, speed_through_water as stw,
    --me1_run_hours as me1_running_hour, me2_run_hours as me2_running_hour, me3_run_hours as me3_running_hour,
    me1_consumption as me1_fuel_cons,
    me2_consumption as me2_fuel_cons,
    me3_consumption as me3_fuel_cons,
    ae1_consumption as ae1_fuel_cons, ae1_energy_produced as ae1_power,
    ae2_consumption as ae2_fuel_cons, ae2_energy_produced as ae2_power,
    ae3_consumption as ae3_fuel_cons, ae3_energy_produced as ae3_power
from interpolated_data d
inner join vessel v on v.id = d.vessel_id
where v.imo = :imo and nr_time between :start_date and :end_date
order by nr_time desc  """
    )
    try:
        db_engine = Database.get_engine()
        results = pd.read_sql_query(
            sql,
            db_engine,
            params={
                "imo": imo, 
                "start_date": datetime.fromtimestamp(start_date, tz=timezone.utc),
                "end_date": datetime.fromtimestamp(end_date, tz=timezone.utc),
                    },
        )
    except Exception as e:
        logger.error("DB connection error. Error: %s", e)
        raise e
    if results.empty:
        return None
    content = []
    completeness = pd.DataFrame(
            [
                results[["me1_fuel_cons", "me2_fuel_cons", "me3_fuel_cons"]].any(axis=1),
                results[["ae1_fuel_cons", "ae2_fuel_cons", "ae3_fuel_cons"]].any(axis=1),
                results[["lat", "long", "sog", "direction"]].any(axis=1),
            ]
        ).sum(axis=0)
    idx = completeness.idxmax()
    if not isinstance(idx, int):
        return None
    results = results.replace({nan: None})
    for index,results_data in results.iterrows():
        content.append(results_data.to_dict())
   
    return content

def get_engine_rt_data_for_playback_by_engine(imo: int,start_date: int,end_date: int, engine: str | None = None):
    logger.info("Getting engine real time data for playback")
    print('its start')
    
    # Get data from DB
    sql = text(
        """select nr_time as timestamp, longitude as long, latitude as lat,
    course as direction, average_speed_gps as sog, speed_through_water as stw,
    --me1_run_hours as me1_running_hour, me2_run_hours as me2_running_hour, me3_run_hours as me3_running_hour,
    me1_consumption as me1_fuel_cons,
    me2_consumption as me2_fuel_cons,
    me3_consumption as me3_fuel_cons,
    ae1_consumption as ae1_fuel_cons, ae1_energy_produced as ae1_power,
    ae2_consumption as ae2_fuel_cons, ae2_energy_produced as ae2_power,
    ae3_consumption as ae3_fuel_cons, ae3_energy_produced as ae3_power
from interpolated_data d
inner join vessel v on v.id = d.vessel_id
where v.imo = :imo and nr_time between :start_date and :end_date
order by nr_time desc limit 10 """
    )
    try:
        
        db_engine = Database.get_engine()
        results = pd.read_sql_query(
            sql,
            db_engine,
            params={
                "imo": imo, 
                "start_date": datetime.fromtimestamp(start_date, tz=timezone.utc),
                "end_date": datetime.fromtimestamp(end_date, tz=timezone.utc),
                    },
        )
        
    except Exception as e:
        logger.error("DB connection error. Error: %s", e)
        raise e
    if results.empty:
        return []
    add_sql = text(
            """
    with t as (select * from canbus_report_data crd where message_datetime between :start_date and :end_date)

    select t.message_datetime as timestamp, d.label_name, t.me_rpm as rpm, t.me_engine_load as engine_load,
    t.me_fuel_temp as fuel_temp, t.me_exhaust_gas_temp_left as exhaust_gas_temp_left,
    t.me_exhaust_gas_temp_right as exhaust_gas_temp_right, e.running_hours as running_hour
    from device d
    inner join engine e on d.engine_id = e.id
    inner join vessel v on v.id = d.vessel_id
    left outer join t on d.device_id = t.device_id
    where v.imo = :imo and d.device_type_id in (1,4,6) and d.is_active = True
    and ((:label)::text is null or d.label_name = upper(:label)::text) limit 100 """
        )
    try:
        
            engine_results = pd.read_sql_query(
                add_sql,
                db_engine,
                params={"imo": imo, 
                        "start_date": datetime.fromtimestamp(start_date, tz=timezone.utc),
                "end_date": datetime.fromtimestamp(end_date, tz=timezone.utc),
                         "label": engine},
            )
           
    except Exception as e:
        logger.error("DB connection error. Error: %s", e)
        raise e

    if engine_results.empty:
        return []
    
    for idx, row in results.iterrows():
        # print(row)
        for res_idx, res_row in engine_results.iterrows():
            # print(res_row)
            # Ensure column for fuel consumption is added if not already present
            if f"{engine.lower()}_fuel_cons" not in res_row:
                engine_results[f"{engine.lower()}_fuel_cons"] = None
            
            if pd.to_datetime(row['timestamp']) == pd.to_datetime(res_row['timestamp']):
                print('Timestamp matches')
                value = row[f"{engine.lower()}_fuel_cons"]
                # Set the fuel_cons value in the engine_results dataframe
                engine_results.at[res_idx, f"{engine.lower()}_fuel_cons"] = value
                  # If you only need the first match, you can break out of the inner loop here

                
    
    return [row for _,row in engine_results.iterrows()]
   
    



def get_engine_data(
    imo: int,
    start_date: int,
    end_date: int,
    engine: schema.EngineName,
    params: list[schema.ParamName],
):
    logger.info(
        "Fetching %s engine params: %s",
        engine.value,
        ",".join([param.name for param in params]),
    )
    querry_params = {
        "imo": imo,
        "start_date": datetime.fromtimestamp(start_date, tz=timezone.utc),
        "end_date": datetime.fromtimestamp(end_date, tz=timezone.utc),
        "label_name": engine.value,
    }
    content = {}
    if (
        engine in [schema.EngineName.ME1, schema.EngineName.ME2, schema.EngineName.ME3]
        and schema.ParamName.me_fuel_cons in params
    ):
        sql = text(
            f"""select d.nr_time as timestamp, d.{engine.value.lower()}_consumption
from core.interpolated_data d
inner join core.vessel v on v.id = d.vessel_id
where v.imo = :imo and d.nr_time between :start_date and :end_date
order by d.nr_time desc"""
        )
        data = query_data(sql, querry_params)
        if data:
            content[schema.ParamName.me_fuel_cons.value] = [
                {"timestamp": row[0].timestamp(), "value": row[1]} for row in data
            ]
        params.remove(schema.ParamName.me_fuel_cons)
    if (
        engine in [schema.EngineName.ME1, schema.EngineName.ME2, schema.EngineName.ME3]
        and params
    ):
        sql = text(
            f"""select crd.message_datetime as timestamp,
{','.join([param.name for param in params])}
from core.canbus_report_data crd
inner join core.device d on crd.device_id = d.device_id
inner join core.vessel v on v.id = d.vessel_id
where v.imo = :imo and crd.message_datetime between :start_date and :end_date
order by crd.message_datetime desc"""
        )
        data = query_data(sql, querry_params)
        if data:
            for i, param in enumerate(params):
                content[param.value] = [
                    {"timestamp": row[0].timestamp(), "value": row[i + 1]}
                    for row in data
                ]
    logger.debug("Engine data: %s", content)
    return content


# TODO: Remove get_engine_rt_data when switch to async
async def get_async_engine_rt_data(imo: int, engine: str | None = None):
    logger.info("Getting engine real time data")
    # Setting delta to 15 minutes
    fresh_delta = 15
    # Get data from DB
    sql = text(
        """select nr_time as timestamp, longitude as long, latitude as lat,
    course as direction, average_speed_gps as sog, speed_through_water as stw,
    --me1_run_hours as me1_running_hour, me2_run_hours as me2_running_hour, me3_run_hours as me3_running_hour,
    me1_consumption as me1_fuel_cons,
    me2_consumption as me2_fuel_cons,
    me3_consumption as me3_fuel_cons,
    ae1_consumption as ae1_fuel_cons, ae1_energy_produced as ae1_power,
    ae2_consumption as ae2_fuel_cons, ae2_energy_produced as ae2_power,
    ae3_consumption as ae3_fuel_cons, ae3_energy_produced as ae3_power
from interpolated_data d
inner join vessel v on v.id = d.vessel_id
where v.imo = :imo and nr_time > CURRENT_TIMESTAMP - INTERVAL '1 minute' * :fresh_delta
order by nr_time desc;"""
    )
    results = await query_df_async(sql, dict(imo = int(imo), fresh_delta = fresh_delta))
    if results.empty:
        return None
    # Select the most complete data from fresh data period
    completeness = pd.DataFrame(
        [
            results[["me1_fuel_cons", "me2_fuel_cons", "me3_fuel_cons"]].any(axis=1),
            results[["ae1_fuel_cons", "ae2_fuel_cons", "ae3_fuel_cons"]].any(axis=1),
            results[["lat", "long", "sog", "direction"]].any(axis=1),
        ]
    ).sum(axis=0)
    logger.debug("Completetness: %s", completeness)
    idx = completeness.idxmax()
    if not isinstance(idx, int):
        return None
    result = results.iloc[idx].replace({nan: None}).to_dict()
    if engine:
        engine = engine.lower()
        engines = (engine,)
    else:
        engines = ("me", "ae")
    engines_data = {}
    for key in result.keys():
        if key.startswith(engines):
            prefix, suffix = key.split("_", 1)
            if prefix not in engines_data:
                engines_data[prefix] = {"timestamp": result["timestamp"]}
            engines_data[prefix][suffix] = result[key]
    add_sql = text(
        """
with t as (select * from canbus_report_data crd where message_datetime = :dt)

select d.label_name, t.me_rpm as rpm, t.me_engine_load as engine_load,
t.me_fuel_temp as fuel_temp, t.me_exhaust_gas_temp_left as exhaust_gas_temp_left,
t.me_exhaust_gas_temp_right as exhaust_gas_temp_right, e.running_hours as running_hour
from device d
inner join engine e on d.engine_id = e.id
inner join vessel v on v.id = d.vessel_id
left outer join t on d.device_id = t.device_id
where v.imo = :imo and d.device_type_id in (1,4,6) and d.is_active = True
and ((:label)::text is null or d.label_name = upper(:label)::text)"""
    )
    add_results = await query_df_async(add_sql, {"imo": imo, "dt": result["timestamp"], "label": engine})
    if not add_results.empty:
        for i in range(0, add_results.shape[0]):
            lablel = str(add_results.at[i, "label_name"]).lower()
            if lablel not in engines_data:
                engines_data[lablel] = {"timestamp": result["timestamp"]}
            engines_data[lablel].update(add_results.iloc[i].replace({nan: None}).to_dict())
    result.update(engines_data)
    logger.debug("engine: %s", engine)
    logger.debug("Result: %s", result)
    if engine is None:
        logger.debug("engine: %s", engines_data)
        return VesselData.model_validate(result)
    elif engine.startswith("me"):
        return schema.MERTData.model_validate(result[engine])
    elif engine.startswith("ae"):
        logger.debug("engine: %s", engine)
        return schema.AERTData.model_validate(result[engine])