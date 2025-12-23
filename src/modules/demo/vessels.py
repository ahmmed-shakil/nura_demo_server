from datetime import datetime, timezone, timedelta
import json
import os
import random
from random import randrange
import pandas as pd
from sqlalchemy import text, insert
from src.modules import Database, logger
from src.modules.schemas.vessel import Links, Position
from src.modules.schemas.fleet import VesselInfo, VesselKPI, VesselPosition

vessel_mapping = {
    '997147': '9971147',
    '997123': '9971123',
    '997135': '9971135',
    '9425863': '9425863',
    '9318266': '9318266',
    '9366469': '9366469',
    '9404730': '9404730',
    '9404728': '9404728'
}

name_mapping = {
    9971147: 'OSV 1',
    9971123: 'OSV 2',
    9971135: 'OSV 3',
}

imo_mapping = {
    9971147: 997147,
    9971123: 997123,
    9971135: 997135
}

mmsi_mapping = {
    9971147: 997147,
    9971123: 997123,
    9971135: 997135
}

customer_mapping = {
    'democompany': 'suryanautika'
}

merchant_vessels  = [
    {"imo": 99999009, "mmsi": 533333009, "name": "Compass Rose"},
    {"imo": 99999008, "mmsi": 533333008, "name": "Tidal Venture"},
    {"imo": 99999007, "mmsi": 533333007, "name": "Silver Crest"},
    {"imo": 99999006, "mmsi": 533333006, "name": "Eternal Drift"},
    {"imo": 99999005, "mmsi": 533333005, "name": "Seabound Spirit"},
    {"imo": 99999004, "mmsi": 533333004, "name": "Golden Trident"},
    {"imo": 99999003, "mmsi": 533333003, "name": "Mariner's Pride"},
    {"imo": 99999002, "mmsi": 533333002, "name": "Sea Falcon"},
    {"imo": 99999001, "mmsi": 533333001, "name": "Sea Guardian"},
    {"imo": 99999000, "mmsi": 533333000, "name": "Horizon Explorer"},
    {"imo": 9972827, "mmsi": 123412342, "name": "Pacific Navigator"},
    {"imo": 9971135, "mmsi": 533132669, "name": "Blue Horizon"},
    {"imo": 9971123, "mmsi": 533132535, "name": "Ocean Voyager"},
    {"imo": 9971147, "mmsi": 533132773, "name": "Atlantic Pioneer"}
]

def anonymize_dataset(imo):
    # Open the JSON file
    current_dir = os.path.dirname(os.path.realpath(__file__))
    data_points = {}
    with open(current_dir + "/vessel_data.json") as data_file:
        data_points = json.load(data_file)
        data_file.close()

    # Update date to be starting some days ago
    current_time = datetime.now(timezone.utc).timestamp()
    start_time = current_time - (len(data_points) - 1) * 24 * 60 * 60
    for data_point in data_points:
        data_point["Datetime_UTC"] = int(start_time) * 1000
        start_time += 24 * 60 * 60

    return data_points


def load_bulkset(imo):
    current_dir = os.path.dirname(os.path.realpath(__file__))
    data_points = pd.read_csv(current_dir + "/bulkship_data.csv")

    return data_points


def anonymize_bulkset(data_column, imo, points):
    content = points
    current_time = datetime.now(timezone.utc).timestamp()
    if imo == 9425863:
        dataset = load_bulkset(imo)
        nr_points = len(dataset.index)
        step = 60  # 10 minutes
        start_time = current_time - (nr_points + 1) * step
        for index in dataset.index:
            if data_column == "sog":
                content.append({
                        "timestamp": start_time + index * step,
                        "sog": dataset["STW"][index],
                    })

    return content


def anonymize_link(imo, results: Links) -> Links:
    current_time = datetime.now(timezone.utc).timestamp()
    for vessel in merchant_vessels:
        if imo == vessel["imo"]:
            for link in results.model_fields.keys():
                setattr(results, link, current_time - randrange(0, 120))

    if imo == 9425863:
        results = Links(
            online = 0,
            nmea = 0,
            noon = current_time - randrange(0, 120),
            canbus = 0,
            me1 = 0,
            me2 = 0,
            me3 = 0,
            ae1 = 0,
            ae2 = 0,
        )
    return results


def anonymize_vessels_kpi(vessels_kpi: list[VesselKPI]) -> list[VesselKPI]:
    demo_vessel = vessels_kpi[0]
    # Remove vessels for which we don't have anon data
    vessels_kpi[:] =  [x for x in vessels_kpi if x.imo in name_mapping]
    for index, item in enumerate(vessels_kpi):
        anon_imo = item.imo
        item.name = name_mapping[anon_imo]
        item.imo = imo_mapping[anon_imo]
        item.mmsi = mmsi_mapping[anon_imo]
        item.company_id = "democompany"
        item.fleet_type = "offshore"
        # Dummy positioning. Make these 2 vessel always green
        delta = timedelta(seconds=randrange(0, 60 * 9))
        item.position = set_dummy_position(item.position, delta, 10, 10)
        vessels_kpi[index] = item
    # Add merchant vessels
    for merchant_vessel in merchant_vessels:
        m_vessel = demo_vessel.model_copy()
        # Anonimize naming
        m_vessel.name = merchant_vessel["name"]
        m_vessel.imo = int(merchant_vessel["imo"])
        m_vessel.mmsi = int(merchant_vessel["mmsi"])
        m_vessel.company_id = "democompany"
        m_vessel.fleet_type = "merchant"
        if m_vessel.imo == 9425863:
            m_vessel.daily_est_cons = 17
            m_vessel.daily_avg_cons = 21
        # Dummy positioning. Merchant vessels can be whatever color
        delta = timedelta(seconds=randrange(0, 60 * 60 * 4))
        m_vessel.position = set_dummy_position(m_vessel.position, delta, 25, 110)
        # Specific bulkship data
        if m_vessel.imo == 9425863:
            m_vessel.position.timestamp = datetime.now(timezone.utc)
        vessels_kpi.append(m_vessel)
    return vessels_kpi


def set_dummy_position(
    status: Position, delta: timedelta, d_lat: int, d_long: int
) -> Position:
    current_time = datetime.now(timezone.utc)
    random.seed(current_time.microsecond)
    status.timestamp = current_time - delta
    status.direction = randrange(0, 3600) / 10
    if status.lat is None:
        status.lat = 0
    if status.long is None:
        status.long = 0
    status.lat += randrange(0, d_lat) / 10
    status.long += randrange(0, d_long) / 10
    return status


def anonymize_vessels(vessels: list[VesselInfo]) -> list[VesselInfo]:
    demo_vessel = vessels[0]
    # Remove vessels for which we don't have anon data
    vessels[:] =  [x for x in vessels if x.imo in name_mapping]
    for index, item in enumerate(vessels):
        anon_imo = item.imo
        item.name = name_mapping[anon_imo]
        item.imo = imo_mapping[anon_imo]
        item.company_id = "democompany"
        # Replage image links also
        if item.image_url:
            image_url = item.image_url.split("/")
            image_url[2] = "democompany"
            item.image_url = "/".join(image_url)
        vessels[index] = item

    # Add merchant vessels
    for merchant_vessel in merchant_vessels:
        m_vessel = demo_vessel.model_copy()
        # Anonimize naming
        m_vessel.name = merchant_vessel["name"]
        m_vessel.imo = int(merchant_vessel["imo"])
        m_vessel.company_id = "democompany"
        vessels.append(m_vessel)
    return vessels

def anonymize_vessels_position(positions: list[VesselPosition]) -> list[VesselPosition]:
    demo_vessel = positions[0]
    # Remove positions for which we don't have anon data
    positions[:] =  [x for x in positions if x.imo in name_mapping]

    for index, item in enumerate(positions):
        anon_imo = item.imo
        # item.name = name_mapping[anon_imo]
        # item.imo = imo_mapping[anon_imo]
        # Dummy positioning. Make these 2 vessel always green
        delta = timedelta(seconds=randrange(0, 60 * 9))
        item.position = set_dummy_position(item.position, delta, 10, 10)
        positions[index] = item

    # Add merchant positions
    for merchant_vessel in merchant_vessels:
        m_vessel = demo_vessel.model_copy()
        # Anonimize naming
        m_vessel.name = merchant_vessel["name"]
        m_vessel.imo = int(merchant_vessel["imo"])
        # Dummy positioning. Merchant vessels can be whatever color
        delta = timedelta(seconds=randrange(0, 60 * 60 * 4))
        m_vessel.position = set_dummy_position(m_vessel.position, delta, 25, 110)
        positions.append(m_vessel)
    return positions

def demo_import_vessels():
    # Loading & preparing demo-vessels
    demo_vessels = pd.read_excel('/app/src/temp/demo_ships.xlsx')
    table_columns = [
        'id', 'imo', 'mmsi', 'vessel_type_id', 'fuel_type_id',
        'customer_id', 'name',
        'is_noon_report_link', 'is_canbus_link', 'is_nmea_link',
        'alert_active_since', 'dwt', 'service_speed', 'image', 'is_active',
        'fleet_type_id'
        ]
    customer_id = 6
    vessel_type_id = 13
    fuel_type_id = 1
    demo_vessels['id'] = 2000 + demo_vessels.index
    demo_vessels['vessel_type_id'] = vessel_type_id
    demo_vessels['fuel_type_id'] = fuel_type_id
    demo_vessels['customer_id'] = customer_id

    # Prepare data for insertion - drop columns that are not needed
    imported_columns = list(demo_vessels.columns.values)
    for column in table_columns:
        if column in imported_columns:
            imported_columns.remove(column)

    demo_vessels =demo_vessels.drop(imported_columns, axis=1)
    demo_vessels = demo_vessels.to_dict('records')

    # Data insertion
    insert_stmt = text(
        """
INSERT INTO
    core.vessel
    (id, imo, mmsi, dwt, name, service_speed, fuel_type_id, customer_id, vessel_type_id)
VALUES
    (:id, :imo, :mmsi, :dwt, :name, :service_speed, :fuel_type_id, :customer_id, :vessel_type_id)
""")
    try:
        engine = Database.get_engine()
        with engine.connect() as connection:
          # clear previous demo_vessels

          # import new demo_vessels
          for row in demo_vessels:
              stmt = connection.execute(insert_stmt, row)
              pass;
          connection.commit()

    except Exception as e:
        logger.error("DB connection error. Error: %s", e)
        raise e
    return 'Import completed'
