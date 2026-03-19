import json
from dataclasses import dataclass
import dataclasses
import math
import numpy as np

@dataclass
class Ride:
    PULocationID: int
    DOLocationID: int
    passenger_count: int
    trip_distance: float
    tip_amount: float
    total_amount: float
    lpep_pickup_datetime: int  
    lpep_dropoff_datetime: int # epoch milliseconds

def clean_int(value):
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return 0
    return int(value)

def ride_from_row(row):
    return Ride(
        PULocationID=int(row['PULocationID']),
        DOLocationID=int(row['DOLocationID']),
        passenger_count=clean_int(row['passenger_count']),
        trip_distance=float(row['trip_distance']),
        tip_amount=float(row['tip_amount']),
        total_amount=float(row['total_amount']),
        lpep_pickup_datetime=int(row['lpep_pickup_datetime'].timestamp() * 1000),
        lpep_dropoff_datetime=int(row['lpep_dropoff_datetime'].timestamp() * 1000),
    )

def ride_serializer(ride):
    ride_dict = dataclasses.asdict(ride)
    ride_json = json.dumps(ride_dict)
    return ride_json.encode('utf-8')

def ride_deserializer(data):
    ride_json = data.decode('utf-8')
    ride_dict = json.loads(ride_json)
    return Ride(**ride_dict)

