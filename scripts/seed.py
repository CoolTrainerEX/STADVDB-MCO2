import csv
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from sqlmodel import Session, SQLModel, create_engine, text
from stadvdb_mco2.models import TaxiTrip

CSV_PATH = Path("data/green_taxi_data.csv")
MAX_ROWS = 10000  

NODE_URLS = {
    "node1": "postgresql+psycopg://ddb_user:ddb_password@localhost:5436/taxi_node1",
    "node2": "postgresql+psycopg://ddb_user:ddb_password@localhost:5434/taxi_node2",
    "node3": "postgresql+psycopg://ddb_user:ddb_password@localhost:5435/taxi_node3",
}


def parse_int(val: str | None) -> int | None:
    if val is None or val.strip() == "":
        return None
    return int(float(val.strip()))


def parse_float(val: str | None) -> float | None:
    if val is None or val.strip() == "":
        return None
    return float(val.strip())


def parse_str(val: str | None) -> str | None:
    if val is None or val.strip() == "":
        return None
    return val.strip()


def parse_dt(val: str | None) -> datetime | None:
    if val is None or val.strip() == "":
        return None
    s = val.strip()
    for fmt in ("%m/%d/%Y %H:%M", "%m/%d/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def make_trip(trip_id: int, row: dict[str, str], now: datetime) -> TaxiTrip:
    return TaxiTrip(
        trip_id=trip_id,
        VendorID=parse_int(row.get("VendorID")),
        lpep_pickup_datetime=parse_dt(row.get("lpep_pickup_datetime")),
        lpep_dropoff_datetime=parse_dt(row.get("lpep_dropoff_datetime")),
        store_and_fwd_flag=parse_str(row.get("store_and_fwd_flag")),
        RatecodeID=parse_int(row.get("RatecodeID")),
        PULocationID=parse_int(row.get("PULocationID")),
        DOLocationID=parse_int(row.get("DOLocationID")),
        passenger_count=parse_int(row.get("passenger_count")),
        trip_distance=parse_float(row.get("trip_distance")),
        fare_amount=parse_float(row.get("fare_amount")),
        extra=parse_float(row.get("extra")),
        mta_tax=parse_float(row.get("mta_tax")),
        tip_amount=parse_float(row.get("tip_amount")),
        tolls_amount=parse_float(row.get("tolls_amount")),
        ehail_fee=parse_float(row.get("ehail_fee")),
        improvement_surcharge=parse_float(row.get("improvement_surcharge")),
        total_amount=parse_float(row.get("total_amount")),
        payment_type=parse_int(row.get("payment_type")),
        trip_type=parse_int(row.get("trip_type")),
        congestion_surcharge=parse_float(row.get("congestion_surcharge")),
        cbd_congestion_fee=parse_float(row.get("cbd_congestion_fee")),
        updated_at=now,
    )


def main():
    engines = {name: create_engine(url) for name, url in NODE_URLS.items()}

    for engine in engines.values():
        SQLModel.metadata.create_all(engine)

    now = datetime.now(timezone.utc)
    node1_rows, node2_rows, node3_rows = [], [], []

    with CSV_PATH.open(mode="r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for idx, row in enumerate(reader, start=1):
            if idx > MAX_ROWS:
                break
            node1_rows.append(make_trip(idx, row, now))
            if idx % 2 == 0:
                node2_rows.append(make_trip(idx, row, now))
            else:
                node3_rows.append(make_trip(idx, row, now))

    batches = {
        "node1": node1_rows,
        "node2": node2_rows,
        "node3": node3_rows,
    }

    for name, engine in engines.items():
        with Session(engine) as session:
            session.exec(text("TRUNCATE TABLE taxi_trips RESTART IDENTITY;"))
            session.bulk_save_objects(batches[name])
            session.commit()
        print(f"Seeded {name}: {len(batches[name])} rows")


if __name__ == "__main__":
    main()