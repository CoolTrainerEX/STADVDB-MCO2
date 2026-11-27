from datetime import datetime, timezone
from typing import Optional
from sqlmodel import Field, SQLModel


class TaxiTrip(SQLModel, table=True):
    __tablename__ = "taxi_trips"

    trip_id: Optional[int] = Field(default=None, primary_key=True)
    VendorID: Optional[int] = None
    lpep_pickup_datetime: Optional[datetime] = None
    lpep_dropoff_datetime: Optional[datetime] = None
    store_and_fwd_flag: Optional[str] = None
    RatecodeID: Optional[int] = None
    PULocationID: Optional[int] = None
    DOLocationID: Optional[int] = None
    passenger_count: Optional[int] = None
    trip_distance: Optional[float] = None
    fare_amount: Optional[float] = None
    extra: Optional[float] = None
    mta_tax: Optional[float] = None
    tip_amount: Optional[float] = None
    tolls_amount: Optional[float] = None
    ehail_fee: Optional[float] = None
    improvement_surcharge: Optional[float] = None
    total_amount: Optional[float] = None
    payment_type: Optional[int] = None
    trip_type: Optional[int] = None
    congestion_surcharge: Optional[float] = None
    cbd_congestion_fee: Optional[float] = None
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )


class ReplicationLog(SQLModel, table=True):
    __tablename__ = "replication_log"

    log_id: Optional[int] = Field(default=None, primary_key=True)
    tx_id: str = Field(index=True)
    target_node: str
    operation: str
    trip_id: int
    payload_json: str
    status: str = Field(default="PENDING", index=True)
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )