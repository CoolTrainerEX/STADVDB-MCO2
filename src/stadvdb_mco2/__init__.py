import asyncio
import json
import os
import random
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional
from uuid import uuid4

import httpx
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
from sqlalchemy.exc import DBAPIError
from sqlmodel import Session, SQLModel, create_engine, select, text

from stadvdb_mco2.models import ReplicationLog, TaxiTrip

NODE_ID = os.getenv("NODE_ID", "1")
DATABASE_URL = os.getenv(
    "LOCAL_DB_URL",
    "postgresql+psycopg://ddb_user:ddb_password@localhost:5436/taxi_node1",
)
NODE_URLS = {
    "1": os.getenv("NODE1_URL", "http://localhost:8001"),
    "2": os.getenv("NODE2_URL", "http://localhost:8002"),
    "3": os.getenv("NODE3_URL", "http://localhost:8003"),
}
DEFAULT_ISOLATION = os.getenv("ISOLATION_LEVEL", "REPEATABLE READ").upper()
VALID_ISOLATIONS = {"READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"}

engine = create_engine(DATABASE_URL, pool_pre_ping=True)


# SCHEMAS
class TripCreate(BaseModel):
    trip_id: Optional[int] = None
    VendorID: Optional[int] = 2
    lpep_pickup_datetime: Optional[datetime] = None
    lpep_dropoff_datetime: Optional[datetime] = None
    store_and_fwd_flag: Optional[str] = "N"
    RatecodeID: Optional[int] = 1
    PULocationID: Optional[int] = None
    DOLocationID: Optional[int] = None
    passenger_count: Optional[int] = 1
    trip_distance: Optional[float] = 0.0
    fare_amount: Optional[float] = 0.0
    extra: Optional[float] = 0.0
    mta_tax: Optional[float] = 0.5
    tip_amount: Optional[float] = 0.0
    tolls_amount: Optional[float] = 0.0
    ehail_fee: Optional[float] = None
    improvement_surcharge: Optional[float] = 1.0
    total_amount: Optional[float] = 0.0
    payment_type: Optional[int] = 1
    trip_type: Optional[int] = 1
    congestion_surcharge: Optional[float] = 0.0
    cbd_congestion_fee: Optional[float] = 0.0


class TripUpdate(BaseModel):
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


class ReplicationPayload(BaseModel):
    tx_id: str
    operation: str
    trip_id: int
    payload: dict


# ROUTING HELPERS
def fragment_node_for(trip_id: int) -> str:
    return "2" if trip_id % 2 == 0 else "3"


def owners_for(trip_id: int) -> set[str]:
    return {"1", fragment_node_for(trip_id)}


def is_local_owner(trip_id: int) -> bool:
    return NODE_ID in owners_for(trip_id)


def peer_owner(trip_id: int) -> str:
    return fragment_node_for(trip_id) if NODE_ID == "1" else "1"


def resolve_isolation(header_val: Optional[str]) -> str:
    if header_val and header_val.upper() in VALID_ISOLATIONS:
        return header_val.upper()
    return DEFAULT_ISOLATION if DEFAULT_ISOLATION in VALID_ISOLATIONS else "REPEATABLE READ"


def parse_dt_field(val) -> Optional[datetime]:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val if val.tzinfo else val.replace(tzinfo=timezone.utc)
    dt = datetime.fromisoformat(str(val))
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def hydrate_trip(data: dict) -> TaxiTrip:
    cleaned = dict(data)
    for dt_col in ("lpep_pickup_datetime", "lpep_dropoff_datetime", "updated_at"):
        if dt_col in cleaned and cleaned[dt_col] is not None:
            cleaned[dt_col] = parse_dt_field(cleaned[dt_col])
    return TaxiTrip(**cleaned)


# REPLICATION & RECOVERY
async def push_log_entry(log_id: int) -> bool:
    with Session(engine) as session:
        entry = session.get(ReplicationLog, log_id)
        if not entry or entry.status == "SYNCED":
            return True
        target_url = NODE_URLS[entry.target_node]
        body = {
            "tx_id": entry.tx_id,
            "operation": entry.operation,
            "trip_id": entry.trip_id,
            "payload": json.loads(entry.payload_json),
        }

    try:
        async with httpx.AsyncClient(timeout=2.0) as client:
            resp = await client.post(f"{target_url}/internal/replicate", json=body)
        if resp.status_code == 200:
            with Session(engine) as session:
                entry = session.get(ReplicationLog, log_id)
                if entry:
                    entry.status = "SYNCED"
                    session.add(entry)
                    session.commit()
            return True
    except httpx.RequestError:
        pass
    return False


async def recovery_worker():
    while True:
        try:
            with Session(engine) as session:
                pending_ids = session.exec(
                    select(ReplicationLog.log_id)
                    .where(ReplicationLog.status == "PENDING")
                    .order_by(ReplicationLog.log_id)
                ).all()
            for log_id in pending_ids:
                if log_id is not None:
                    ok = await push_log_entry(log_id)
                    if not ok:
                        break  
        except Exception:
            pass
        await asyncio.sleep(2.0)


@asynccontextmanager
async def lifespan(app: FastAPI):
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        session.exec(
            text(
                "SELECT setval(pg_get_serial_sequence('taxi_trips', 'trip_id'), "
                "COALESCE((SELECT MAX(trip_id) FROM taxi_trips), 0) + 1, false);"
            )
        )
        session.commit()

    task = asyncio.create_task(recovery_worker())
    yield
    task.cancel()


app = FastAPI(title=f"STADVDB MCO2 - Node {NODE_ID}", lifespan=lifespan)


# ENDPOINTS
@app.get("/health")
def health():
    with Session(engine) as session:
        pending = len(
            session.exec(
                select(ReplicationLog).where(ReplicationLog.status == "PENDING")
            ).all()
        )
    return {"node_id": NODE_ID, "status": "ok", "pending_replications": pending}


@app.get("/trips")
def list_trips(skip: int = 0, limit: int = 20):
    with Session(engine) as session:
        rows = session.exec(
            select(TaxiTrip).order_by(TaxiTrip.trip_id).offset(skip).limit(limit)
        ).all()
        return {"served_by_node": NODE_ID, "count": len(rows), "trips": rows}


@app.get("/trips/{trip_id}")
async def get_trip(
    trip_id: int,
    x_isolation_level: Optional[str] = Header(default=None),
    x_forwarded: Optional[str] = Header(default=None),
):
    iso = resolve_isolation(x_isolation_level)

    if is_local_owner(trip_id):
        with Session(engine) as session:
            session.exec(text(f"SET TRANSACTION ISOLATION LEVEL {iso}"))
            trip = session.get(TaxiTrip, trip_id)
            if trip:
                return {"served_by_node": NODE_ID, "trip": trip}

    if x_forwarded != "true":
        candidates = [fragment_node_for(trip_id), "1"]
        for target in candidates:
            if target == NODE_ID:
                continue
            try:
                async with httpx.AsyncClient(timeout=3.0) as client:
                    resp = await client.get(
                        f"{NODE_URLS[target]}/trips/{trip_id}",
                        headers={"X-Isolation-Level": iso, "X-Forwarded": "true"},
                    )
                if resp.status_code == 200:
                    return resp.json()
                if resp.status_code == 404:
                    raise HTTPException(status_code=404, detail=f"Trip {trip_id} not found")
            except httpx.RequestError:
                continue

    raise HTTPException(status_code=404, detail=f"Trip {trip_id} not found")


@app.post("/trips", status_code=201)
async def create_trip(
    payload: TripCreate, x_isolation_level: Optional[str] = Header(default=None)
):
    iso = resolve_isolation(x_isolation_level)

    if payload.trip_id is None and NODE_ID != "1":
        try:
            async with httpx.AsyncClient(timeout=4.0) as client:
                resp = await client.post(
                    f"{NODE_URLS['1']}/trips",
                    json=payload.model_dump(mode="json"),
                    headers={"X-Isolation-Level": iso},
                )
            if resp.status_code in (200, 201):
                return resp.json()
        except httpx.RequestError:
            pass

    if payload.trip_id is not None and not is_local_owner(payload.trip_id):
        for target in (fragment_node_for(payload.trip_id), "1"):
            if target == NODE_ID:
                continue
            try:
                async with httpx.AsyncClient(timeout=4.0) as client:
                    resp = await client.post(
                        f"{NODE_URLS[target]}/trips",
                        json=payload.model_dump(mode="json"),
                        headers={"X-Isolation-Level": iso},
                    )
                if resp.status_code in (200, 201):
                    return resp.json()
            except httpx.RequestError:
                continue
        raise HTTPException(status_code=503, detail="Owner nodes unavailable")

    now = datetime.now(timezone.utc)
    trip_data = payload.model_dump()
    if trip_data["trip_id"] is None and NODE_ID != "1":
        with Session(engine) as s:
            max_id = s.exec(select(TaxiTrip.trip_id).order_by(TaxiTrip.trip_id.desc())).first() or 10000
        candidate = max_id + 1
        desired_mod = 0 if NODE_ID == "2" else 1
        if candidate % 2 != desired_mod:
            candidate += 1
        trip_data["trip_id"] = candidate

    trip = TaxiTrip(**trip_data, updated_at=now)
    with Session(engine) as session:
        session.exec(text(f"SET TRANSACTION ISOLATION LEVEL {iso}"))
        session.add(trip)
        session.flush()  
        session.refresh(trip)

        peer_id = peer_owner(trip.trip_id)
        log_entry = ReplicationLog(
            tx_id=str(uuid4()),
            target_node=peer_id,
            operation="INSERT",
            trip_id=trip.trip_id,
            payload_json=json.dumps(trip.model_dump(mode="json")),
            status="PENDING",
        )
        session.add(log_entry)
        session.commit()
        session.refresh(trip)
        session.refresh(log_entry)
        log_id = log_entry.log_id
        trip_out = trip.model_dump(mode="json")

    synced = await push_log_entry(log_id)
    return {"served_by_node": NODE_ID, "replicated_immediately": synced, "trip": trip_out}


@app.put("/trips/{trip_id}")
async def update_trip(
    trip_id: int,
    payload: TripUpdate,
    x_isolation_level: Optional[str] = Header(default=None),
):
    iso = resolve_isolation(x_isolation_level)

    if not is_local_owner(trip_id):
        for target in (fragment_node_for(trip_id), "1"):
            if target == NODE_ID:
                continue
            try:
                async with httpx.AsyncClient(timeout=4.0) as client:
                    resp = await client.put(
                        f"{NODE_URLS[target]}/trips/{trip_id}",
                        json=payload.model_dump(exclude_unset=True, mode="json"),
                        headers={"X-Isolation-Level": iso},
                    )
                if resp.status_code == 200:
                    return resp.json()
                if resp.status_code == 404:
                    raise HTTPException(status_code=404, detail=f"Trip {trip_id} not found")
            except httpx.RequestError:
                continue
        raise HTTPException(status_code=503, detail="Owner nodes unavailable")

    updates = payload.model_dump(exclude_unset=True)
    if not updates:
        raise HTTPException(status_code=400, detail="No fields provided to update")

    for attempt in range(4):
        try:
            with Session(engine) as session:
                session.exec(text(f"SET TRANSACTION ISOLATION LEVEL {iso}"))
                session.exec(text("SET LOCAL lock_timeout = '3s'"))

                stmt = select(TaxiTrip).where(TaxiTrip.trip_id == trip_id).with_for_update()
                trip = session.exec(stmt).first()
                if not trip:
                    raise HTTPException(status_code=404, detail=f"Trip {trip_id} not found")

                for key, val in updates.items():
                    setattr(trip, key, val)
                trip.updated_at = datetime.now(timezone.utc)

                peer_id = peer_owner(trip_id)
                log_entry = ReplicationLog(
                    tx_id=str(uuid4()),
                    target_node=peer_id,
                    operation="UPDATE",
                    trip_id=trip_id,
                    payload_json=json.dumps(trip.model_dump(mode="json")),
                    status="PENDING",
                )
                session.add(trip)
                session.add(log_entry)
                session.commit()
                session.refresh(trip)
                session.refresh(log_entry)
                log_id = log_entry.log_id
                trip_out = trip.model_dump(mode="json")
                break
        except DBAPIError as exc:
            sqlstate = getattr(exc.orig, "sqlstate", None)
            if sqlstate in {"40001", "40P01"} and attempt < 3:
                await asyncio.sleep(random.uniform(0.02, 0.12))
                continue
            raise HTTPException(status_code=409, detail=f"Transaction conflict ({sqlstate})")

    synced = await push_log_entry(log_id)
    return {"served_by_node": NODE_ID, "replicated_immediately": synced, "trip": trip_out}


@app.delete("/trips/{trip_id}")
async def delete_trip(
    trip_id: int, x_isolation_level: Optional[str] = Header(default=None)
):
    iso = resolve_isolation(x_isolation_level)

    if not is_local_owner(trip_id):
        for target in (fragment_node_for(trip_id), "1"):
            if target == NODE_ID:
                continue
            try:
                async with httpx.AsyncClient(timeout=4.0) as client:
                    resp = await client.delete(
                        f"{NODE_URLS[target]}/trips/{trip_id}",
                        headers={"X-Isolation-Level": iso},
                    )
                if resp.status_code == 200:
                    return resp.json()
                if resp.status_code == 404:
                    raise HTTPException(status_code=404, detail=f"Trip {trip_id} not found")
            except httpx.RequestError:
                continue
        raise HTTPException(status_code=503, detail="Owner nodes unavailable")

    with Session(engine) as session:
        session.exec(text(f"SET TRANSACTION ISOLATION LEVEL {iso}"))
        session.exec(text("SET LOCAL lock_timeout = '3s'"))

        stmt = select(TaxiTrip).where(TaxiTrip.trip_id == trip_id).with_for_update()
        trip = session.exec(stmt).first()
        if not trip:
            raise HTTPException(status_code=404, detail=f"Trip {trip_id} not found")

        now = datetime.now(timezone.utc)
        peer_id = peer_owner(trip_id)
        log_entry = ReplicationLog(
            tx_id=str(uuid4()),
            target_node=peer_id,
            operation="DELETE",
            trip_id=trip_id,
            payload_json=json.dumps({"trip_id": trip_id, "updated_at": now.isoformat()}),
            status="PENDING",
        )
        session.delete(trip)
        session.add(log_entry)
        session.commit()
        session.refresh(log_entry)
        log_id = log_entry.log_id

    synced = await push_log_entry(log_id)
    return {"served_by_node": NODE_ID, "deleted_trip_id": trip_id, "replicated_immediately": synced}


# INTERNAL REPLICA ENDPOINT
@app.post("/internal/replicate")
def receive_replication(req: ReplicationPayload):
    with Session(engine) as session:
        session.exec(text("SET LOCAL lock_timeout = '3s'"))
        existing = session.exec(
            select(TaxiTrip).where(TaxiTrip.trip_id == req.trip_id).with_for_update()
        ).first()

        incoming_ts = parse_dt_field(req.payload.get("updated_at")) or datetime.now(timezone.utc)

        if req.operation in {"INSERT", "UPDATE"}:
            if existing:
                local_ts = parse_dt_field(existing.updated_at)
                if local_ts and incoming_ts < local_ts:
                    return {"status": "ignored_stale", "tx_id": req.tx_id}
                updated_obj = hydrate_trip(req.payload)
                for field_name in TaxiTrip.model_fields:
                    if field_name != "trip_id":
                        setattr(existing, field_name, getattr(updated_obj, field_name))
                session.add(existing)
            else:
                new_trip = hydrate_trip(req.payload)
                session.add(new_trip)
            session.commit()
            return {"status": "applied", "tx_id": req.tx_id}

        if req.operation == "DELETE":
            if existing:
                local_ts = parse_dt_field(existing.updated_at)
                if local_ts and incoming_ts < local_ts:
                    return {"status": "ignored_stale", "tx_id": req.tx_id}
                session.delete(existing)
                session.commit()
            return {"status": "deleted", "tx_id": req.tx_id}

    raise HTTPException(status_code=400, detail=f"Unknown operation: {req.operation}")