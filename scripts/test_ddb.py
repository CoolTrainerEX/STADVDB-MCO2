import asyncio
import subprocess
import time
import httpx

NODES = {
    "1": "http://localhost:8001",
    "2": "http://localhost:8002",
    "3": "http://localhost:8003",
}


async def wait_for_node(node_id: str, timeout: float = 20.0):
    url = f"{NODES[node_id]}/health"
    deadline = time.time() + timeout
    async with httpx.AsyncClient(timeout=2.0) as client:
        while time.time() < deadline:
            try:
                r = await client.get(url)
                if r.status_code == 200:
                    return r.json()
            except httpx.RequestError:
                pass
            await asyncio.sleep(0.5)
    raise TimeoutError(f"Node {node_id} did not come back online within {timeout}s")


async def test_transparency():
    print("\n=== TEST 1: Fragmentation & Location Transparency ===")
    async with httpx.AsyncClient(timeout=5.0) as client:
        # 1. Ask Node 2 (even) for odd trip_id=1 -> must route to Node 3
        r_odd = await client.get(f"{NODES['2']}/trips/1")
        assert r_odd.status_code == 200, f"Expected 200, got {r_odd.status_code}"
        assert r_odd.json()["served_by_node"] == "3"
        print("[PASS] Node 2 transparently routed odd trip_id=1 to Node 3")

        # 2. Ask Node 3 (odd) for even trip_id=2 -> must route to Node 2
        r_even = await client.get(f"{NODES['3']}/trips/2")
        assert r_even.status_code == 200
        assert r_even.json()["served_by_node"] == "2"
        print("[PASS] Node 3 transparently routed even trip_id=2 to Node 2")

        # 3. Insert new odd trip_id=10001 via Node 3 -> must replicate to Node 1
        new_trip = {"trip_id": 10001, "fare_amount": 42.5, "total_amount": 45.0}
        r_ins = await client.post(f"{NODES['3']}/trips", json=new_trip)
        assert r_ins.status_code == 201 and r_ins.json()["replicated_immediately"] is True

        r_n1 = await client.get(f"{NODES['1']}/trips/10001")
        assert r_n1.status_code == 200 and r_n1.json()["served_by_node"] == "1"
        print("[PASS] Inserted trip_id=10001 on Node 3 and verified replica on Node 1")

        # 4. Delete trip_id=10001 via Node 1 -> must delete on both Node 1 and Node 3
        r_del = await client.delete(f"{NODES['1']}/trips/10001")
        assert r_del.status_code == 200 and r_del.json()["replicated_immediately"] is True

        r_gone = await client.get(f"{NODES['3']}/trips/10001")
        assert r_gone.status_code == 404
        print("[PASS] Deleted trip_id=10001 on Node 1 and verified removal on Node 3")


async def test_concurrency():
    print("\n=== TEST 2: Concurrent Reads & Writes Across Isolation Levels ===")
    isolation_levels = ["READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"]

    async with httpx.AsyncClient(timeout=10.0) as client:
        for iso in isolation_levels:
            headers = {"X-Isolation-Level": iso}
            tasks = []

            # Launch 8 concurrent updates (4 on Node 1, 4 on Node 2) + 4 concurrent reads on trip_id=20
            for i in range(1, 9):
                target_node = "1" if i % 2 == 1 else "2"
                payload = {"fare_amount": round(20.0 + i * 1.25, 2), "tip_amount": float(i)}
                tasks.append(
                    client.put(f"{NODES[target_node]}/trips/20", json=payload, headers=headers)
                )
            for _ in range(4):
                tasks.append(client.get(f"{NODES['3']}/trips/20", headers=headers))

            responses = await asyncio.gather(*tasks)
            status_codes = [r.status_code for r in responses]
            assert all(code == 200 for code in status_codes), f"Failed codes under {iso}: {status_codes}"

            # Allow any in-flight replication requests to settle
            await asyncio.sleep(0.5)

            # Verify Node 1 and Node 2 converged to the exact same row state
            n1_trip = (await client.get(f"{NODES['1']}/trips/20")).json()["trip"]
            n2_trip = (await client.get(f"{NODES['2']}/trips/20")).json()["trip"]
            assert n1_trip["fare_amount"] == n2_trip["fare_amount"], (
                f"Replica mismatch under {iso}: N1={n1_trip['fare_amount']} vs N2={n2_trip['fare_amount']}"
            )
            assert n1_trip["updated_at"] == n2_trip["updated_at"]
            print(
                f"[PASS] {iso}: 12 concurrent ops succeeded; Node 1 & Node 2 converged "
                f"(fare_amount={n1_trip['fare_amount']})"
            )


async def test_crash_recovery():
    print("\n=== TEST 3: Node Crash Simulation & Automatic Outbox Recovery ===")
    async with httpx.AsyncClient(timeout=5.0) as client:
        # 1. Stop app_node2 to simulate a node crash
        print("Stopping app_node2 container...")
        subprocess.run(["docker", "compose", "stop", "app_node2"], check=True)

        # 2. Write an update to even trip_id=10 on Node 1 while Node 2 is offline
        r_put = await client.put(f"{NODES['1']}/trips/10", json={"fare_amount": 123.45})
        assert r_put.status_code == 200
        body = r_put.json()
        assert body["replicated_immediately"] is False, "Expected immediate replication to fail while Node 2 is down"

        health_n1 = (await client.get(f"{NODES['1']}/health")).json()
        assert health_n1["pending_replications"] >= 1
        print(
            f"[PASS] Write succeeded on Node 1 while Node 2 was offline "
            f"(pending_replications={health_n1['pending_replications']})"
        )

        # 3. Restart app_node2 and wait for automatic background recovery
        print("Restarting app_node2 container...")
        subprocess.run(["docker", "compose", "start", "app_node2"], check=True)
        await wait_for_node("2")

        # Wait up to 10s for Node 1's recovery_worker to drain the PENDING queue
        for _ in range(20):
            h = (await client.get(f"{NODES['1']}/health")).json()
            if h["pending_replications"] == 0:
                break
            await asyncio.sleep(0.5)

        final_health = (await client.get(f"{NODES['1']}/health")).json()
        assert final_health["pending_replications"] == 0, "Pending replications did not drain!"

        # 4. Query Node 2 directly to verify it received the missed update
        r_n2 = await client.get(f"{NODES['2']}/trips/10")
        assert r_n2.status_code == 200
        n2_data = r_n2.json()
        assert n2_data["served_by_node"] == "2"
        assert n2_data["trip"]["fare_amount"] == 123.45
        print("[PASS] Node 2 recovered and synced missed update automatically (fare_amount=123.45)")


async def main():
    for node_id in ("1", "2", "3"):
        await wait_for_node(node_id)
    await test_transparency()
    await test_concurrency()
    await test_crash_recovery()
    print("\nAll distributed database tests passed!")


if __name__ == "__main__":
    asyncio.run(main())