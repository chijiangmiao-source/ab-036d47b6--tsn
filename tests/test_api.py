"""HTTP 接口测试：冻结裁决、幂等读取、冲突、校验、逐时隙渲染数据。"""
from __future__ import annotations

import importlib
import os

import pytest


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("VERDICT_STORE", str(tmp_path / "verdicts.json"))
    import app.main as main
    importlib.reload(main)  # 让存储路径在临时目录重建
    from fastapi.testclient import TestClient
    with TestClient(main.app) as c:
        yield c


CARRY_PAYLOAD = {
    "audit_id": "AUDIT-CARRY-1",
    "cycle_us": 1000,
    "flows": [
        {"id": "F_HIGH", "priority": 6, "period_us": 2000, "length_us": 120, "deadline_us": 2000},
        {"id": "F_LOW", "priority": 1, "period_us": 3000, "length_us": 260, "deadline_us": 3000},
    ],
    "gates": [
        {"time_us": 0, "priorities": [1, 2, 3, 4, 5, 6, 7]},
        {"time_us": 700, "priorities": [6, 7]},
    ],
}

GROWTH_PAYLOAD = {
    "audit_id": "AUDIT-GROWTH-1",
    "cycle_us": 1000,
    "flows": [
        {"id": "A", "priority": 6, "period_us": 500, "length_us": 300, "deadline_us": 500},
        {"id": "B", "priority": 6, "period_us": 500, "length_us": 300, "deadline_us": 500},
    ],
    "gates": [{"time_us": 0, "priorities": [6, 7]}],
}


def test_health(client):
    assert client.get("/api/health").json()["status"] == "ok"


def test_index_page_served(client):
    r = client.get("/")
    assert r.status_code == 200 and "门控调度" in r.text


def test_submit_carryover_pass_and_freeze(client):
    r = client.post("/api/submit", json=CARRY_PAYLOAD)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["created"] is True
    assert body["verdict"]["status"] == "PASS"
    assert body["verdict"]["flow_evidence"]["F_HIGH"]
    # 逐时隙数据完备
    tl = body["verdict"]["timeline"]
    assert tl and all(set(s) >= {"t0", "t1", "gate", "queues", "transmission"} for s in tl)


def test_identical_resubmit_returns_same_frozen_verdict(client):
    a = client.post("/api/submit", json=CARRY_PAYLOAD).json()
    b = client.post("/api/submit", json=CARRY_PAYLOAD).json()
    assert b["created"] is False
    assert b["verdict"] == a["verdict"]
    assert b["submitted_at"] == a["submitted_at"]
    assert b["fingerprint"] == a["fingerprint"]

    r = client.get(f"/api/verdict/{CARRY_PAYLOAD['audit_id']}").json()
    assert r["verdict"] == a["verdict"]


def test_conflicting_content_keeps_original_verdict(client):
    first = client.post("/api/submit", json=CARRY_PAYLOAD).json()
    conflict = {**CARRY_PAYLOAD, "cycle_us": 2000}
    r = client.post("/api/submit", json=conflict)
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["error"] == "AUDIT_ID_CONFLICT"
    assert detail["existing_verdict"]["verdict"]["status"] == "PASS"

    kept = client.get(f"/api/verdict/{CARRY_PAYLOAD['audit_id']}").json()
    assert kept["request"]["cycle_us"] == 1000
    assert kept["verdict"] == first["verdict"]


def test_growth_scenario_rejected_via_http(client):
    r = client.post("/api/submit", json=GROWTH_PAYLOAD)
    assert r.status_code == 200
    v = r.json()["verdict"]
    assert v["status"] == "REJECTED_QUEUE_GROWTH"
    samples = v["growth"]["samples"]
    assert len(samples) == 5
    assert all(samples[i + 1]["outstanding_length"] > samples[i]["outstanding_length"]
               for i in range(4))


def test_missing_verdict_404(client):
    assert client.get("/api/verdict/NOPE").status_code == 404


@pytest.mark.parametrize("payload", [
    # 门控项未按时间排序
    {"audit_id": "X", "cycle_us": 1000,
     "flows": [{"id": "F", "priority": 1, "period_us": 1000, "length_us": 10, "deadline_us": 1000}],
     "gates": [{"time_us": 10, "priorities": [1]}, {"time_us": 0, "priorities": [1]}]},
    # 首门不在 0
    {"audit_id": "X", "cycle_us": 1000,
     "flows": [{"id": "F", "priority": 1, "period_us": 1000, "length_us": 10, "deadline_us": 1000}],
     "gates": [{"time_us": 5, "priorities": [1]}]},
    # 非法审计标识
    {"audit_id": "bad id!", "cycle_us": 1000,
     "flows": [{"id": "F", "priority": 1, "period_us": 1000, "length_us": 10, "deadline_us": 1000}],
     "gates": [{"time_us": 0, "priorities": [1]}]},
    # 重复流标识
    {"audit_id": "X", "cycle_us": 1000,
     "flows": [
         {"id": "F", "priority": 1, "period_us": 1000, "length_us": 10, "deadline_us": 1000},
         {"id": "F", "priority": 2, "period_us": 1000, "length_us": 10, "deadline_us": 1000}],
     "gates": [{"time_us": 0, "priorities": [1, 2]}]},
    # 9 条流
    {"audit_id": "X", "cycle_us": 1000,
     "flows": [{"id": f"F{i}", "priority": 1, "period_us": 1000, "length_us": 1,
                "deadline_us": 1000} for i in range(9)],
     "gates": [{"time_us": 0, "priorities": [1]}]},
])
def test_invalid_payloads_rejected(client, payload):
    r = client.post("/api/submit", json=payload)
    assert r.status_code == 422
    # 校验失败不得写入冻结记录
    assert client.get("/api/verdict/X").status_code == 404


def test_overdue_payload_reports_blockers(client):
    payload = {
        "audit_id": "AUDIT-OD",
        "cycle_us": 1000,
        "flows": [
            {"id": "H", "priority": 6, "period_us": 500, "length_us": 300, "deadline_us": 500},
            {"id": "L", "priority": 1, "period_us": 1000, "length_us": 300, "deadline_us": 500},
        ],
        "gates": [{"time_us": 0, "priorities": [1, 2, 3, 4, 5, 6, 7]}],
    }
    v = client.post("/api/submit", json=payload).json()["verdict"]
    assert v["status"] == "OVERDUE"
    o = v["first_overdue"]
    assert o["flow"] == "L" and o["release"] == 0 and o["enqueue"] == 0
    assert o["start"] == 300 and o["end"] == 600
    assert any(b["reason"] == "higher_priority" for b in o["blockers"])
