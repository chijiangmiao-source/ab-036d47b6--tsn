"""机载以太网门控调度裁决服务。"""
from __future__ import annotations

import os

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from . import engine
from .models import Submission
from .store import ConflictError, VerdictStore

STORE_PATH = os.environ.get("VERDICT_STORE", "/data/verdicts.json")
HERE = os.path.dirname(os.path.abspath(__file__))

app = FastAPI(title="机载以太网门控调度裁决", version="1.0.0")
store = VerdictStore(STORE_PATH)


def _run_engine(payload: dict) -> dict:
    sub = Submission(**payload)
    flows = [
        engine.Flow(
            id=f.id,
            priority=f.priority,
            period=f.period_us,
            length=f.length_us,
            deadline=f.deadline_us,
        )
        for f in sub.flows
    ]
    gates = [
        engine.Gate(time=g.time_us, priorities=frozenset(g.priorities))
        for g in sub.gates
    ]
    try:
        return engine.simulate(flows, gates, sub.cycle_us)
    except engine.EngineLimit as exc:
        raise HTTPException(status_code=422, detail=f"周期边界无法收敛（{exc}），拒绝裁决")


@app.get("/")
def index():
    return FileResponse(os.path.join(HERE, "static", "index.html"))


@app.get("/api/health")
def health():
    return {"status": "ok"}


@app.post("/api/submit")
def submit(payload: dict):
    try:
        sub = Submission(**payload)
    except Exception as exc:  # pydantic 校验错误
        return JSONResponse(status_code=422, content={"error": "输入校验失败", "detail": str(exc)})
    normalized = sub.model_dump()
    verdict = _run_engine(normalized)
    try:
        record, created = store.submit(normalized, verdict)
    except ConflictError as exc:
        existing = store.get(payload["audit_id"])
        raise HTTPException(
            status_code=409,
            detail={
                "error": "AUDIT_ID_CONFLICT",
                "message": str(exc),
                "hint": "原冻结裁决保持不变；请更换审计标识或提交完全相同的内容",
                "existing_verdict": existing,
            },
        )
    return {"created": created, **record}


@app.get("/api/verdict/{audit_id}")
def read_verdict(audit_id: str):
    record = store.get(audit_id)
    if record is None:
        raise HTTPException(status_code=404, detail="未找到该审计标识的冻结裁决")
    return {"created": False, **record}


app.mount("/static", StaticFiles(directory=os.path.join(HERE, "static")), name="static")
