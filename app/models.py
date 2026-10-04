"""接口请求/响应模型与校验。"""
from __future__ import annotations

import re
from typing import List

from pydantic import BaseModel, Field, field_validator, model_validator

AUDIT_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


class FlowIn(BaseModel):
    id: str = Field(min_length=1, max_length=32)
    priority: int = Field(ge=0, le=7)
    period_us: int = Field(ge=1)
    length_us: int = Field(ge=1)
    deadline_us: int = Field(ge=1)


class GateIn(BaseModel):
    time_us: int = Field(ge=0)
    priorities: List[int] = Field(default_factory= list)

    @field_validator("priorities")
    @classmethod
    def _check_priorities(cls, v: List[int]) -> List[int]:
        if any(p < 0 or p > 7 for p in v):
            raise ValueError("优先级必须在 0..7 之间")
        if len(set(v)) != len(v):
            raise ValueError("同一门控项内优先级不得重复")
        return v


class Submission(BaseModel):
    audit_id: str
    cycle_us: int = Field(ge=1)
    flows: List[FlowIn] = Field(min_length=1, max_length=8)
    gates: List[GateIn] = Field(min_length=1)

    @field_validator("audit_id")
    @classmethod
    def _check_audit(cls, v: str) -> str:
        if not AUDIT_ID_RE.match(v):
            raise ValueError("审计标识须为 1..64 位字母、数字、'-' 或 '_'")
        return v

    @model_validator(mode="after")
    def _check_shape(self) -> "Submission":
        ids = [f.id for f in self.flows]
        if len(set(ids)) != len(ids):
            raise ValueError("流标识不得重复")
        times = [g.time_us for g in self.gates]
        if times[0] != 0:
            raise ValueError("首个门控项时间必须为 0")
        if any(times[i] >= times[i + 1] for i in range(len(times) - 1)):
            raise ValueError("门控项必须按时间严格升序排列")
        if times[-1] >= self.cycle_us:
            raise ValueError("门控项时间必须小于门控周期")
        return self
