"""冻结裁决持久化：同一审计标识的相同提交永远返回同一结论。"""
from __future__ import annotations

import hashlib
import json
import os
import threading
from datetime import datetime, timezone
from typing import Optional

_LOCK = threading.Lock()


def fingerprint(payload: dict) -> str:
    """对提交内容（不含审计标识本身）做规范化哈希。"""
    body = {k: v for k, v in payload.items() if k != "audit_id"}
    canon = json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canon.encode("utf-8")).hexdigest()


class VerdictStore:
    def __init__(self, path: str):
        self.path = path
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        if not os.path.exists(path):
            self._write({})

    def _read(self) -> dict:
        with open(self.path, "r", encoding="utf-8") as fh:
            return json.load(fh)

    def _write(self, data: dict) -> None:
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False, indent=2)
        os.replace(tmp, self.path)

    def get(self, audit_id: str) -> Optional[dict]:
        with _LOCK:
            return self._read().get(audit_id)

    def submit(self, payload: dict, verdict: dict) -> tuple[dict, bool]:
        """返回 (冻结记录, 是否新创建)。内容冲突抛 ConflictError。"""
        fp = fingerprint(payload)
        with _LOCK:
            data = self._read()
            existing = data.get(payload["audit_id"])
            if existing is not None:
                if existing["fingerprint"] != fp:
                    raise ConflictError(payload["audit_id"])
                return existing, False
            record = {
                "audit_id": payload["audit_id"],
                "fingerprint": fp,
                "submitted_at": datetime.now(timezone.utc).isoformat(),
                "request": payload,
                "verdict": verdict,
            }
            data[payload["audit_id"]] = record
            self._write(data)
            return record, True


class ConflictError(Exception):
    def __init__(self, audit_id: str):
        self.audit_id = audit_id
        super().__init__(f"审计标识 {audit_id} 已用于不同内容的提交")
