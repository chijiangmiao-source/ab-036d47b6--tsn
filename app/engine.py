"""门控调度引擎。

语义（时间单位：整数微秒）：
- 单出口、非抢占：一帧一旦开始发送必定发完，期间门控变化不打断发送。
- 严格优先级：优先级数值越大越高（IEEE 802.1Q 约定，7 最高）；同优先级 FIFO。
- 门控项按周期循环，仅放行列明优先级；若帧无法在本优先级“关门”前完整发送，则继续等待。
- 流按自身周期释放帧（首帧 t=0），未发送帧跨门控周期遗留，同流新帧入队追加，不覆盖旧帧。
- 超期判定：帧实际发送完成时间 > 释放时间 + 截止期。
- 收敛判定：在超周期（门控周期与各流周期的 LCM）边界比较队列签名，
  空边界或签名重复即收敛；连续增长则以队列增长证据拒绝，不截断为通过。
"""
from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass
from typing import Optional

PASS = "PASS"
OVERDUE = "OVERDUE"
REJECTED_QUEUE_GROWTH = "REJECTED_QUEUE_GROWTH"

INF = float("inf")

# 连续 4 次超周期边界未完成工作量严格上升即判定无法收敛
GROWTH_RUN = 4
MAX_HYPERPERIODS = 40
EVENT_GUARD = 1_000_000
# 通过取证时最多再排空的超周期数
MAX_DRAIN_PERIODS = 6


class EngineLimit(Exception):
    """超出分析限仍无法给出结论。"""


@dataclass(frozen=True)
class Flow:
    id: str
    priority: int
    period: int
    length: int  # 发送时长
    deadline: int


@dataclass(frozen=True)
class Gate:
    time: int
    priorities: frozenset


@dataclass
class Frame:
    flow: Flow
    idx: int
    release: int

    @property
    def deadline(self) -> int:
        return self.release + self.flow.deadline

    @property
    def length(self) -> int:
        return self.flow.length

    @property
    def priority(self) -> int:
        return self.flow.priority


@dataclass
class Tx:
    frame: Frame
    start: int
    end: int


def lcm(a: int, b: int) -> int:
    return a // math.gcd(a, b) * b


def _windows(cycle: int, gates: list[Gate]):
    """返回 (bounds, sets)：窗口 i 为 [bounds[i], bounds[i+1])，bounds 末位为 cycle。"""
    bounds = [g.time for g in gates] + [cycle]
    sets = [g.priorities for g in gates]
    return bounds, sets


def gate_set_at(t: int, cycle: int, bounds, sets) -> frozenset:
    off = t % cycle
    for i in range(len(sets)):
        if bounds[i] <= off < bounds[i + 1]:
            return sets[i]
    return sets[-1]


def next_gate_change(t: int, cycle: int, bounds) -> int:
    c, off = divmod(t, cycle)
    for i in range(len(bounds) - 1):
        if bounds[i] <= off < bounds[i + 1]:
            return c * cycle + bounds[i + 1]
    return (c + 1) * cycle


def close_time(t: int, priority: int, cycle: int, bounds, sets):
    """从 t 起 priority 连续被放行的截止时刻（该优先级“关门”时刻）；恒开放返回 INF。"""
    c, off = divmod(t, cycle)
    n = len(sets)
    i = 0
    for k in range(n):
        if bounds[k] <= off < bounds[k + 1]:
            i = k
            break
    if priority not in sets[i]:
        return t
    # 当前窗口剩余时长，再依次考察后续窗口；扫过全部窗口仍开放 => 恒开放
    remaining = bounds[i + 1] - off
    k = i
    for _ in range(n - 1):
        nk = (k + 1) % n
        if priority not in sets[nk]:
            return t + remaining
        remaining += bounds[nk + 1] - bounds[nk]  # nk==n-1 时为 cycle-bounds[n-1]
        k = nk
    return INF


def _frame_dict(f: Frame, **extra) -> dict:
    d = {
        "flow": f.flow.id,
        "instance": f.idx,
        "priority": f.priority,
        "release": f.release,
        "enqueue": f.release,  # 释放即入出口队列
        "length": f.length,
        "deadline": f.deadline,
    }
    d.update(extra)
    return d


class _Machine:
    """一次具体模拟的出口机台：队列 + 单发送位。"""

    def __init__(self, flows: list[Flow], cycle: int, bounds, sets):
        self.flows = flows
        self.cycle = cycle
        self.bounds = bounds
        self.sets = sets
        self.queues: dict[int, deque[Frame]] = {p: deque() for p in range(8)}
        self.next_idx = {f.id: 0 for f in flows}
        self.next_release_at = {f.id: 0 for f in flows}
        self.tx: Optional[Tx] = None
        self.records: list[dict] = []
        self.slots: list[dict] = []

    def enqueue_due(self, now: int) -> None:
        for f in self.flows:
            while self.next_release_at[f.id] <= now:
                k = self.next_idx[f.id]
                # 新帧追加到队尾，绝不覆盖未发送帧
                self.queues[f.priority].append(Frame(f, k, self.next_release_at[f.id]))
                self.next_idx[f.id] = k + 1
                self.next_release_at[f.id] = (k + 1) * f.period

    def next_release(self) -> float:
        return min(self.next_release_at[f.id] for f in self.flows)

    def queued_frames(self) -> list[Frame]:
        out = []
        for p in range(8):
            out.extend(self.queues[p])
        return out

    def snapshot(self, now: int) -> dict:
        qf = self.queued_frames()
        outstanding_length = sum(fr.length for fr in qf)
        if self.tx is not None:
            outstanding_length += self.tx.end - now
        return {
            "time": now,
            "queued_count": len(qf),
            "queued_length": sum(fr.length for fr in qf),
            "outstanding_length": outstanding_length,
            "queues": [_frame_dict(fr) for fr in sorted(qf, key=lambda x: (x.release, x.flow.id))],
            "transmission": (
                {
                    "flow": self.tx.frame.flow.id,
                    "instance": self.tx.frame.idx,
                    "priority": self.tx.frame.priority,
                    "start": self.tx.start,
                    "end": self.tx.end,
                    "remaining": self.tx.end - now,
                }
                if self.tx
                else None
            ),
        }

    def signature(self, now: int) -> tuple:
        items = [("Q", fr.flow.id, fr.release - now) for fr in self.queued_frames()]
        if self.tx is not None:
            items.append(("T", self.tx.frame.flow.id, self.tx.start - now, self.tx.frame.length))
        return tuple(sorted(items))

    def settle(self, now: int) -> None:
        """入账到达帧；完成恰在 now 结束的发送。"""
        self.enqueue_due(now)
        if self.tx is not None and self.tx.end == now:
            fr = self.tx.frame
            self.records.append(
                _frame_dict(fr, start=self.tx.start, end=self.tx.end, met=now <= fr.deadline)
            )
            self.tx = None

    def choose(self, now: int) -> Optional[tuple[int, Frame]]:
        """严格优先级：在“关门前能完整发完”的队首帧中取最高优先级。"""
        allowed = gate_set_at(now, self.cycle, self.bounds, self.sets)
        for p in range(7, -1, -1):
            if not self.queues[p]:
                continue
            head = self.queues[p][0]
            if p not in allowed:
                continue
            close = close_time(now, p, self.cycle, self.bounds, self.sets)
            if close == INF or close - now >= head.length:
                return p, head
        return None

    def start_due(self, now: int) -> None:
        if self.tx is None:
            pick = self.choose(now)
            if pick is not None:
                p, fr = pick
                self.queues[p].popleft()
                self.tx = Tx(fr, now, now + fr.length)

    def advance(self, now: int, cap: Optional[float] = None, record_slot: bool = True) -> int:
        candidates = [next_gate_change(now, self.cycle, self.bounds), self.next_release()]
        if self.tx is not None:
            candidates.append(self.tx.end)
        seg_end = min(candidates)
        if cap is not None:
            seg_end = min(seg_end, cap)
        seg_end = int(seg_end)
        if record_slot:
            self.slots.append(
                {
                    "t0": now,
                    "t1": seg_end,
                    "gate": sorted(gate_set_at(now, self.cycle, self.bounds, self.sets)),
                    "queues": [_frame_dict(fr) for fr in self.queued_frames()],
                    "transmission": (
                        {
                            "flow": self.tx.frame.flow.id,
                            "instance": self.tx.frame.idx,
                            "priority": self.tx.frame.priority,
                            "release": self.tx.frame.release,
                            "deadline": self.tx.frame.deadline,
                            "start": self.tx.start,
                            "end": self.tx.end,
                        }
                        if self.tx
                        else None
                    ),
                }
            )
        return seg_end


def simulate(flows: list[Flow], gates: list[Gate], cycle: int) -> dict:
    bounds, sets = _windows(cycle, gates)
    hyper = cycle
    for f in flows:
        hyper = lcm(hyper, f.period)

    # ---------- 阶段一：超周期边界收敛性分析 ----------
    m = _Machine(flows, cycle, bounds, sets)
    t = 0
    m.settle(0)
    boundary_samples = [m.snapshot(0)]
    signatures = [m.signature(0)]
    repeat: Optional[tuple[int, int]] = None
    empty_at: Optional[int] = None
    growth_at: Optional[int] = None
    boundary_no = 0
    events = 0

    def on_boundary(now: int) -> bool:
        nonlocal repeat, empty_at, growth_at, boundary_no
        boundary_no += 1
        boundary_samples.append(m.snapshot(now))
        sig = m.signature(now)
        for a, prev in enumerate(signatures):
            if prev == sig:
                repeat = (a, boundary_no)
                return True
        signatures.append(sig)
        if sig == ():
            empty_at = boundary_no
            return True
        if len(boundary_samples) >= GROWTH_RUN + 1:
            tail = boundary_samples[-(GROWTH_RUN + 1) :]
            if all(tail[i + 1]["outstanding_length"] > tail[i]["outstanding_length"]
                   for i in range(GROWTH_RUN)):
                growth_at = boundary_no
                return True
        return False

    while True:
        events += 1
        if events > EVENT_GUARD or boundary_no >= MAX_HYPERPERIODS:
            raise EngineLimit("超过超周期上限仍未收敛")
        m.start_due(t)
        t = m.advance(t)
        m.settle(t)  # 入账本时刻释放的帧与完成的发送
        if t % hyper == 0:
            if on_boundary(t):
                break

    if growth_at is not None:
        return {
            "status": REJECTED_QUEUE_GROWTH,
            "hyperperiod_us": hyper,
            "convergence": {
                "mode": "QUEUE_GROWTH",
                "boundary_samples": boundary_samples,
            },
            "growth": {
                "samples": boundary_samples[-(GROWTH_RUN + 1) :],
                "message": "连续超周期边界队列工作量严格增长，周期边界无法收敛，拒绝裁决（未截断模拟）",
            },
            "timeline": m.slots,
            "records": m.records,
            "carried_frames": _carried(m.records, cycle),
        }

    # ---------- 阶段二：重新完整模拟取证（首超期即停；否则取稳态模板） ----------
    a, b = repeat if repeat is not None else (0, empty_at)
    verdict = _collect(flows, cycle, bounds, sets, hyper, a, b)
    verdict["hyperperiod_us"] = hyper
    verdict["convergence"] = {
        "mode": "EMPTY_BOUNDARY" if empty_at is not None else "REPEAT_SIGNATURE",
        "segment_start": 0 if empty_at is not None else a * hyper,
        "segment_end": b * hyper,
        "boundary_samples": boundary_samples,
    }
    return verdict


def _collect(flows, cycle, bounds, sets, hyper, a, b) -> dict:
    """从 t=0 重新模拟：遇到首个超期帧即判 OVERDUE；否则取 [bH,(b+1)H) 稳态模板判 PASS。"""
    m = _Machine(flows, cycle, bounds, sets)
    t = 0
    m.settle(0)
    first_miss: Optional[dict] = None
    events = 0
    template_end = (b + 1) * hyper

    while True:
        events += 1
        if events > EVENT_GUARD:
            raise EngineLimit("取证超过事件上限")
        m.start_due(t)
        t = m.advance(t)
        m.settle(t)

        # 首个超期帧（完成时刻超过截止期）
        if m.records and m.records[-1].get("end") == t and not m.records[-1]["met"]:
            first_miss = m.records[-1]
            break

        # 越过模板末端且发送位空闲：模板前释放的帧全部完成即收证
        if t >= template_end and m.tx is None:
            pending = [fr for fr in m.queued_frames() if fr.release < template_end]
            if not pending:
                break

    if first_miss is not None:
        miss = first_miss
        first_overdue = {k: miss[k] for k in (
            "flow", "instance", "priority", "release", "enqueue", "start", "end",
            "length", "deadline",
        )}
        first_overdue["blockers"] = _blockers(miss, m.slots)
        return {
            "status": OVERDUE,
            "first_overdue": first_overdue,
            "timeline": m.slots,
            "records": m.records,
            "flow_evidence": {},
            "carried_frames": _carried(m.records, cycle),
        }

    # 稳态模板：释放时刻落在 [bH, (b+1)H) 的帧
    template = [
        r for r in m.records
        if b * hyper <= r["release"] < (b + 1) * hyper
    ]
    want = set()
    for f in flows:
        for k in range(b * hyper // f.period, (b + 1) * hyper // f.period):
            want.add((f.id, k))
    have = {(r["flow"], r["instance"]) for r in template}
    if have != want or any(not r["met"] for r in template):
        # 理论上不应发生（边界签名重复保证稳态），显式拒绝而非误判通过
        raise EngineLimit("稳态模板帧取证不完整，拒绝给出通过裁决")

    flow_evidence: dict[str, list] = {f.id: [] for f in flows}
    for r in sorted(template, key=lambda x: (x["release"], x["flow"])):
        flow_evidence[r["flow"]].append(
            {k: r[k] for k in (
                "instance", "release", "enqueue", "start", "end", "length", "deadline", "met"
            )}
        )
    return {
        "status": PASS,
        "flow_evidence": flow_evidence,
        "first_overdue": None,
        "timeline": m.slots,
        "records": m.records,
        "carried_frames": _carried(m.records, cycle),
    }


def _carried(records: list[dict], cycle: int) -> list[tuple]:
    """释放后等待跨过门控周期边界才开始发送的帧。"""
    carried = set()
    for rec in records:
        if "start" not in rec:
            continue
        first_b = (rec["release"] // cycle + 1) * cycle
        if first_b <= rec["start"]:
            carried.add((rec["flow"], rec["instance"]))
    return sorted(carried)


def _blockers(rec: dict, slots: list[dict]) -> list[dict]:
    """聚合首个超期帧从释放到开始发送之间的阻塞来源。"""
    lo, hi = rec["release"], rec["start"]
    if lo == hi:
        return []
    raw = []
    for s in slots:
        a, b = max(s["t0"], lo), min(s["t1"], hi)
        if a >= b:
            continue
        tr = s["transmission"]
        if tr is not None:
            if tr["priority"] > rec["priority"]:
                reason = "higher_priority"
            elif tr["priority"] < rec["priority"]:
                reason = "lower_priority_nonpreemptive"
            else:
                reason = "same_priority_fifo"
            raw.append((reason, tr["flow"], tr["instance"], a, b))
        elif rec["priority"] not in s["gate"]:
            raw.append(("gate_closed", None, None, a, b))
        else:
            raw.append(("window_too_short", None, None, a, b))

    merged: list[dict] = []
    for reason, fid, inst, a, b in raw:
        if merged and merged[-1]["reason"] == reason and merged[-1].get("flow") == fid \
                and merged[-1].get("instance") == inst and merged[-1]["to"] == a:
            merged[-1]["to"] = b
            merged[-1]["duration"] = b - merged[-1]["from"]
        else:
            merged.append({
                "reason": reason,
                "flow": fid,
                "instance": inst,
                "from": a,
                "to": b,
                "duration": b - a,
            })
    return merged
