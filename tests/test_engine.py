"""调度引擎语义测试。"""
from __future__ import annotations

import pytest

from app import engine as E


def sim(cycle, flows, gates):
    return E.simulate(
        [E.Flow(*f) for f in flows],
        [E.Gate(t, frozenset(p)) for t, p in gates],
        cycle,
    )


OPEN = [(0, {0, 1, 2, 3, 4, 5, 6, 7})]


def test_basic_pass_with_periodic_evidence():
    r = sim(1000, [("F", 3, 1000, 100, 1000)], OPEN)
    assert r["status"] == E.PASS
    ev = r["flow_evidence"]["F"]
    assert ev and all(e["met"] for e in ev)
    # 模板内每帧释放即入队，且完成不晚于截止期
    for e in ev:
        assert e["enqueue"] == e["release"]
        assert e["end"] <= e["deadline"]
        assert e["end"] - e["start"] == e["length"]


def test_strict_priority_blocks_lower_frame():
    # H 每 500us 发 300us；L 帧 [0,300) 等 H，[300,500) 发 200us，500 又被 H 抢占发送位
    r = sim(1000, [("H", 6, 500, 300, 500), ("L", 1, 1000, 300, 500)], OPEN)
    assert r["status"] == E.OVERDUE
    o = r["first_overdue"]
    assert (o["flow"], o["instance"]) == ("L", 0)
    assert (o["release"], o["start"], o["end"]) == (0, 300, 600)
    assert o["end"] > o["deadline"] == 500
    assert any(b["reason"] == "higher_priority" and b["flow"] == "H" for b in o["blockers"])


def test_nonpreemptive_lower_frame_blocks_higher():
    # L#1 在 1000 开始发 1800us；H#1 1000 释放，无法抢占，1900 才发，截止期 1150
    r = sim(1000, [("H", 6, 1000, 100, 150), ("L", 1, 2000, 1800, 2000)], OPEN)
    assert r["status"] == E.OVERDUE
    o = r["first_overdue"]
    assert (o["flow"], o["instance"]) == ("H", 1)
    assert o["start"] == 1900
    assert any(b["reason"] == "lower_priority_nonpreemptive" and b["flow"] == "L"
               for b in o["blockers"])


def test_gate_only_allows_listed_priorities():
    # 仅放行 p6；p1 帧永远无法开始
    r = sim(1000, [("L", 1, 1000, 100, 3000)], [(0, {6, 7})])
    assert r["status"] == E.REJECTED_QUEUE_GROWTH
    for s in r["timeline"]:
        assert s["transmission"] is None
        assert 1 not in s["gate"]


def test_window_too_short_frame_keeps_waiting_then_overdue():
    r = sim(
        1000,
        [("L", 1, 2000, 500, 800)],
        [(0, {0, 1}), (300, set()), (400, {0, 1})],
    )
    assert r["status"] == E.OVERDUE
    o = r["first_overdue"]
    assert o["start"] == 400 and o["end"] == 900
    reasons = [b["reason"] for b in o["blockers"]]
    assert reasons[0] == "window_too_short"
    assert "gate_closed" in reasons


def test_cross_cycle_carryover_meets_deadline():
    # p1 每周期仅前 500us 放行，流周期 1900 -> 部分帧关门外释放、跨周期等待
    r = sim(1000, [("L", 1, 1900, 200, 1900)],
            [(0, {1}), (500, set())])
    assert r["status"] == E.PASS
    assert r["carried_frames"]  # 存在跨门控周期遗留帧
    ev = r["flow_evidence"]["L"]
    # 模板证据中含“释放后等到下一周期”的帧：start >= release 后首个周期边界
    waited = [e for e in ev if e["start"] >= ((e["release"] // 1000) + 1) * 1000]
    assert waited and all(e["met"] for e in ev)


def test_new_release_does_not_overwrite_pending_frame():
    # 每周期释放但门永不开：队列逐帧累积，同流多帧并存
    r = sim(1000, [("F", 1, 1000, 100, 100000)], [(0, set())])
    assert r["status"] == E.REJECTED_QUEUE_GROWTH
    samples = r["growth"]["samples"]
    counts = [s["queued_count"] for s in samples]
    assert counts == sorted(counts) and counts[-1] > counts[0]
    # 队列中帧实例号各不相同 => 新帧追加而非覆盖
    last_queues = samples[-1]["queues"]
    instances = [q["instance"] for q in last_queues]
    assert len(instances) == len(set(instances))


def test_queue_growth_rejected_not_truncated_pass():
    # 过载：两条 p6 流合计 1200us/500us 需求，出口 1000us/周期
    r = sim(1000, [("A", 6, 500, 300, 500), ("B", 6, 500, 300, 500)], [(0, {6, 7})])
    assert r["status"] == E.REJECTED_QUEUE_GROWTH
    samples = r["growth"]["samples"]
    assert len(samples) == E.GROWTH_RUN + 1
    outstanding = [s["outstanding_length"] for s in samples]
    queued = [s["queued_length"] for s in samples]
    # 未完成总工作量（含发送中剩余）连续严格增长
    assert all(outstanding[i + 1] > outstanding[i] for i in range(E.GROWTH_RUN))
    # 仅统计队列长度会有平台期（积压藏在发送位中），证明必须计入发送中剩余
    assert any(queued[i + 1] <= queued[i] for i in range(len(queued) - 1))


def test_timeline_is_partitioned_and_slots_carried_state():
    r = sim(1000, [("F", 3, 1000, 100, 1000)], OPEN)
    tl = r["timeline"]
    for a, b in zip(tl, tl[1:]):
        assert a["t1"] == b["t0"]
    for s in tl:
        assert s["t1"] > s["t0"]
        assert isinstance(s["gate"], list)
        assert "queues" in s and "transmission" in s


def test_eight_flow_limit_shape_handled_at_model_layer():
    # 引擎本身支持多流；构造 8 条可调度流
    flows = [(f"F{p}", p, 8000, 10, 8000) for p in range(8)]
    r = sim(1000, flows, OPEN)
    assert r["status"] == E.PASS
    assert len(r["flow_evidence"]) == 8
