"""一次性验收（执行后以退出码报告）：

1. 构建检查：字节码编译全部源码与测试；
2. 代码测试：运行 pytest；
3. HTTP 冒烟：对真实运行的审计接口验证
   a. 跨门控周期遗留帧的收敛/按期证据，以及冻结读取的幂等与冲突保持原裁决；
   b. 队列连续增长的拒绝场景（不截断为通过）。

用法：
  python verify.py                # 自行拉起 uvicorn 后冒烟
  BASE_URL=http://host:port python verify.py   # 对已运行服务冒烟（Compose 用法）
"""
from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

PASS_MARK = "\033[92m✓\033[0m"
FAIL_MARK = "\033[91m✗\033[0m"


def section(title: str) -> None:
    print(f"\n\033[1m=== {title} ===\033[0m", flush=True)


def fail(msg: str) -> None:
    print(f"{FAIL_MARK} {msg}", flush=True)
    raise SystemExit(1)


def step_build() -> None:
    section("1/3 构建检查：compileall")
    rc = subprocess.call(
        [sys.executable, "-m", "compileall", "-q", "app", "tests", "verify.py"],
        cwd=ROOT,
    )
    if rc != 0:
        fail("字节码编译失败")
    print(f"{PASS_MARK} 编译通过")


def step_tests() -> None:
    section("2/3 代码测试：pytest")
    rc = subprocess.call([sys.executable, "-m", "pytest", "tests", "-q"], cwd=ROOT)
    if rc != 0:
        fail("代码测试失败")
    print(f"{PASS_MARK} 全部测试通过")


def wait_port(host: str, port: int, timeout: float = 20.0) -> None:
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if s.connect_ex((host, port)) == 0:
                return
        time.sleep(0.2)
    fail(f"等待 {host}:{port} 超时")


def start_server():
    if os.environ.get("BASE_URL"):
        return None, os.environ["BASE_URL"].rstrip("/")
    port = int(os.environ.get("VERIFY_PORT", "8123"))
    store = ROOT / ".verify-data" / "verdicts.json"
    store.parent.mkdir(exist_ok=True)
    store.unlink(missing_ok=True)
    env = dict(os.environ, VERDICT_STORE=str(store))
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.main:app",
         "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning"],
        cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )
    try:
        wait_port("127.0.0.1", port)
    except Exception:
        out = proc.stdout.read().decode() if proc.stdout else ""
        proc.kill()
        print(out)
        raise
    return proc, f"http://127.0.0.1:{port}"


CARRY_PAYLOAD = {
    # p1 每门控周期仅前 500us 放行；流周期 1900us =>
    # 部分帧在关门后释放，必须跨门控周期遗留等待，且仍按期发送
    "audit_id": "SMOKE-CARRY-001",
    "cycle_us": 1000,
    "flows": [{"id": "L", "priority": 1, "period_us": 1900,
               "length_us": 200, "deadline_us": 1900}],
    "gates": [{"time_us": 0, "priorities": [1]},
              {"time_us": 500, "priorities": []}],
}

GROWTH_PAYLOAD = {
    "audit_id": "SMOKE-GROWTH-001",
    "cycle_us": 1000,
    "flows": [
        {"id": "A", "priority": 6, "period_us": 500, "length_us": 300, "deadline_us": 500},
        {"id": "B", "priority": 6, "period_us": 500, "length_us": 300, "deadline_us": 500},
    ],
    "gates": [{"time_us": 0, "priorities": [6, 7]}],
}


def check(cond: bool, msg: str) -> None:
    if not cond:
        fail(msg)
    print(f"{PASS_MARK} {msg}")


def step_smoke(base: str) -> None:
    section(f"3/3 HTTP 冒烟：{base}")
    import httpx

    with httpx.Client(base_url=base, timeout=10) as cli:
        r = cli.get("/api/health")
        check(r.status_code == 200 and r.json()["status"] == "ok", "健康检查 200")

        # ---- 场景 A：遗留帧收敛证据 ----
        r = cli.post("/api/submit", json=CARRY_PAYLOAD)
        check(r.status_code == 200, "遗留帧场景提交 200")
        body = r.json()
        v = body["verdict"]
        check(body["created"] is True, "首次提交创建冻结裁决")
        check(v["status"] == "PASS", f"裁决为 PASS（实际 {v['status']}）")
        check(v["convergence"]["mode"] in ("REPEAT_SIGNATURE", "EMPTY_BOUNDARY"),
              f"给出周期收敛依据（{v['convergence']['mode']}）")
        check(len(v.get("carried_frames", [])) > 0,
              f"存在跨门控周期遗留帧（{len(v.get('carried_frames', []))} 个）")
        ev = v["flow_evidence"]["L"]
        check(bool(ev) and all(e["met"] and e["end"] <= e["deadline"] for e in ev),
              f"稳态模板 {len(ev)} 帧全部按期发送")
        waited = [e for e in ev if e["start"] >= ((e["release"] // 1000) + 1) * 1000]
        check(bool(waited), "模板中含跨周期等待后发送的遗留帧证据")
        tl = v["timeline"]
        check(bool(tl) and all({"t0", "t1", "gate", "queues", "transmission"} <= set(s)
                               for s in tl),
              f"逐时隙队列/门状态/发送结果完备（{len(tl)} 个时隙）")

        # 冻结读取幂等
        got = cli.get(f"/api/verdict/{CARRY_PAYLOAD['audit_id']}")
        check(got.status_code == 200, "读取冻结裁决 200")
        check(got.json()["verdict"] == v, "重复读取返回同一冻结结论")
        again = cli.post("/api/submit", json=CARRY_PAYLOAD)
        check(again.status_code == 200 and again.json()["created"] is False
              and again.json()["verdict"] == v,
              "完全相同提交返回同一冻结结论（created=false）")

        # 同标识不同内容 => 409 且原裁决不变
        conflict = {**CARRY_PAYLOAD, "cycle_us": 2000}
        rc = cli.post("/api/submit", json=conflict)
        check(rc.status_code == 409, "不同内容复用审计标识冲突 409")
        existing = rc.json()["detail"]["existing_verdict"]
        check(existing["verdict"]["status"] == "PASS", "冲突响应携带原裁决")
        kept = cli.get(f"/api/verdict/{CARRY_PAYLOAD['audit_id']}").json()
        check(kept["request"]["cycle_us"] == 1000 and kept["verdict"] == v,
              "冲突后原冻结裁决保持不变")

        # ---- 场景 B：队列增长拒绝 ----
        r = cli.post("/api/submit", json=GROWTH_PAYLOAD)
        check(r.status_code == 200, "过载场景提交 200")
        gv = r.json()["verdict"]
        check(gv["status"] == "REJECTED_QUEUE_GROWTH",
              f"周期边界无法收敛时拒绝裁决（实际 {gv['status']}，非截断通过）")
        samples = gv["growth"]["samples"]
        check(len(samples) >= 4, f"给出连续边界采样（{len(samples)} 个）")
        growing = all(samples[i + 1]["outstanding_length"] > samples[i]["outstanding_length"]
                      for i in range(len(samples) - 1))
        check(growing, "未完成工作量（队列+发送中剩余）连续严格增长")
        check(bool(samples[-1]["queues"]), "增长采样附带逐帧队列证据")
        frozen = cli.get(f"/api/verdict/{GROWTH_PAYLOAD['audit_id']}").json()
        check(frozen["verdict"] == gv, "拒绝结论同样被冻结并可读取")


def main() -> None:
    step_build()
    step_tests()
    proc, base = start_server()
    try:
        step_smoke(base)
    finally:
        if proc is not None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
    section("验收结果")
    print(f"{PASS_MARK} 构建检查、代码测试与 HTTP 冒烟全部通过")
    sys.exit(0)


if __name__ == "__main__":
    main()
