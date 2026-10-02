#!/usr/bin/env python3
"""并行回填脚本 —— 专为 GitHub Actions 海外 runner 设计。

为什么要这个脚本
----------------
仓库自带的 `main.py --backfill` 是**单线程串行**拉取。实测在 GitHub
runner（美国）上约 14 只/分钟，全市场 5224 只需要 5~6 小时，正好撞上
GitHub 单 job 6 小时硬上限，且超时被取消时 cache 保存步骤会被跳过，
进度全部丢失。

本脚本的三项改进：
1. **多进程并行**（默认 8 workers，每个 worker 独立 login baostock），
   把吞吐提升约 6~8 倍。
2. **缩短起始日期**：策略最大历史需求是 RPS 的 120 根 K 线，
   START_DATE 设为约 8 个月前即可，数据量比默认 2024-01-01 少 4 倍。
3. **分块落盘 + 时间预算**：每 25 只一块，完成即写入 SQLite 并打印进度；
   到达 TIME_BUDGET_MIN 主动优雅退出，保证 Actions 的 post cache 步骤
   能正常执行，下次重跑自动续传。

环境变量
--------
DB_PATH           SQLite 路径，默认 data/sequoia_v2.db
START_DATE        回填起始日，默认 2026-02-01（覆盖 RPS 的 120 根需求）
BACKFILL_WORKERS  并行进程数，默认 8
BACKFILL_CHUNK    每块股票数，默认 25
BACKFILL_LIMIT    只处理前 N 只，0 表示不限（调试用）
TIME_BUDGET_MIN   时间预算（分钟），默认 45，到点优雅退出
"""

import os
import sqlite3
import sys
import time
from datetime import date
from multiprocessing import Pool

DB_PATH = os.getenv("DB_PATH", "data/sequoia_v2.db")
START_DATE = os.getenv("START_DATE", "2026-02-01")
WORKERS = int(os.getenv("BACKFILL_WORKERS", "8"))
CHUNK = int(os.getenv("BACKFILL_CHUNK", "25"))
LIMIT = int(os.getenv("BACKFILL_LIMIT", "0"))
TIME_BUDGET_MIN = float(os.getenv("TIME_BUDGET_MIN", "45"))

FIELDS = "date,open,high,low,close,volume,amount"
RAW_COLS = ["symbol"] + FIELDS.split(",")
NUM_COLS = ["open", "high", "low", "close", "volume", "turnover"]
FINAL_COLS = ["symbol", "date", "open", "high", "low", "close", "volume", "turnover"]

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS stock_daily (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol   TEXT    NOT NULL,
    date     TEXT    NOT NULL,
    open     REAL,
    high     REAL,
    low      REAL,
    close    REAL,
    volume   REAL,
    turnover REAL,
    UNIQUE (symbol, date)
);
"""


def log(msg: str) -> None:
    print(f"[fast_backfill] {msg}", flush=True)


def to_bs_code(symbol: str) -> str:
    """纯数字代码 -> baostock 格式。"""
    prefix = "sh" if symbol.startswith(("6", "9")) else "sz"
    return f"{prefix}.{symbol}"


def init_db() -> None:
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(_CREATE_TABLE)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_symbol_date ON stock_daily (symbol, date)"
        )
        conn.commit()


def get_all_symbols() -> list[str]:
    import baostock as bs

    lg = bs.login()
    if lg.error_code != "0":
        log(f"ERROR baostock 登录失败: {lg.error_msg}")
        return []
    try:
        rs = bs.query_stock_basic(code_name="", code="")
        out: list[str] = []
        while rs.next():
            row = rs.get_row_data()
            if row[4] == "1" and row[5] == "1":  # 上市 + 股票
                out.append(row[0].split(".")[1])
        log(f"获取股票列表完成，共 {len(out)} 只")
        return out
    finally:
        bs.logout()


def get_done_symbols() -> set[str]:
    """已拉到今天的股票视为完成（用于续传跳过）。"""
    if not os.path.exists(DB_PATH):
        return set()
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            "SELECT symbol, MAX(date) FROM stock_daily GROUP BY symbol"
        ).fetchall()
    today = date.today().isoformat()
    return {s for s, d in rows if d and d >= today}


def fetch_chunk(args) -> list[list]:
    """worker：独立 login，拉取一批股票，返回原始行。"""
    import baostock as bs

    symbols, start, end = args
    bs.login()
    out: list[list] = []
    try:
        for symbol in symbols:
            rows: list[list] = []
            for attempt in range(2):
                try:
                    rs = bs.query_history_k_data_plus(
                        to_bs_code(symbol),
                        FIELDS,
                        start_date=start,
                        end_date=end,
                        frequency="d",
                        adjustflag="1",  # 后复权
                    )
                    if rs.error_code != "0":
                        raise RuntimeError(rs.error_msg)
                    rows = []
                    while rs.next():
                        rows.append(rs.get_row_data())
                    break
                except Exception:
                    if attempt == 0:
                        time.sleep(1)
                        try:
                            bs.logout()
                        except Exception:
                            pass
                        bs.login()
            out.extend([symbol] + r for r in rows)
    finally:
        try:
            bs.logout()
        except Exception:
            pass
    return out


def write_rows(rows: list[list]) -> int:
    """主进程写库，避免 SQLite 并发写冲突。"""
    if not rows:
        return 0
    import pandas as pd

    df = pd.DataFrame(rows, columns=RAW_COLS)
    df = df.rename(columns={"amount": "turnover"})
    for c in NUM_COLS:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["close"])
    df = df[df["volume"] > 0]
    if df.empty:
        return 0
    df = df[FINAL_COLS]
    with sqlite3.connect(DB_PATH) as conn:
        conn.executemany(
            "INSERT OR IGNORE INTO stock_daily "
            "(symbol,date,open,high,low,close,volume,turnover) VALUES (?,?,?,?,?,?,?,?)",
            list(df.itertuples(index=False, name=None)),
        )
        conn.commit()
    return len(df)


def main() -> int:
    t0 = time.time()
    end = date.today().isoformat()
    init_db()

    symbols = get_all_symbols()
    if not symbols:
        log("ERROR 未获取到股票列表")
        return 1

    done = get_done_symbols()
    todo = [s for s in symbols if s not in done]
    if LIMIT:
        todo = todo[:LIMIT]

    log(
        f"全市场 {len(symbols)} 只 | 已完成 {len(done)} | 本次待处理 {len(todo)} "
        f"| workers={WORKERS} start={START_DATE}"
    )
    if not todo:
        log("全部已完成，无需回填")
        return 0

    chunks = [(todo[i:i + CHUNK], START_DATE, end) for i in range(0, len(todo), CHUNK)]
    total_rows = 0
    done_chunks = 0
    hit_budget = False

    with Pool(WORKERS) as pool:
        for batch in pool.imap_unordered(fetch_chunk, chunks):
            total_rows += write_rows(batch)
            done_chunks += 1
            processed = min(done_chunks * CHUNK, len(todo))
            elapsed_min = (time.time() - t0) / 60
            speed = processed / elapsed_min if elapsed_min > 0 else 0
            log(
                f"进度 {processed}/{len(todo)} ({processed * 100 // len(todo)}%) "
                f"| 累计写入 {total_rows} 行 | 用时 {elapsed_min:.1f} min "
                f"| {speed:.0f} 只/min"
            )
            if elapsed_min >= TIME_BUDGET_MIN:
                log(f"达到时间预算 {TIME_BUDGET_MIN} min，优雅退出（进度已落盘，可重跑续传）")
                hit_budget = True
                pool.terminate()
                break

    elapsed_min = (time.time() - t0) / 60
    log(f"结束：写入 {total_rows} 行，用时 {elapsed_min:.1f} min，budget_hit={hit_budget}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
