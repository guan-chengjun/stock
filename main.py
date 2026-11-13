# -*- coding: utf-8 -*-
"""
A股看盘工具 —— FastAPI 后端
=============================
封装 zzshare（主数据源，云端 SDK，匿名可用但限流严格），
并以腾讯/新浪公开行情作为容错备用源。

设计要点（防止触发频率限制）：
1. 全内存 TTL 缓存：全市场快照、日线、分钟线、基本面等均带缓存；
2. 单飞（single-flight）：同一时刻相同请求只放一个出站调用；
3. 出站最小间隔：所有外部 HTTP 调用共享节流闸，默认 ≥0.8s 一次；
4. 上游 429 不重试到天荒地老：向客户端返回 503 + Retry-After，前端自动退避。

启动：uvicorn main:app --host 127.0.0.1 --port 8000
文档：http://127.0.0.1:8000/docs
"""
from __future__ import annotations

import asyncio
import logging
import math
import os
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from zzshare import DataApi
from zzshare.core import ApiAuthError, ApiRateLimitError

# --------------------------------------------------------------------------- #
# 全局配置
# --------------------------------------------------------------------------- #
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("stock-api")

ZQS_TOKEN = os.environ.get("ZZSHARE_TOKEN", "")
# zzshare SDK 自身在 429 时会阻塞重试，这里给较短超时，配合我们自己的节流/缓存
api = DataApi(token=ZQS_TOKEN, timeout=8)

# SDK 默认 429 会重试 3 次（每次按 Retry-After 最多等约 60s），
# 本地服务不应长时间挂起：内部重试设为 0，快速失败后由缓存/备用源/前端重试兜底。
_orig_request_with_retry = api._request_with_retry


def _limited_retry(url, params=None, max_retries: int = 0):  # noqa: ANN001
    return _orig_request_with_retry(url, params=params, max_retries=max_retries)


api._request_with_retry = _limited_retry

HTTP_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
UPSTREAM_MIN_GAP = float(os.environ.get("UPSTREAM_MIN_GAP", "0.8"))  # 出站调用最小间隔（秒）

# 缓存 TTL（秒）
TTL_MARKET = 10          # 全市场快照
TTL_SPOT = 8             # 单票报价
TTL_TIMELINE = 15        # 分时
TTL_TRADE = 20           # 逐笔成交
TTL_RANK = 15            # 涨幅榜
TTL_DAILY = 12 * 3600    # 历史日线
TTL_MINUTE = 60          # 分钟K线
TTL_BASIC = 24 * 3600    # 基本面
TTL_SEARCH = 24 * 3600   # 股票列表


class UpstreamError(RuntimeError):
    """上游数据源统一异常。"""

    def __init__(self, message: str, status: int = 502, retry_after: int = 30):
        super().__init__(message)
        self.status = status
        self.retry_after = retry_after


# --------------------------------------------------------------------------- #
# 缓存 + 单飞 + 节流
# --------------------------------------------------------------------------- #
@dataclass
class CacheEntry:
    value: Any
    expire_at: float


class TTLCache:
    def __init__(self, max_size: int = 400):
        self._store: Dict[str, CacheEntry] = {}
        self._max = max_size

    def get(self, key: str) -> Any:
        entry = self._store.get(key)
        if entry is None:
            return None
        if time.time() > entry.expire_at:
            self._store.pop(key, None)
            return None
        return entry.value

    def set(self, key: str, value: Any, ttl: int) -> None:
        if len(self._store) >= self._max:
            # 简单淘汰最早过期的 1/4
            for k in sorted(self._store, key=lambda x: self._store[x].expire_at)[: self._max // 4]:
                self._store.pop(k, None)
        self._store[key] = CacheEntry(value, time.time() + ttl)


cache = TTLCache()
_inflight: Dict[str, asyncio.Future] = {}
_throttle_lock = asyncio.Lock()
_last_call_ts = 0.0


async def _throttle() -> None:
    """全局出站节流：保证相邻外部调用至少间隔 UPSTREAM_MIN_GAP。"""
    global _last_call_ts
    async with _throttle_lock:
        wait = _last_call_ts + UPSTREAM_MIN_GAP - time.time()
        if wait > 0:
            await asyncio.sleep(wait)
        _last_call_ts = time.time()


async def single_flight(key: str, ttl: int, loader):
    """缓存 + 单飞包装。loader 为同步函数，会放到线程池执行。"""
    cached = cache.get(key)
    if cached is not None:
        return cached

    fut = _inflight.get(key)
    if fut is None:
        fut = asyncio.get_event_loop().create_future()
        _inflight[key] = fut
        try:
            await _throttle()
            result = await asyncio.to_thread(loader)
            cache.set(key, result, ttl)
            fut.set_result(result)
        except Exception as exc:  # 放行异常，等待者一起收到
            fut.set_exception(exc)
            raise
        finally:
            _inflight.pop(key, None)
        return result
    return await fut


# --------------------------------------------------------------------------- #
# 工具函数
# --------------------------------------------------------------------------- #
def to_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        if isinstance(v, float) and math.isnan(v):
            return None
        f = float(v)
        if math.isnan(f):
            return None
        return f
    except (TypeError, ValueError):
        return None


def _to_lot(v: Any) -> Optional[float]:
    """股 -> 手（1 手 = 100 股）。"""
    f = to_float(v)
    return round(f / 100.0, 2) if f is not None else None


def normalize_code(code: str) -> str:
    """接受 000001 / sz000001 / 000001.SZ，统一返回 6 位代码。"""
    c = code.strip().upper()
    if "." in c:
        c = c.split(".")[0]
    if c.startswith(("SH", "SZ", "BJ")):
        c = c[2:]
    return c.zfill(6) if c.isdigit() else c


def to_ts_code(code: str) -> str:
    c = normalize_code(code)
    if c.startswith(("6", "5")):
        return f"{c}.SH"
    if c.startswith(("0", "3")):
        return f"{c}.SZ"
    return f"{c}.BJ"


def to_qt_symbol(code: str) -> str:
    c = normalize_code(code)
    if c.startswith(("6", "5")):
        return "sh" + c
    if c.startswith(("0", "3")):
        return "sz" + c
    return "bj" + c


def df_records(df: pd.DataFrame) -> List[Dict[str, Any]]:
    if df is None or df.empty:
        return []
    return json_safe(df.to_dict(orient="records"))


def json_safe(obj: Any) -> Any:
    """把 numpy/pandas/NaN 转成 JSON 友好的原生类型。"""
    if isinstance(obj, dict):
        return {k: json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [json_safe(v) for v in obj]
    if isinstance(obj, (pd.Timestamp, datetime)):
        return obj.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(obj, float):
        return None if math.isnan(obj) else obj
    if hasattr(obj, "item"):  # numpy scalar
        try:
            return json_safe(obj.item())
        except Exception:
            return str(obj)
    return obj


def run_zqs(fn, *args, **kwargs):
    """执行 zzshare 调用，把限流/鉴权异常转成统一 UpstreamError。"""
    try:
        return fn(*args, **kwargs)
    except ApiRateLimitError as e:
        logger.warning("zzshare 429: %s", e)
        raise UpstreamError("数据接口触发频率限制，请稍后再试（已自动缓存，请耐心等待退避）", 503, 30)
    except ApiAuthError as e:
        logger.error("zzshare 401: %s", e)
        raise UpstreamError("数据接口鉴权失败，可配置 ZZSHARE_TOKEN 环境变量后重启", 502, 60)
    except Exception as e:  # noqa: BLE001
        logger.warning("zzshare 调用失败: %r", e)
        raise UpstreamError(f"数据接口调用失败: {e}", 502, 15)


# --------------------------------------------------------------------------- #
# 备用数据源：腾讯行情 / 腾讯逐笔
# --------------------------------------------------------------------------- #
def http_get(url: str, timeout: int = 8, encoding: Optional[str] = None) -> str:
    r = requests.get(url, headers=HTTP_HEADERS, timeout=timeout)
    if encoding:
        r.encoding = encoding
    r.raise_for_status()
    return r.text


def tencent_quote(codes: List[str]) -> Dict[str, Dict[str, Any]]:
    """腾讯 qt.gtimg.cn 批量快照，含五档。"""
    symbols = ",".join(to_qt_symbol(c) for c in codes)
    text = http_get(f"https://qt.gtimg.cn/q={symbols}", encoding="gbk")
    out: Dict[str, Dict[str, Any]] = {}
    for line in text.strip().split(";"):
        line = line.strip()
        if not line or "=" not in line or '="' not in line:
            continue
        var_name, payload = line.split("=", 1)
        symbol = var_name.replace("v_", "").strip()
        code = symbol[2:] if symbol[:2] in ("sh", "sz", "bj") else symbol
        parts = payload.strip('"').split("~")
        if len(parts) < 50:
            continue
        try:
            pre_close, price, open_px = to_float(parts[4]), to_float(parts[3]), to_float(parts[5])
            change, pct = to_float(parts[31]), to_float(parts[32])
            if change is None and price is not None and pre_close:
                change = round(price - pre_close, 4)
            if pct is None and pre_close:
                pct = round((price - pre_close) / pre_close * 100, 3)
            # 五档：parts[9..28]，每档 价/量 两个字段，先买 1-5 后卖 1-5
            bids, asks = [], []
            for i in range(5):
                bids.append({"price": to_float(parts[9 + i * 2]), "volume": to_float(parts[10 + i * 2])})
            for i in range(5):
                asks.append({"price": to_float(parts[19 + i * 2]), "volume": to_float(parts[20 + i * 2])})
            out[code] = {
                "code": code,
                "ts_code": to_ts_code(code),
                "name": parts[1],
                "price": price,
                "pre_close": pre_close,
                "open": open_px,
                "high": to_float(parts[33]),
                "low": to_float(parts[34]),
                "change": change,
                "pct_chg": pct,
                "volume": to_float(parts[36]),       # 手
                "amount": (to_float(parts[37]) or 0) * 10000 or None,  # 万元 -> 元
                "turnover_rate": to_float(parts[38]) if len(parts) > 38 else None,
                "pe": to_float(parts[39]) if len(parts) > 39 else None,
                "high_limit": to_float(parts[47]) if len(parts) > 47 else None,
                "low_limit": to_float(parts[48]) if len(parts) > 48 else None,
                "market_value": (to_float(parts[45]) or 0) * 1e8 if len(parts) > 45 and to_float(parts[45]) else None,
                "circulation_value": (to_float(parts[44]) or 0) * 1e8 if len(parts) > 44 and to_float(parts[44]) else None,
                "bids": bids,
                "asks": asks,
                "source": "tencent",
                "time": parts[30],
            }
        except Exception as e:  # noqa: BLE001
            logger.warning("腾讯快照解析失败 %s: %r", code, e)
    return out


def tencent_timeline(code: str) -> Dict[str, Any]:
    """腾讯分时 minute/query：每分钟 价格/累计成交量(手)/累计成交额(元)。"""
    symbol = to_qt_symbol(code)
    text = http_get(f"https://web.ifzq.gtimg.cn/appstock/app/minute/query?code={symbol}")
    import json as _json
    data = _json.loads(text)
    outer = data.get("data", {}).get(symbol, {})
    node = outer.get("data", {})
    rows_raw = node.get("data", [])
    # 昨收价在 qt 快照数组索引 [4]（与 qt.gtimg.cn 同结构）
    pre_close = to_float(node.get("prec")) or to_float(node.get("preClose"))
    if pre_close is None:
        qt_arr = outer.get("qt", {}).get(symbol)
        if isinstance(qt_arr, list) and len(qt_arr) > 4:
            pre_close = to_float(qt_arr[4])
    points, prev_cum_vol, prev_cum_amt = [], 0.0, 0.0
    for row in rows_raw:
        seg = row.split()
        if len(seg) < 4:
            continue
        t, px, cum_vol, cum_amt = seg[0], to_float(seg[1]), to_float(seg[2]) or 0.0, to_float(seg[3]) or 0.0
        minute_vol = max(cum_vol - prev_cum_vol, 0.0)
        minute_amt = max(cum_amt - prev_cum_amt, 0.0)
        prev_cum_vol, prev_cum_amt = cum_vol, cum_amt
        avg = round(cum_amt / (cum_vol * 100), 4) if cum_vol > 0 else px
        points.append({
            "time": f"{t[:2]}:{t[2:]}",
            "price": px,
            "avg_price": avg,
            "volume": minute_vol,           # 手
            "amount": minute_amt,           # 元
            "pct_chg": round((px - pre_close) / pre_close * 100, 3) if pre_close else None,
        })
    return {"code": normalize_code(code), "pre_close": pre_close, "points": points, "source": "tencent"}


def tencent_trades(code: str) -> Dict[str, Any]:
    """腾讯 getMingxi 逐笔：序号/时间/价格/涨跌/手数/成交额/B|S|M。"""
    symbol = to_qt_symbol(code)
    text = http_get(f"https://web.ifzq.gtimg.cn/appstock/app/dealinfo/getMingxi?code={symbol}")
    import json as _json
    data = _json.loads(text)
    raw = data.get("data", {}).get("data", "")
    trades: List[Dict[str, Any]] = []
    for item in str(raw).split("|"):
        seg = item.split("/")
        if len(seg) < 7:
            continue
        side_map = {"B": "买盘", "S": "卖盘", "M": "中性盘"}
        trades.append({
            "index": to_float(seg[0]),
            "time": seg[1],
            "price": to_float(seg[2]),
            "change": to_float(seg[3]),
            "volume": to_float(seg[4]),     # 手
            "amount": to_float(seg[5]),     # 元
            "side": side_map.get(seg[6], seg[6]),
        })
    trades.reverse()  # 接口默认倒序，转为时间正序
    return {"code": normalize_code(code), "trades": trades[:300], "source": "tencent"}


# --------------------------------------------------------------------------- #
# zzshare 数据加载器（同步，在线程池执行）
# --------------------------------------------------------------------------- #
_stock_list_cache: Dict[str, Any] = {"rows": [], "loaded_at": 0.0}


def load_stock_list() -> List[Dict[str, str]]:
    if _stock_list_cache["rows"] and time.time() - _stock_list_cache["loaded_at"] < TTL_SEARCH:
        return _stock_list_cache["rows"]
    df = run_zqs(api.stock_basic, fields="ts_code,symbol,name,exchange,list_status")
    rows = []
    for r in df_records(df):
        rows.append({
            "code": str(r.get("symbol") or normalize_code(str(r.get("ts_code", "")))),
            "ts_code": r.get("ts_code"),
            "name": r.get("name"),
            "exchange": r.get("exchange"),
        })
    _stock_list_cache["rows"] = rows
    _stock_list_cache["loaded_at"] = time.time()
    return rows


def load_market_snapshot() -> List[Dict[str, Any]]:
    df = run_zqs(api.rt_k, ts_code="", fields="all")
    if df is None or df.empty:
        raise UpstreamError("全市场快照为空（可能在限流中）", 503, 20)
    rows: List[Dict[str, Any]] = []
    for r in df_records(df):
        code = normalize_code(str(r.get("ts_code", "")))
        bids, asks = parse_levels(r.get("bid_grp")), parse_levels(r.get("offer_grp"))
        rows.append({
            "code": code,
            "ts_code": r.get("ts_code"),
            "name": r.get("name"),
            "price": to_float(r.get("close")),
            "pre_close": to_float(r.get("pre_close")),
            "open": to_float(r.get("open")),
            "high": to_float(r.get("high")),
            "low": to_float(r.get("low")),
            "pct_chg": to_float(r.get("quote_rate")),
            "change": (round(to_float(r.get("close")) - to_float(r.get("pre_close")), 4)
                       if to_float(r.get("close")) is not None and to_float(r.get("pre_close")) is not None else None),
            "volume": _to_lot(r.get("vol")),        # 统一为手（1手=100股）
            "amount": to_float(r.get("amount")),       # 元
            "turnover_rate": to_float(r.get("turnover_rate")),
            "pe": to_float(r.get("ttm_pe_rate")),
            "high_limit": to_float(r.get("high_limit")),
            "low_limit": to_float(r.get("low_limit")),
            "market_value": to_float(r.get("market_value")),
            "circulation_value": to_float(r.get("circulation_value")),
            "bids": bids,
            "asks": asks,
            "source": "zzshare",
        })
    return rows


def parse_levels(grp: Any) -> List[Dict[str, Any]]:
    """解析 rt_k 的 bid_grp / offer_grp：价,量,0, 价,量,0 ..."""
    levels = []
    if not grp:
        return [{"price": None, "volume": None} for _ in range(5)]
    tokens = [t.strip() for t in str(grp).split(",") if t.strip() != ""]
    for i in range(5):
        idx = i * 3
        if idx + 1 < len(tokens):
            levels.append({"price": to_float(tokens[idx]), "volume": to_float(tokens[idx + 1])})
        else:
            levels.append({"price": None, "volume": None})
    return levels


def load_zqs_spot(code: str) -> Dict[str, Any]:
    df = run_zqs(api.rt_k, ts_code=to_ts_code(code), fields="all")
    if df is None or df.empty:
        raise UpstreamError("该股票暂无实时报价", 502, 15)
    r = df_records(df)[0]
    return {
        "code": normalize_code(code),
        "ts_code": r.get("ts_code"),
        "name": r.get("name"),
        "price": to_float(r.get("close")),
        "pre_close": to_float(r.get("pre_close")),
        "open": to_float(r.get("open")),
        "high": to_float(r.get("high")),
        "low": to_float(r.get("low")),
        "pct_chg": to_float(r.get("quote_rate")),
        "change": (round(to_float(r.get("close")) - to_float(r.get("pre_close")), 4)
                   if to_float(r.get("close")) is not None and to_float(r.get("pre_close")) is not None else None),
        "volume": _to_lot(r.get("vol")),
        "amount": to_float(r.get("amount")),
        "turnover_rate": to_float(r.get("turnover_rate")),
        "pe": to_float(r.get("ttm_pe_rate")),
        "eps": to_float(r.get("eps_ttm")),
        "high_limit": to_float(r.get("high_limit")),
        "low_limit": to_float(r.get("low_limit")),
        "market_value": to_float(r.get("market_value")),
        "circulation_value": to_float(r.get("circulation_value")),
        "bids": parse_levels(r.get("bid_grp")),
        "asks": parse_levels(r.get("offer_grp")),
        "source": "zzshare",
    }


def load_daily(code: str, adjust: str, limit: int) -> List[Dict[str, Any]]:
    df = run_zqs(api.daily, ts_code=to_ts_code(code), limit=limit, adj=adjust)
    if df is None or df.empty:
        raise UpstreamError("日线数据为空", 502, 20)
    records = df_records(df)
    records.reverse()  # SDK 默认日期倒序，前端K线需要正序
    for r in records:
        r["date"] = str(r.get("trade_date", ""))
        r["vol"] = _to_lot(r.get("vol"))  # 股 -> 手
    return records


def load_minute(code: str, freq: str, count: int) -> List[Dict[str, Any]]:
    df = run_zqs(api.stk_mins, ts_code=to_ts_code(code), freq=freq, count=count)
    if df is None or df.empty:
        raise UpstreamError("分钟线数据为空", 502, 20)
    records = df_records(df)
    records.reverse()  # 时间正序
    for r in records:
        tt = str(r.get("trade_time", ""))
        r["datetime"] = f"{tt[:4]}-{tt[4:6]}-{tt[6:8]} {tt[8:10]}:{tt[10:12]}" if len(tt) >= 12 else tt
        r["time"] = f"{tt[8:10]}:{tt[10:12]}" if len(tt) >= 12 else tt
        r["vol"] = _to_lot(r.get("vol"))  # 股 -> 手
    return records


def load_zqs_timeline(code: str) -> Dict[str, Any]:
    from zzshare.utils import trend_data_to_df
    data = run_zqs(api.trend_minute, normalize_code(code))
    if not data:
        raise UpstreamError("分时数据为空", 502, 15)
    df = trend_data_to_df(data)  # 本地转换，不再发第二次请求
    pre_close = to_float(data.get("preclose_px"))
    points = []
    for r in df_records(df):
        points.append({
            "time": str(r.get("time", "")),
            "price": to_float(r.get("price")),
            "avg_price": to_float(r.get("avg_price")),
            "volume": _to_lot(r.get("vol")),  # 股 -> 手
            "amount": to_float(r.get("amount")),
            "pct_chg": to_float(r.get("pct_chg")),
        })
    return {"code": normalize_code(code), "pre_close": pre_close, "points": points, "source": "zzshare"}


def load_basic_akshare(code: str) -> Dict[str, Any]:
    import akshare as ak
    try:
        df = ak.stock_individual_info_em(symbol=normalize_code(code))
    except Exception as e:  # noqa: BLE001
        raise UpstreamError(f"基本面接口暂不可用（东财限流，稍后自动恢复）: {str(e)[:80]}", 503, 40)
    info: Dict[str, Any] = {}
    for r in df_records(df):
        info[str(r.get("item"))] = r.get("value")
    return {
        "code": normalize_code(code),
        "total_market_value": to_float(info.get("总市值")),
        "circulation_value": to_float(info.get("流通市值")),
        "industry": info.get("行业"),
        "listing_date": str(info.get("上市时间", "")) or None,
        "stock_name": info.get("股票简称"),
        "main_business": info.get("主营业务") or "暂无数据",
        "source": "akshare",
    }


# --------------------------------------------------------------------------- #
# FastAPI
# --------------------------------------------------------------------------- #
@asynccontextmanager
async def lifespan(_app: FastAPI):
    # 启动时预热股票列表与全市场快照（失败不影响服务，首次访问时会重试）
    async def _job():
        for key, ttl, loader, label in (
            ("stock_list", TTL_SEARCH, load_stock_list, "股票列表"),
            ("market", TTL_MARKET, load_market_snapshot, "全市场快照"),
        ):
            try:
                await single_flight(key, ttl, loader)
                logger.info("%s预热完成", label)
            except Exception as e:  # noqa: BLE001
                logger.warning("%s预热失败（首次访问时自动重试）: %r", label, e)

    task = asyncio.create_task(_job())
    yield
    task.cancel()


app = FastAPI(title="A股看盘 API（zzshare 封装）", version="1.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 本地单文件页面（file://）需要宽松 CORS
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(UpstreamError)
async def upstream_handler(_request, exc: UpstreamError):
    from fastapi.responses import JSONResponse
    return JSONResponse(
        status_code=exc.status,
        content={"detail": str(exc), "retry_after": exc.retry_after},
        headers={"Retry-After": str(exc.retry_after)},
    )


@app.get("/api/health")
async def health():
    return {"status": "ok", "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "cache_keys": len(cache._store)}


@app.get("/api/search")
async def search(q: str = Query(..., min_length=1, description="代码或名称关键字"),
                 limit: int = Query(20, ge=1, le=50)):
    rows = await single_flight("stock_list", TTL_SEARCH, load_stock_list)
    kw = q.strip().upper()
    matched = [
        r for r in rows
        if kw in str(r["code"]) or kw in str(r["name"]).upper()
    ]
    return {"q": q, "results": matched[:limit], "total": len(matched)}


def is_valid_code(code: str) -> bool:
    c = normalize_code(code)
    return len(c) == 6 and c.isdigit()


def require_code(code: str) -> str:
    code = normalize_code(code)
    if not is_valid_code(code):
        raise HTTPException(status_code=400, detail=f"非法股票代码: {code}")
    return code


@app.get("/api/spot")
async def spot(code: Optional[str] = Query(None, description="股票代码，如 000001"),
               codes: Optional[str] = Query(None, description="批量代码，逗号分隔（自选股用，最多30）")):
    if not code and not codes:
        raise HTTPException(status_code=400, detail="必须提供 code 或 codes 参数")

    if codes:
        wanted = [normalize_code(c) for c in codes.split(",") if c.strip()][:30]
        wanted = [c for c in wanted if is_valid_code(c)]
        if not wanted:
            raise HTTPException(status_code=400, detail="codes 不能为空或格式非法")
        # 优先命中全市场快照缓存
        try:
            market = await single_flight("market", TTL_MARKET, load_market_snapshot)
            idx = {r["code"]: r for r in market}
            missing = [c for c in wanted if c not in idx]
            result = [idx[c] for c in wanted if c in idx]
            if missing:
                try:
                    qt = await single_flight(
                        "qt:" + ",".join(missing), TTL_SPOT,
                        lambda: tencent_quote(missing))
                    result += [qt[c] for c in missing if c in qt]
                except UpstreamError:
                    pass
            return {"items": result, "count": len(result)}
        except UpstreamError:
            qt = await single_flight("qt:" + ",".join(wanted), TTL_SPOT,
                                     lambda: tencent_quote(wanted))
            return {"items": [qt[c] for c in wanted if c in qt], "count": len(qt)}

    code = normalize_code(code)
    if not is_valid_code(code):
        raise HTTPException(status_code=400, detail=f"非法股票代码: {code}")

    # 优先复用全市场快照（一次调用覆盖全部股票，避免逐票打 zzshare 触发限流）
    try:
        market = await single_flight("market", TTL_SPOT, load_market_snapshot)
        row = next((r for r in market if r["code"] == code), None)
        if row:
            return row
    except UpstreamError:
        pass

    # 快照未命中（如北交所/新股）再尝试单票 zzshare
    try:
        return await single_flight(f"spot:{code}", TTL_SPOT, lambda: load_zqs_spot(code))
    except UpstreamError:
        qt = await single_flight(f"qt:{code}", TTL_SPOT, lambda: tencent_quote([code]))
        if code in qt:
            return qt[code]
        raise UpstreamError("所有报价源均不可用，请稍后重试", 503, 30)


@app.get("/api/timeline")
async def timeline(code: str = Query(...)):
    code = require_code(code)
    try:
        return await single_flight(f"tl:{code}", TTL_TIMELINE,
                                   lambda: load_zqs_timeline(code))
    except UpstreamError:
        return await single_flight(f"tlq:{code}", TTL_TIMELINE,
                                   lambda: tencent_timeline(code))


@app.get("/api/daily")
async def daily(code: str = Query(...),
                adjust: str = Query("qfq", pattern="^(qfq|hfq|none)$"),
                limit: int = Query(250, ge=20, le=1000)):
    code = require_code(code)
    return await single_flight(f"daily:{code}:{adjust}:{limit}", TTL_DAILY,
                               lambda: load_daily(code, adjust, limit))


@app.get("/api/minute")
async def minute(code: str = Query(...),
                 freq: str = Query("5min", pattern="^(1min|5min|15min|30min|60min)$"),
                 count: int = Query(240, ge=20, le=1000)):
    code = require_code(code)
    return await single_flight(f"min:{code}:{freq}:{count}", TTL_MINUTE,
                               lambda: load_minute(code, freq, count))


@app.get("/api/rank")
async def rank(sort_by: str = Query("pct_chg", pattern="^(pct_chg|amount|turnover_rate|change)$"),
               order: str = Query("desc", pattern="^(desc|asc)$"),
               limit: int = Query(50, ge=10, le=200)):
    market = await single_flight("market", TTL_RANK, load_market_snapshot)
    rows = [r for r in market if r.get("price") not in (None, 0)]
    rows.sort(key=lambda r: (r.get(sort_by) is None, r.get(sort_by) or 0),
              reverse=(order == "desc"))
    return {"sort_by": sort_by, "order": order, "items": rows[:limit], "count": len(rows)}


@app.get("/api/basic")
async def basic(code: str = Query(...)):
    code = require_code(code)
    info = await single_flight(f"basic:{code}", TTL_BASIC, lambda: load_basic_akshare(code))
    # 补充实时估值（尝试命中市场快照，失败忽略）
    try:
        market = await single_flight("market", TTL_MARKET, load_market_snapshot)
        snap = next((r for r in market if r["code"] == code), None)
        if snap:
            info.setdefault("pe", snap.get("pe"))
            info.setdefault("total_market_value", snap.get("market_value"))
            info.setdefault("circulation_value", snap.get("circulation_value"))
            info.setdefault("stock_name", snap.get("name"))
    except UpstreamError:
        pass
    return info


@app.get("/api/trades")
async def trades(code: str = Query(...)):
    code = require_code(code)
    return await single_flight(f"trades:{code}", TTL_TRADE, lambda: tencent_trades(code))


if __name__ == "__main__":
    import uvicorn
    # 直接传 app 对象，避免 python -I 下字符串形式无法重新导入模块
    uvicorn.run(app, host="127.0.0.1", port=8000)
