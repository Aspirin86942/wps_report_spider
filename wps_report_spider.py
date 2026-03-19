"""WPS 在线表格公告抓取与补数脚本。"""

from __future__ import annotations

import logging
import math
import random
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from typing import Any, Callable, cast
from urllib.parse import quote
from uuid import uuid4

import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag
from requests import Response, Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# =========================
# 配置区
# =========================
BASE_URL: str = "https://www.cninfo.com.cn/new/fulltextSearch/full"
SEARCH_PAGE_URL: str = "https://www.cninfo.com.cn/new/fulltextSearch"
STATIC_BASE_URL: str = "https://static.cninfo.com.cn/"
THS_STOCK_PAGE_URL: str = "https://stockpage.10jqka.com.cn"
THS_OPERATE_PAGE_URL: str = "https://basic.10jqka.com.cn"

SEARCH_KEY: str = "内部控制评价"
START_DATE: str = "2026-01-01"
END_DATE: str = "2026-04-01"
PAGE_SIZE: int = 20
SLEEP_MIN_SECONDS: float = 0.8
SLEEP_MAX_SECONDS: float = 1.6
CONCEPT_SLEEP_SECONDS: float = 0.5
OPERATE_SLEEP_SECONDS: float = 0.5

CONFIG_SHEET: str = "config"
CONCEPT_CACHE_SHEET: str = "_cache_concept"
OPERATE_CACHE_SHEET: str = "_cache_operate"
RESULT_SHEET: str = "result"
ERROR_LOG_SHEET: str = "error_log"
INTEGRITY_REPORT_SHEET: str = "integrity_report"
CONCEPT_SUMMARY_SHEET: str = "concept_summary"
RUN_SUMMARY_SHEET: str = "run_summary"

ERROR_LOG_COLUMNS: list[str] = [
    "run_id",
    "source",
    "severity",
    "error_code",
    "error_type",
    "message",
    "retryable",
    "page_num",
    "stock_code",
    "field_name",
    "raw_value",
    "action",
]
CONFIG_SHEET_ALIASES: dict[str, str] = {
    "key": "key",
    "name": "key",
    "config_key": "key",
    "配置项": "key",
    "参数": "key",
    "字段": "key",
    "value": "value",
    "config_value": "value",
    "值": "value",
    "参数值": "value",
}

CONCEPT_CACHE_COLUMNS: list[str] = [
    "fetch_code",
    "concept_count",
    "concept_list",
    "concept_text_raw",
    "concept_source_url",
]
OPERATE_CACHE_COLUMNS: list[str] = [
    "fetch_code",
    "主营业务",
    "产品类型",
    "产品名称",
    "经营范围",
    "产品类型_list",
    "产品名称_list",
]
OPERATE_EXPECTED_FIELDS: set[str] = {"主营业务", "产品类型", "产品名称", "经营范围"}
HK_PAGE_COLUMN: str = "HKZB"
THS_FETCH_CODE_PATTERN: str = r"(?:\d{6}|HK\d{4,5})"
KEYWORD_HIGHLIGHT_WORDS: list[str] = [
    "激光",
    "自动化",
    "工控",
    "工业控制",
    "焊接",
    "切割",
]
KEYWORD_HIGHLIGHT_COLUMNS: list[str] = [
    "secName",
    "announcement_title_clean",
    "concept_list",
    "concept_text_raw",
    "主营业务",
    "产品类型",
    "产品名称",
    "经营范围",
    "产品类型_list",
    "产品名称_list",
]
KEYWORD_HIGHLIGHT_COLOR: str = "FFFF0000"
KEYWORD_HIGHLIGHT_FILL_COLOR: str = "FFFFF2CC"

# =========================
# 日志配置
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)
ANNOUNCEMENT_TIMEZONE: str = "Asia/Shanghai"

_WPS_RUNTIME_PLACEHOLDER: object = object()
Application: Any = globals().get("Application", _WPS_RUNTIME_PLACEHOLDER)


def _missing_wps_runtime(*args: Any, **kwargs: Any) -> Any:
    raise EnvironmentError(
        "当前环境缺少 WPS 内置对象/函数，请在金山文档智能表格的 PY脚本编辑器中运行。"
    )


xl: Callable[..., Any] = globals().get("xl", _missing_wps_runtime)
write_xl: Callable[..., Any] = globals().get("write_xl", _missing_wps_runtime)


@dataclass
class CrawlConfig:
    """运行期配置。

    Attributes:
        search_key: 搜索关键词，不能为空。
        start_date: 起始日期，格式 YYYY-MM-DD。
        end_date: 结束日期，格式 YYYY-MM-DD。
        page_size: 每页条数，范围 1-100，默认 20。
        sleep_min_seconds: 请求间隔最小秒数。
        sleep_max_seconds: 请求间隔最大秒数。
        concept_sleep_seconds: 概念抓取间隔秒数。
        operate_sleep_seconds: 主营抓取间隔秒数。
        is_fulltext: 是否全文搜索。
        sort_name: 排序字段，默认 "pubdate"。
        sort_type: 排序方向，默认 "desc"。
        type_value: 公告类型过滤。
        max_concurrent_workers: 最大并发线程数，默认 5。
        per_request_timeout: 单次请求超时秒数，默认 30。
    """

    search_key: str
    start_date: str
    end_date: str
    page_size: int = 20
    sleep_min_seconds: float = SLEEP_MIN_SECONDS
    sleep_max_seconds: float = SLEEP_MAX_SECONDS
    concept_sleep_seconds: float = CONCEPT_SLEEP_SECONDS
    operate_sleep_seconds: float = OPERATE_SLEEP_SECONDS
    is_fulltext: bool = False
    sort_name: str = "pubdate"
    sort_type: str = "desc"
    type_value: str = ""
    max_concurrent_workers: int = 5
    per_request_timeout: float = 30.0


DEFAULT_CRAWL_CONFIG: CrawlConfig = CrawlConfig(
    search_key=SEARCH_KEY,
    start_date=START_DATE,
    end_date=END_DATE,
)


def assert_wps_runtime() -> None:
    """
    断言当前运行环境为 WPS 在线 Python 运行环境。
    """
    missing_names: list[str] = []
    if Application is _WPS_RUNTIME_PLACEHOLDER:
        missing_names.append("Application")
    if xl is _missing_wps_runtime:
        missing_names.append("xl")
    if write_xl is _missing_wps_runtime:
        missing_names.append("write_xl")
    if missing_names:
        raise EnvironmentError(
            f"当前环境缺少 WPS 内置对象/函数: {missing_names}。"
            "请在金山文档智能表格的 PY脚本编辑器中运行。"
        )


def get_worksheet(sheet_name: str) -> Any | None:
    """
    安全获取工作表对象。
    WPS 某些环境下，sheet 不存在时可能返回 None，而不是抛异常。
    """
    try:
        worksheet = Application.Worksheets.Item(sheet_name)
        if worksheet is None:
            return None
        return worksheet
    except Exception:
        return None


def sheet_exists(sheet_name: str) -> bool:
    """
    判断工作表是否存在。
    """
    return get_worksheet(sheet_name) is not None


def clear_sheet_contents(sheet_name: str) -> None:
    """
    清空工作表已用区域内容，避免旧结果残留。
    """
    worksheet = get_worksheet(sheet_name)
    if worksheet is None:
        return

    try:
        used_range = worksheet.UsedRange
        if used_range is not None:
            used_range.ClearContents()
    except Exception as exc:
        raise RuntimeError(f"清空工作表失败: {sheet_name}") from exc


def clear_sheet_formats(sheet_name: str) -> None:
    """
    清空工作表已用区域格式，避免重跑后保留旧高亮。
    """
    worksheet = get_worksheet(sheet_name)
    if worksheet is None:
        return

    try:
        used_range = worksheet.UsedRange
        if used_range is not None:
            used_range.ClearFormats()
    except Exception as exc:
        raise RuntimeError(f"清空工作表格式失败: {sheet_name}") from exc


def read_sheet_df(sheet_name: str, strict: bool = False) -> pd.DataFrame:
    """
    从 WPS 工作表读取 DataFrame。
    """
    if not sheet_exists(sheet_name):
        return pd.DataFrame()

    try:
        df = xl(sheet_name=sheet_name, headers=True)
    except Exception as exc:
        if strict:
            raise RuntimeError(f"读取工作表失败: {sheet_name}") from exc
        logger.warning("读取工作表失败，按空表处理：%s | %s", sheet_name, exc)
        return pd.DataFrame()

    if not isinstance(df, pd.DataFrame):
        if strict:
            raise TypeError(
                f"读取工作表返回结果不是 DataFrame: sheet={sheet_name}, type={type(df)}"
            )
        logger.warning(
            "读取工作表返回结果不是 DataFrame，按空表处理：sheet=%s, type=%s",
            sheet_name,
            type(df),
        )
        return pd.DataFrame()

    df = df.dropna(how="all").reset_index(drop=True)
    return df


def write_sheet_df(df: pd.DataFrame, sheet_name: str) -> None:
    """
    将 DataFrame 写回 WPS 工作表。
    处理策略：
    1. 先尝试按"已存在 sheet"覆盖写入
    2. 若明确不存在，再尝试 new_sheet=True
    3. 若出现 duplicated sheet_name，说明已存在，回退到覆盖写入
    """
    payload = df.copy()

    if payload.empty:
        payload = pd.DataFrame({"message": [f"{sheet_name} is empty"]})

    # 先尽力清空，但清空失败不阻断主流程
    try:
        clear_sheet_contents(sheet_name)
    except Exception as exc:
        logger.warning("清空工作表失败，继续尝试写入：%s | %s", sheet_name, exc)

    # 第一优先级：按"已存在 sheet"写入
    try:
        write_xl(payload, range="A1", sheet_name=sheet_name, write_df_index=False)
        logger.info(
            "工作表写入完成：sheet=%s, rows=%s, cols=%s",
            sheet_name,
            len(payload),
            len(payload.columns),
        )
        return
    except Exception as exc:
        first_error = exc

    # 第二优先级：如果看起来像"不存在"，则尝试新建
    try:
        write_xl(
            payload,
            range="A1",
            sheet_name=sheet_name,
            new_sheet=True,
            write_df_index=False,
        )
        logger.info(
            "工作表新建并写入完成：sheet=%s, rows=%s, cols=%s",
            sheet_name,
            len(payload),
            len(payload.columns),
        )
        return
    except Exception as exc:
        second_error = exc
        second_error_text = str(exc).lower()

    # 若第二次报 duplicated，说明 sheet 其实存在，再强行回退一次普通写入
    if (
        "duplicated sheet_name" in second_error_text
        or "duplicated" in second_error_text
    ):
        try:
            write_xl(payload, range="A1", sheet_name=sheet_name, write_df_index=False)
            logger.info(
                "重复 sheet 回退写入成功：sheet=%s, rows=%s, cols=%s",
                sheet_name,
                len(payload),
                len(payload.columns),
            )
            return
        except Exception as exc:
            raise RuntimeError(
                "工作表写入失败: "
                f"{sheet_name} | first={first_error} | "
                f"second={second_error} | fallback={exc}"
            ) from exc

    raise RuntimeError(
        f"工作表写入失败: {sheet_name} | first={first_error} | second={second_error}"
    ) from second_error


def build_run_id() -> str:
    """
    生成单次运行标识，便于串联 run_summary 与 error_log。
    """
    timestamp = datetime.utcnow() + timedelta(hours=8)
    return f"wps-report-{timestamp.strftime('%Y%m%d%H%M%S')}-{uuid4().hex[:8]}"


def make_error_record(
    source: str,
    severity: str,
    error_type: str,
    message: str,
    *,
    error_code: str | None = None,
    retryable: bool | None = None,
    page_num: Any = pd.NA,
    stock_code: Any = pd.NA,
    field_name: Any = pd.NA,
    raw_value: Any = pd.NA,
    action: Any = pd.NA,
) -> dict[str, Any]:
    """
    构造统一错误记录，避免不同链路产出不同字段名。
    """
    return {
        "source": source,
        "severity": severity,
        "error_code": error_code or error_type.upper(),
        "error_type": error_type,
        "message": message,
        "retryable": retryable,
        "page_num": page_num,
        "stock_code": stock_code,
        "field_name": field_name,
        "raw_value": raw_value,
        "action": action,
    }


def normalize_error_code(value: Any) -> str:
    """
    将错误码标准化为便于检索的全大写下划线形式。
    """
    value_text = str(value or "UNKNOWN_ERROR").strip().upper()
    value_text = re.sub(r"[^A-Z0-9]+", "_", value_text)
    return value_text.strip("_") or "UNKNOWN_ERROR"


def normalize_error_log(error_log: pd.DataFrame, run_id: str) -> pd.DataFrame:
    """
    统一 error_log 字段，兼容旧记录结构。
    """
    if error_log.empty:
        return pd.DataFrame(columns=ERROR_LOG_COLUMNS)

    normalized_df = error_log.copy()
    normalized_df = normalized_df.rename(
        columns={
            "detail": "message",
            "error_message": "message",
            "check_name": "error_type",
        }
    )

    for column in ERROR_LOG_COLUMNS:
        if column not in normalized_df.columns:
            normalized_df[column] = pd.NA

    normalized_df["run_id"] = run_id
    normalized_df["severity"] = (
        normalized_df["severity"].fillna("ERROR").astype("string").str.upper()
    )
    normalized_df["error_type"] = (
        normalized_df["error_type"].fillna("UnknownError").astype("string")
    )
    normalized_df["message"] = normalized_df["message"].fillna("").astype("string")
    normalized_df["retryable"] = normalized_df["retryable"].astype("boolean")
    normalized_df["error_code"] = normalized_df["error_code"].where(
        normalized_df["error_code"].notna(), normalized_df["error_type"]
    )
    normalized_df["error_code"] = normalized_df["error_code"].map(
        normalize_error_code
    )

    return normalized_df.loc[:, ERROR_LOG_COLUMNS].reset_index(drop=True)


def normalize_config_sheet(config_df: pd.DataFrame) -> pd.DataFrame:
    """
    将配置工作表统一映射为 key/value 结构，兼容中英文列名。
    """
    if config_df.empty:
        return pd.DataFrame(columns=["key", "value"])

    renamed_columns = {
        column: CONFIG_SHEET_ALIASES.get(str(column).strip(), str(column).strip())
        for column in config_df.columns
    }
    normalized_df = config_df.rename(columns=renamed_columns).copy()
    required_columns = {"key", "value"}
    if not required_columns.issubset(set(normalized_df.columns)):
        actual_columns = ",".join(map(str, normalized_df.columns))
        raise ValueError(
            f"{CONFIG_SHEET} 工作表缺少 key/value 列，当前列为: {actual_columns}"
        )

    normalized_df["key"] = normalized_df["key"].astype("string").str.strip()
    normalized_df["value"] = normalized_df["value"].astype("string").str.strip()
    normalized_df = normalized_df.loc[
        normalized_df["key"].notna() & normalized_df["key"].ne("")
    ].copy()
    normalized_df = normalized_df.drop_duplicates(subset=["key"], keep="last")
    return normalized_df.loc[:, ["key", "value"]].reset_index(drop=True)


def parse_bool_config_value(value: str) -> bool:
    """
    解析布尔型配置值。
    """
    normalized_value = value.strip().lower()
    if normalized_value in {"1", "true", "yes", "y", "是"}:
        return True
    if normalized_value in {"0", "false", "no", "n", "否"}:
        return False
    raise ValueError(f"布尔值非法: {value}")


def load_runtime_config() -> tuple[CrawlConfig, pd.DataFrame, pd.DataFrame]:
    """
    读取运行期配置。
    约定 config 工作表包含 key/value 两列；缺失时回退默认配置并记录 WARN。
    """
    config_values: dict[str, Any] = asdict(DEFAULT_CRAWL_CONFIG)
    config_errors: list[dict[str, Any]] = []
    config_sheet_exists = sheet_exists(CONFIG_SHEET)
    default_field_count = len(config_values)

    if config_sheet_exists:
        try:
            config_df = normalize_config_sheet(read_sheet_df(CONFIG_SHEET, strict=True))
        except Exception as exc:
            config_errors.append(
                make_error_record(
                    source="config",
                    severity="ERROR",
                    error_type=type(exc).__name__,
                    message=str(exc),
                    error_code="CONFIG_SHEET_INVALID",
                    retryable=False,
                    action="回退默认配置",
                )
            )
            config_df = pd.DataFrame(columns=["key", "value"])
    else:
        config_errors.append(
            make_error_record(
                source="config",
                severity="WARN",
                error_type="ConfigSheetMissing",
                message="未找到 config 工作表，已使用脚本默认配置。",
                error_code="CONFIG_SHEET_MISSING",
                retryable=False,
                action="使用默认配置",
            )
        )
        config_df = pd.DataFrame(columns=["key", "value"])

    if not config_df.empty:
        raw_config_map = dict(zip(config_df["key"], config_df["value"]))
        parsers: dict[str, Callable[[str], Any]] = {
            "search_key": str,
            "start_date": str,
            "end_date": str,
            "page_size": int,
            "sleep_min_seconds": float,
            "sleep_max_seconds": float,
            "concept_sleep_seconds": float,
            "operate_sleep_seconds": float,
            "is_fulltext": parse_bool_config_value,
            "sort_name": str,
            "sort_type": str,
            "type_value": str,
            "max_concurrent_workers": int,
            "per_request_timeout": float,
        }
        for key, parser in parsers.items():
            raw_value = raw_config_map.get(key)
            if raw_value is None or raw_value == "":
                continue
            try:
                config_values[key] = parser(raw_value)
            except Exception as exc:
                config_errors.append(
                    make_error_record(
                        source="config",
                        severity="ERROR",
                        error_type=type(exc).__name__,
                        message=f"配置项 {key} 解析失败: {exc}",
                        error_code="CONFIG_VALUE_INVALID",
                        retryable=False,
                        field_name=key,
                        raw_value=raw_value,
                        action="回退默认值",
                    )
                )

    if config_values["sleep_min_seconds"] > config_values["sleep_max_seconds"]:
        config_errors.append(
            make_error_record(
                source="config",
                severity="ERROR",
                error_type="InvalidSleepWindow",
                message="sleep_min_seconds 不能大于 sleep_max_seconds，已回退默认值。",
                error_code="CONFIG_SLEEP_WINDOW_INVALID",
                retryable=False,
                field_name="sleep_min_seconds,sleep_max_seconds",
                raw_value=(
                    f"{config_values['sleep_min_seconds']},"
                    f"{config_values['sleep_max_seconds']}"
                ),
                action="回退默认值",
            )
        )
        config_values["sleep_min_seconds"] = DEFAULT_CRAWL_CONFIG.sleep_min_seconds
        config_values["sleep_max_seconds"] = DEFAULT_CRAWL_CONFIG.sleep_max_seconds

    # 输入验证：日期格式校验
    date_pattern = r"^\d{4}-\d{2}-\d{2}$"
    for date_field in ["start_date", "end_date"]:
        date_value = config_values.get(date_field, "")
        if date_value and not re.fullmatch(date_pattern, date_value):
            config_errors.append(
                make_error_record(
                    source="config",
                    severity="ERROR",
                    error_type="InvalidDateFormat",
                    message=f"{date_field} 格式错误，期望 YYYY-MM-DD，实际为：{date_value}",
                    error_code="CONFIG_DATE_FORMAT_INVALID",
                    retryable=False,
                    field_name=date_field,
                    raw_value=date_value,
                    action="回退默认值",
                )
            )
            config_values[date_field] = getattr(DEFAULT_CRAWL_CONFIG, date_field)

    # 输入验证：日期范围校验
    if (
        config_values.get("start_date")
        and config_values.get("end_date")
        and config_values["start_date"] > config_values["end_date"]
    ):
        config_errors.append(
            make_error_record(
                source="config",
                severity="ERROR",
                error_type="InvalidDateRange",
                message=f"start_date ({config_values['start_date']}) 不能晚于 end_date ({config_values['end_date']})",
                error_code="CONFIG_DATE_RANGE_INVALID",
                retryable=False,
                field_name="start_date,end_date",
                raw_value=f"{config_values['start_date']},{config_values['end_date']}",
                action="交换日期或回退默认值",
            )
        )
        config_values["start_date"] = DEFAULT_CRAWL_CONFIG.start_date
        config_values["end_date"] = DEFAULT_CRAWL_CONFIG.end_date

    # 输入验证：page_size 范围校验
    page_size = config_values.get("page_size", 20)
    if not (1 <= page_size <= 100):
        config_errors.append(
            make_error_record(
                source="config",
                severity="ERROR",
                error_type="InvalidPageSize",
                message=f"page_size 必须在 1-100 之间，实际为：{page_size}",
                error_code="CONFIG_PAGE_SIZE_INVALID",
                retryable=False,
                field_name="page_size",
                raw_value=str(page_size),
                action="回退默认值",
            )
        )
        config_values["page_size"] = 20

    # 输入验证：search_key 非空校验
    search_key = config_values.get("search_key", "")
    if not search_key or not str(search_key).strip():
        config_errors.append(
            make_error_record(
                source="config",
                severity="ERROR",
                error_type="EmptySearchKey",
                message="search_key 不能为空",
                error_code="CONFIG_SEARCH_KEY_EMPTY",
                retryable=False,
                field_name="search_key",
                raw_value=str(search_key),
                action="回退默认值",
            )
        )
        config_values["search_key"] = DEFAULT_CRAWL_CONFIG.search_key

    config = CrawlConfig(**config_values)
    config_integrity_report = pd.DataFrame(
        [
            {
                "metric": "config_sheet_exists",
                "value": config_sheet_exists,
            },
            {
                "metric": "config_error_count",
                "value": len(config_errors),
            },
            {
                "metric": "config_sheet_row_count",
                "value": 0 if config_df.empty else len(config_df),
            },
            {
                "metric": "config_default_field_count",
                "value": default_field_count,
            },
        ]
    )

    return config, pd.DataFrame(config_errors), config_integrity_report


def build_retry_adapter_session() -> Session:
    """
    构建带重试能力的 Session。
    """
    session = requests.Session()
    session.trust_env = False

    retry = Retry(
        total=3,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def build_search_page_referer(search_key: str) -> str:
    """
    构建巨潮搜索页 Referer。
    """
    encoded_search_key: str = quote(search_key, safe="")
    return (
        f"{SEARCH_PAGE_URL}"
        f"?notautosubmit=&keyWord={encoded_search_key}&searchType=0"
    )


def build_session(search_key: str) -> Session:
    """
    构建 Session，并先访问搜索页拿 Cookie。
    """
    session = build_retry_adapter_session()
    session.headers.update(
        {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/137.0.0.0 Safari/537.36"
            ),
            "X-Requested-With": "XMLHttpRequest",
            "Referer": build_search_page_referer(search_key),
        }
    )

    warmup_params: dict[str, str] = {
        "notautosubmit": "",
        "keyWord": search_key,
        "searchType": "0",
    }
    resp: Response = session.get(SEARCH_PAGE_URL, params=warmup_params, timeout=20)
    resp.raise_for_status()
    logger.info("Session 预热完成，已获取初始 Cookie。")
    return session


def build_concept_session() -> Session:
    """
    构建同花顺概念/主营抓取 Session。
    """
    session = build_retry_adapter_session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/137.0.0.0 Safari/537.36"
            ),
            "Referer": "https://www.10jqka.com.cn/",
        }
    )
    return session


def normalize_stock_code(stock_code: Any) -> str:
    """
    统一证券代码格式，保证下游请求与 merge 键稳定。
    """
    code: str = str(stock_code).strip()
    code = re.sub(r"\.0$", "", code)
    code = code.zfill(6)
    if not re.fullmatch(r"\d{6}", code):
        raise ValueError(f"证券代码格式非法: {stock_code}")
    return code


def normalize_stock_code_series(series: pd.Series) -> pd.Series:
    """
    向量化标准化证券代码。
    """
    return (
        series.astype("string")
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
        .str.zfill(6)
    )


def normalize_display_sec_code(stock_code: Any, page_column: Any) -> str:
    """
    按公告市场口径标准化展示代码。
    A 股保留 6 位，港股主板 HKZB 保留 5 位。
    """
    code: str = str(stock_code).strip()
    code = re.sub(r"\.0$", "", code)
    page_column_text: str = str(page_column).strip().upper()

    if page_column_text == HK_PAGE_COLUMN:
        code = code.zfill(5)
        if not re.fullmatch(r"\d{5}", code):
            raise ValueError(f"港股证券代码格式非法: {stock_code}")
        return code

    return normalize_stock_code(code)


def normalize_ths_fetch_code(stock_code: Any) -> str:
    """
    标准化同花顺抓取代码，允许 A 股 6 位代码或 HK0267 这类港股代码。
    """
    code: str = str(stock_code).strip().upper()
    code = re.sub(r"\.0$", "", code)

    if re.fullmatch(r"\d{1,6}", code):
        return code.zfill(6)

    hk_match = re.fullmatch(r"HK(\d{1,5})", code)
    if hk_match:
        return f"HK{hk_match.group(1).zfill(4)}"

    raise ValueError(f"同花顺抓取代码格式非法: {stock_code}")


def normalize_ths_fetch_code_series(series: pd.Series) -> pd.Series:
    """
    标准化缓存中的同花顺抓取代码。
    """

    def _normalize(value: Any) -> Any:
        if value is None or pd.isna(value):
            return pd.NA

        value_text = str(value).strip()
        if not value_text:
            return pd.NA
        return normalize_ths_fetch_code(value_text)

    return series.map(_normalize).astype("string")


def normalize_display_sec_code_series(
    stock_code_series: pd.Series, page_column_series: pd.Series
) -> pd.Series:
    """
    向量化标准化展示代码。
    """
    code_series = (
        stock_code_series.astype("string").str.strip().str.replace(r"\.0$", "", regex=True)
    )
    page_series = page_column_series.astype("string").str.strip().str.upper()
    hk_mask = page_series.eq(HK_PAGE_COLUMN)

    display_series = code_series.copy()
    display_series.loc[hk_mask] = display_series.loc[hk_mask].str.zfill(5)
    display_series.loc[~hk_mask] = display_series.loc[~hk_mask].str.zfill(6)

    invalid_hk = hk_mask & ~display_series.str.fullmatch(r"\d{5}", na=False)
    if invalid_hk.any():
        invalid_value = stock_code_series.loc[invalid_hk].iloc[0]
        raise ValueError(f"港股证券代码格式非法: {invalid_value}")

    invalid_a_share = ~hk_mask & ~display_series.str.fullmatch(r"\d{6}", na=False)
    if invalid_a_share.any():
        invalid_value = stock_code_series.loc[invalid_a_share].iloc[0]
        raise ValueError(f"证券代码格式非法: {invalid_value}")

    return display_series.astype("string")


def build_stock_route_frame(
    stock_code_series: pd.Series,
    page_column_series: pd.Series,
    org_id_series: pd.Series,
) -> pd.DataFrame:
    """
    向量化构建展示代码与同花顺抓取代码，避免大表按行 apply。
    """
    page_series = page_column_series.astype("string").str.strip().str.upper()
    org_series = org_id_series.astype("string").str.strip().str.lower()
    display_code_series = normalize_display_sec_code_series(
        stock_code_series=stock_code_series,
        page_column_series=page_series,
    )

    route_df = pd.DataFrame(
        {
            "secCode": display_code_series,
            "ths_concept_code": display_code_series,
            "ths_operate_code": display_code_series,
            "concept_supported": True,
        }
    )
    hk_mask = page_series.eq(HK_PAGE_COLUMN)
    route_df.loc[hk_mask, "concept_supported"] = False
    route_df.loc[hk_mask, "ths_concept_code"] = pd.NA

    hk_a_share_mask = hk_mask & org_series.str.startswith(("gssz", "gssh"), na=False)
    if hk_a_share_mask.any():
        extracted_digits = org_series.loc[hk_a_share_mask].str.extract(r"(\d{7})$")[0]
        missing_digits_mask = extracted_digits.isna()
        if missing_digits_mask.any():
            invalid_org_id = org_series.loc[hk_a_share_mask].loc[missing_digits_mask].iloc[0]
            raise ValueError(f"港股 A+H orgId 无法映射到 A 股代码: {invalid_org_id}")

        a_share_code = extracted_digits.str[-6:].str.zfill(6)
        if not a_share_code.str.fullmatch(r"\d{6}", na=False).all():
            invalid_org_id = org_series.loc[hk_a_share_mask].iloc[0]
            raise ValueError(f"港股 A+H orgId 无法映射到 A 股代码: {invalid_org_id}")

        route_df.loc[hk_a_share_mask, "ths_concept_code"] = a_share_code.to_numpy()
        route_df.loc[hk_a_share_mask, "ths_operate_code"] = a_share_code.to_numpy()
        route_df.loc[hk_a_share_mask, "concept_supported"] = True

    pure_hk_mask = hk_mask & ~hk_a_share_mask
    if pure_hk_mask.any():
        hk_fetch_code = (
            "HK"
            + display_code_series.loc[pure_hk_mask].str.lstrip("0").str.zfill(4)
        )
        route_df.loc[pure_hk_mask, "ths_operate_code"] = (
            normalize_ths_fetch_code_series(hk_fetch_code).to_numpy()
        )
        unknown_org_mask = pure_hk_mask & ~org_series.str.startswith("gshk", na=False)
        if unknown_org_mask.any():
            sample_org_ids = (
                org_series.loc[unknown_org_mask]
                .drop_duplicates()
                .head(3)
                .astype(str)
                .tolist()
            )
            logger.warning(
                "存在未识别的 HKZB orgId 前缀，按纯港股处理：count=%s, sample_org_ids=%s",
                int(unknown_org_mask.sum()),
                sample_org_ids,
            )

    route_df["secCode"] = route_df["secCode"].astype("string")
    route_df["ths_concept_code"] = route_df["ths_concept_code"].astype("string")
    route_df["ths_operate_code"] = route_df["ths_operate_code"].astype("string")
    route_df["concept_supported"] = route_df["concept_supported"].astype("boolean")
    return route_df


def build_ths_targets(sec_code: Any, page_column: Any, org_id: Any) -> dict[str, Any]:
    """
    构建展示代码与同花顺抓取代码路由。
    """
    page_column_text: str = str(page_column).strip().upper()
    display_code: str = normalize_display_sec_code(sec_code, page_column_text)
    org_id_text: str = str(org_id).strip().lower()

    if page_column_text != HK_PAGE_COLUMN:
        normalized_code = normalize_stock_code(display_code)
        return {
            "secCode": normalized_code,
            "ths_concept_code": normalized_code,
            "ths_operate_code": normalized_code,
            "concept_supported": True,
        }

    if org_id_text.startswith(("gssz", "gssh")):
        matched_digits = re.search(r"(\d{7})$", org_id_text)
        if matched_digits is None:
            raise ValueError(f"港股 A+H orgId 无法映射到 A 股代码: {org_id}")

        a_share_code = matched_digits.group(1)[-6:]
        a_share_code = normalize_stock_code(a_share_code)
        return {
            "secCode": display_code,
            "ths_concept_code": a_share_code,
            "ths_operate_code": a_share_code,
            "concept_supported": True,
        }

    hk_digits = display_code.lstrip("0").zfill(4)
    hk_code = normalize_ths_fetch_code(f"HK{hk_digits}")
    if not org_id_text.startswith("gshk"):
        logger.warning(
            "未识别的 HKZB orgId 前缀，按纯港股处理：orgId=%s, secCode=%s",
            org_id,
            display_code,
        )

    return {
        "secCode": display_code,
        "ths_concept_code": pd.NA,
        "ths_operate_code": hk_code,
        "concept_supported": False,
    }


def clean_text(text: str) -> str:
    """
    清洗主营字段文本：压缩空白字符，去除首尾空格和冒号。
    """
    cleaned_text: str = text.replace("\xa0", " ")
    cleaned_text = re.sub(r"\s+", " ", cleaned_text)
    return cleaned_text.strip(" ：:\n\t\r")


def split_items(text: str) -> list[str]:
    """
    将"产品类型/产品名称"拆分为结构化列表。
    """
    normalized_text: str = clean_text(text)
    if not normalized_text:
        return []

    parts: list[str] = re.split(r"\s*[、,，;/；]\s*", normalized_text)
    return [item for item in parts if item]


def serialize_string_list(value: Any) -> str:
    """
    将字符串列表序列化为表格友好的文本。
    """
    if isinstance(value, list):
        return "、".join(str(item).strip() for item in value if str(item).strip())
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def load_sheet_cache(sheet_name: str, required_columns: list[str]) -> pd.DataFrame:
    """
    从缓存 sheet 读取缓存；若不存在或为空，则返回空表。
    """
    cache_df = read_sheet_df(sheet_name)
    if cache_df.empty:
        return pd.DataFrame(columns=required_columns)
    return cache_df


def save_sheet_cache(cache_df: pd.DataFrame, sheet_name: str) -> None:
    """
    将缓存持久化到缓存 sheet。
    """
    write_sheet_df(cache_df, sheet_name)
    logger.info("缓存已写入工作表：%s", sheet_name)


def normalize_cache_frame(
    cache_df: pd.DataFrame,
    required_columns: list[str],
    code_columns: list[str],
    string_columns: list[str],
    numeric_columns: list[str] | None = None,
    dedup_subset: list[str] | None = None,
    code_normalizers: dict[str, Callable[[pd.Series], pd.Series]] | None = None,
    code_patterns: dict[str, str] | None = None,
    legacy_column_map: dict[str, str] | None = None,
) -> pd.DataFrame:
    """
    统一标准化缓存结构，避免概念缓存和主营缓存各维护一套相同逻辑。
    """
    if cache_df.empty:
        return pd.DataFrame(columns=required_columns)

    normalized_df = cache_df.copy()
    for legacy_col, new_col in (legacy_column_map or {}).items():
        if new_col not in normalized_df.columns and legacy_col in normalized_df.columns:
            normalized_df[new_col] = normalized_df[legacy_col]

    for col in required_columns:
        if col not in normalized_df.columns:
            normalized_df[col] = pd.NA

    normalized_df = normalized_df.loc[:, required_columns].copy()

    for code_col in code_columns:
        normalizer = (code_normalizers or {}).get(code_col, normalize_stock_code_series)
        normalized_df[code_col] = normalizer(normalized_df[code_col])

    for numeric_col in numeric_columns or []:
        normalized_df[numeric_col] = pd.to_numeric(
            normalized_df[numeric_col], errors="coerce"
        )

    for string_col in string_columns:
        normalized_df[string_col] = (
            normalized_df[string_col].astype("string").fillna("")
        )

    valid_mask = pd.Series(True, index=normalized_df.index)
    for code_col in code_columns:
        pattern = (code_patterns or {}).get(code_col, r"\d{6}")
        valid_mask = valid_mask & normalized_df[code_col].astype(
            "string"
        ).str.fullmatch(pattern, na=False)

    dedup_key = dedup_subset if dedup_subset is not None else code_columns
    normalized_df = (
        normalized_df.loc[valid_mask]
        .drop_duplicates(subset=dedup_key, keep="last")
        .reset_index(drop=True)
    )
    return normalized_df


def normalize_concept_cache(cache_df: pd.DataFrame) -> pd.DataFrame:
    """
    标准化概念缓存结构。
    """
    return normalize_cache_frame(
        cache_df=cache_df,
        required_columns=CONCEPT_CACHE_COLUMNS,
        code_columns=["fetch_code"],
        string_columns=["concept_list", "concept_text_raw", "concept_source_url"],
        numeric_columns=["concept_count"],
        code_normalizers={"fetch_code": normalize_ths_fetch_code_series},
        code_patterns={"fetch_code": THS_FETCH_CODE_PATTERN},
        legacy_column_map={"secCode": "fetch_code"},
    )


def load_concept_cache(cache_sheet: str = CONCEPT_CACHE_SHEET) -> pd.DataFrame:
    """
    读取并标准化概念缓存。
    """
    cache_df = load_sheet_cache(cache_sheet, CONCEPT_CACHE_COLUMNS)
    return normalize_concept_cache(cache_df)


def save_concept_cache(
    cache_df: pd.DataFrame, cache_sheet: str = CONCEPT_CACHE_SHEET
) -> None:
    """
    保存概念缓存。
    """
    normalized_df = normalize_concept_cache(cache_df)
    save_sheet_cache(normalized_df, cache_sheet)


def normalize_operate_cache(cache_df: pd.DataFrame) -> pd.DataFrame:
    """
    标准化主营缓存结构。
    """
    return normalize_cache_frame(
        cache_df=cache_df,
        required_columns=OPERATE_CACHE_COLUMNS,
        code_columns=["fetch_code"],
        string_columns=[col for col in OPERATE_CACHE_COLUMNS if col != "fetch_code"],
        code_normalizers={"fetch_code": normalize_ths_fetch_code_series},
        code_patterns={"fetch_code": THS_FETCH_CODE_PATTERN},
        legacy_column_map={"secCode": "fetch_code"},
    )


def load_operate_cache(cache_sheet: str = OPERATE_CACHE_SHEET) -> pd.DataFrame:
    """
    读取并标准化主营缓存。
    """
    cache_df = load_sheet_cache(cache_sheet, OPERATE_CACHE_COLUMNS)
    return normalize_operate_cache(cache_df)


def save_operate_cache(
    cache_df: pd.DataFrame, cache_sheet: str = OPERATE_CACHE_SHEET
) -> None:
    """
    保存主营缓存。
    """
    normalized_df = normalize_operate_cache(cache_df)
    save_sheet_cache(normalized_df, cache_sheet)


def prepare_requested_codes(
    stock_codes: list[str],
    source: str,
    severity: str,
    key_column: str = "secCode",
    code_normalizer: Callable[[Any], str] = normalize_stock_code,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    统一标准化证券代码，并把非法输入沉淀到 error_log。
    """
    normalized_codes: list[str] = []
    error_records: list[dict[str, Any]] = []

    for stock_code in stock_codes:
        try:
            normalized_codes.append(code_normalizer(stock_code))
        except Exception as exc:
            error_records.append(
                {
                    "source": source,
                    "severity": severity,
                    "stock_code": str(stock_code),
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                }
            )

    unique_codes = list(dict.fromkeys(normalized_codes))
    requested_code_df = pd.DataFrame({key_column: unique_codes})
    error_log = pd.DataFrame(error_records)
    return requested_code_df, error_log


def select_requested_results_from_cache(
    requested_code_df: pd.DataFrame,
    cache_df: pd.DataFrame,
    result_columns: list[str],
    hit_mask_builder: Callable[[pd.DataFrame], pd.Series],
    key_column: str = "secCode",
) -> tuple[pd.DataFrame, list[str], int]:
    """
    统一从缓存表中筛出当前请求命中的结果，避免多次重复 merge。
    """
    if requested_code_df.empty:
        return pd.DataFrame(columns=result_columns), [], 0

    if cache_df.empty:
        return (
            pd.DataFrame(columns=result_columns),
            requested_code_df[key_column].astype(str).tolist(),
            0,
        )

    cached_requested_df = requested_code_df.merge(
        cache_df,
        on=key_column,
        how="left",
        validate="one_to_one",
    )
    hit_mask = hit_mask_builder(cached_requested_df)
    result_df = cached_requested_df.loc[hit_mask, result_columns].copy()
    hit_codes: set[str] = set(result_df[key_column].astype(str))
    miss_codes = [
        code
        for code in requested_code_df[key_column].astype(str).tolist()
        if code not in hit_codes
    ]
    return result_df, miss_codes, int(hit_mask.sum())


def merge_and_persist_cache(
    cache_df: pd.DataFrame,
    fresh_df: pd.DataFrame,
    normalizer: Callable[[pd.DataFrame], pd.DataFrame],
    cache_sheet: str,
    required_columns: list[str],
) -> pd.DataFrame:
    """
    统一合并旧缓存与新抓取结果，并持久化到缓存 sheet。
    """
    cache_parts: list[pd.DataFrame] = [
        df for df in [cache_df, fresh_df] if not df.empty
    ]
    if cache_parts:
        merged_cache_df = normalizer(
            pd.concat(cache_parts, ignore_index=True, sort=False)
        )
    else:
        merged_cache_df = pd.DataFrame(columns=required_columns)

    save_sheet_cache(merged_cache_df, cache_sheet)
    return merged_cache_df


def build_cache_integrity_rows(
    prefix: str,
    input_count: int,
    unique_count: int,
    cache_exists: bool,
    cache_hit_count: int,
    cache_miss_count: int,
    success_count: int,
    error_count: int,
    extra_rows: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """
    统一生成缓存型补数链路的完整性指标。
    """
    cache_hit_rate = (
        round(cache_hit_count / unique_count * 100, 2) if unique_count else 0.0
    )
    success_rate = round(success_count / unique_count * 100, 2) if unique_count else 0.0
    rows: list[dict[str, Any]] = [
        {"metric": f"{prefix}_input_count", "value": input_count},
        {"metric": f"{prefix}_unique_sec_code_count", "value": unique_count},
        {"metric": f"{prefix}_cache_sheet_exists", "value": cache_exists},
        {"metric": f"{prefix}_cache_hit_count", "value": cache_hit_count},
        {"metric": f"{prefix}_cache_miss_count", "value": cache_miss_count},
        {"metric": f"{prefix}_cache_hit_rate_pct", "value": cache_hit_rate},
        {"metric": f"{prefix}_network_fetch_count", "value": cache_miss_count},
        {"metric": f"{prefix}_success_count", "value": success_count},
        {"metric": f"{prefix}_error_count", "value": error_count},
        {"metric": f"{prefix}_success_rate_pct", "value": success_rate},
    ]
    if extra_rows:
        rows.extend(extra_rows)
    return rows


def collect_miss_code_results(
    miss_codes: list[str],
    session_builder: Callable[[], Session],
    per_code_handler: Callable[
        [Session, str], tuple[dict[str, Any] | None, list[dict[str, Any]]]
    ],
    source: str,
    severity: str,
    sleep_seconds: float,
    success_log_message: str,
    failure_log_message: str,
    max_workers: int = 5,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """
    统一执行 miss-code 抓取循环（支持并发）。

    Args:
        miss_codes: 待抓取的证券代码列表。
        session_builder: Session 构建函数。
        per_code_handler: 单股票处理函数。
        source: 错误记录来源。
        severity: 错误严重程度。
        sleep_seconds: 请求间隔秒数。
        success_log_message: 成功日志模板。
        failure_log_message: 失败日志模板。
        max_workers: 最大并发线程数，默认 5。

    Returns:
        (success_records, error_records) 元组。
    """
    success_records: list[dict[str, Any]] = []
    error_records: list[dict[str, Any]] = []
    if not miss_codes:
        return success_records, error_records

    # 判断是否为网络相关异常（应降级为 WARN）
    def is_network_error(exc: Exception) -> bool:
        """判断异常是否为网络相关错误（可重试的暂时性故障）。"""
        exc_str = str(exc).lower()
        network_keywords = [
            "connection", "timeout", "name resolution", "remote disconnected",
            "max retries", "network", "503", "502", "504", "temporary failure"
        ]
        return any(kw in exc_str for kw in network_keywords)

    # 单线程模式（保持原有行为）
    if max_workers <= 1:
        session = session_builder()
        for stock_code in miss_codes:
            try:
                record, warning_records = per_code_handler(session, stock_code)
                if warning_records:
                    error_records.extend(warning_records)
                if record is not None:
                    success_records.append(record)
                    logger.info(success_log_message, stock_code)
            except Exception as exc:
                logger.exception(failure_log_message, stock_code)
                # 网络错误降级为 WARN，其他错误保持原有严重性
                error_severity = "WARN" if is_network_error(exc) else severity
                error_records.append(
                    {
                        "source": source,
                        "severity": error_severity,
                        "stock_code": stock_code,
                        "error_type": type(exc).__name__,
                        "error_message": str(exc),
                        "retryable": is_network_error(exc),
                    }
                )
            time.sleep(sleep_seconds)
        return success_records, error_records

    # 并发模式
    logger.info(
        "开始并发抓取：miss_codes=%s, max_workers=%s", len(miss_codes), max_workers
    )

    def fetch_single_code(stock_code: str) -> tuple[str, dict[str, Any] | None, list[dict[str, Any]], str | None, str | None]:
        """
        内部包装函数，捕获异常并返回结构化结果。

        Returns:
            (stock_code, record, warning_records, error_msg, error_type)
        """
        try:
            session = session_builder()
            record, warning_records = per_code_handler(session, stock_code)
            return stock_code, record, warning_records, None, None
        except Exception as exc:
            return stock_code, None, [], str(exc), type(exc).__name__

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_code = {executor.submit(fetch_single_code, code): code for code in miss_codes}

        for future in as_completed(future_to_code):
            try:
                stock_code, record, warning_records, error_msg, error_type = future.result()
            except Exception as exc:
                # 处理 executor 级别的异常
                stock_code = future_to_code[future]
                error_msg = str(exc)
                error_type = type(exc).__name__
                record, warning_records = None, []

            if warning_records:
                error_records.extend(warning_records)

            if record is not None:
                success_records.append(record)
                logger.info(success_log_message, stock_code)

            if error_msg:
                logger.exception(failure_log_message, stock_code)
                # 网络错误降级为 WARN，其他错误保持原有严重性
                error_severity = "WARN" if is_network_error(error_msg) else severity
                error_records.append(
                    {
                        "source": source,
                        "severity": error_severity,
                        "stock_code": stock_code,
                        "error_type": error_type or "ConcurrentFetchError",
                        "error_message": error_msg,
                        "retryable": is_network_error(error_msg),
                    }
                )

            # 并发模式下也需要限速，避免触发风控
            time.sleep(sleep_seconds)

    return success_records, error_records


def fetch_page(session: Session, config: CrawlConfig, page_num: int) -> dict[str, Any]:
    """
    拉取单页 JSON。

    Args:
        session: 已初始化的 requests Session。
        config: 爬取配置。
        page_num: 页码。

    Returns:
        CNINFO API 响应字典。

    Raises:
        ValueError: Content-Type 非 JSON 或响应体过大。
        TypeError: 响应 JSON 不是 dict 类型。
    """
    params: dict[str, Any] = {
        "searchkey": config.search_key,
        "sdate": config.start_date,
        "edate": config.end_date,
        "isfulltext": str(config.is_fulltext).lower(),
        "sortName": config.sort_name,
        "sortType": config.sort_type,
        "pageNum": page_num,
        "pageSize": config.page_size,
        "type": config.type_value,
    }

    resp: Response = session.get(
        BASE_URL, params=params, timeout=(5, config.per_request_timeout)
    )
    resp.raise_for_status()

    # 响应大小检查（防 DoS）
    content_length: int | None = resp.headers.get("Content-Length")
    if content_length is not None and int(content_length) > 5 * 1024 * 1024:
        raise ValueError(
            f"响应体过大 ({content_length} bytes)，超过 5MB 限制，"
            "可能意味着被风控或页面策略有变。"
        )

    content_type: str = resp.headers.get("Content-Type", "")
    if "application/json" not in content_type:
        raise ValueError(
            f"返回类型异常，期望 JSON，实际为: {content_type}；"
            "这通常意味着被风控、被重定向，或页面策略有变。"
        )

    payload: Any = resp.json()
    if not isinstance(payload, dict):
        raise TypeError(f"返回 JSON 不是 dict，而是: {type(payload)}")

    return payload


def strip_html_em(text: Any) -> str:
    """
    清洗标题中的 <em> 标签等高亮标记。
    """
    if text is None:
        return ""
    text_str: str = str(text)
    text_str = re.sub(r"</?em>", "", text_str, flags=re.IGNORECASE)
    text_str = re.sub(r"\s+", " ", text_str).strip()
    return text_str


def extract_concepts_from_html(html_text: str) -> tuple[list[str], str]:
    """
    从同花顺个股页 HTML 中提取"涉及概念"。
    """
    soup = BeautifulSoup(html_text, "html.parser")

    dt_tag = cast(
        Tag | None,
        soup.find("dt", string=lambda x: isinstance(x, str) and "涉及概念" in x),
    )
    if dt_tag is None:
        raise ValueError("未找到涉及概念字段")

    dd_tag = cast(Tag | None, dt_tag.find_next("dd"))
    if dd_tag is None:
        raise ValueError("未找到涉及概念对应 dd 节点")

    title_attr = dd_tag.get("title", "")
    raw_concept_text = (
        title_attr.strip() if isinstance(title_attr, str) else dd_tag.get_text(strip=True)
    )
    if not raw_concept_text:
        raise ValueError("涉及概念内容为空")

    concepts: list[str] = [
        item.strip()
        for item in raw_concept_text.replace("，", ",").split(",")
        if item.strip()
    ]
    concepts = list(dict.fromkeys(concepts))

    if not concepts:
        raise ValueError("涉及概念解析后为空")

    return concepts, raw_concept_text


def fetch_stock_concepts(session: Session, stock_code: str) -> dict[str, Any]:
    """
    按同花顺抓取代码抓取单只股票的"涉及概念"。

    Args:
        session: 已初始化的 requests Session。
        stock_code: 证券代码（6 位 A 股或 HK 开头港股）。

    Returns:
        包含 fetch_code, concept_count, concept_list, concept_text_raw, concept_source_url 的字典。

    Raises:
        ValueError: Content-Type 非 HTML 或页面缺少目标字段。
    """
    code: str = normalize_ths_fetch_code(stock_code)
    url: str = f"{THS_STOCK_PAGE_URL}/{code}/"

    resp: Response = session.get(url, timeout=20)
    resp.raise_for_status()

    content_type: str = resp.headers.get("Content-Type", "")
    if "text/html" not in content_type:
        raise ValueError(f"返回类型异常，期望 HTML，实际为: {content_type}")

    concepts, raw_concept_text = extract_concepts_from_html(resp.text)
    return {
        "fetch_code": code,
        "concept_count": len(concepts),
        "concept_list": "、".join(concepts),
        "concept_text_raw": raw_concept_text,
        "concept_source_url": url,
    }


def handle_single_concept_stock(
    session: Session,
    stock_code: str,
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    """
    单股概念处理。
    """
    return fetch_stock_concepts(session, stock_code), []


def fetch_operate_html(session: Session, stock_code: str) -> str:
    """
    按同花顺抓取代码下载主营介绍页面 HTML。

    Args:
        session: 已初始化的 requests Session。
        stock_code: 证券代码（6 位 A 股或 HK 开头港股）。

    Returns:
        页面 HTML 文本。

    Raises:
        ValueError: Content-Type 非 HTML。
    """
    code: str = normalize_ths_fetch_code(stock_code)
    url: str = f"{THS_OPERATE_PAGE_URL}/{code}/operate.html"

    resp: Response = session.get(url, timeout=20)
    resp.raise_for_status()
    resp.encoding = resp.apparent_encoding

    content_type: str = resp.headers.get("Content-Type", "")
    if "text/html" not in content_type:
        raise ValueError(f"返回类型异常，期望 HTML，实际为: {content_type}")

    return resp.text


def parse_operate_intro(html_text: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    解析主营介绍区域。
    返回：
    - df：字段明细
    - error_log：解析与完整性检查日志
    """
    soup = BeautifulSoup(html_text, "html.parser")

    rows: list[dict[str, Any]] = []
    error_rows: list[dict[str, str]] = []

    intro_div = cast(Tag | None, soup.find("div", id="intro"))
    if intro_div is None:
        error_log = pd.DataFrame(
            [{"error_type": "missing_intro_block", "detail": "未找到 div#intro"}],
            columns=["error_type", "detail"],
        )
        return pd.DataFrame(columns=["字段", "内容"]), error_log

    li_nodes = intro_div.select("ul.main_intro_list > li")
    if not li_nodes:
        error_log = pd.DataFrame(
            [{"error_type": "missing_li_nodes", "detail": "未找到主营介绍 li 节点"}],
            columns=["error_type", "detail"],
        )
        return pd.DataFrame(columns=["字段", "内容"]), error_log

    for idx, li in enumerate(li_nodes, start=1):
        span_tag = cast(Tag | None, li.find("span"))
        p_tag = cast(Tag | None, li.find("p"))

        field: str = clean_text(
            " ".join(span_tag.stripped_strings) if span_tag else ""
        ).rstrip("：:")
        value: str = clean_text(" ".join(p_tag.stripped_strings) if p_tag else "")

        if not field:
            error_rows.append(
                {
                    "error_type": "missing_field",
                    "detail": f"第 {idx} 个 li 缺少字段名",
                }
            )
            continue

        if not value:
            error_rows.append(
                {
                    "error_type": "missing_value",
                    "detail": f"字段 {field} 缺少内容",
                }
            )
            continue

        rows.append({"字段": field, "内容": value})

    df = pd.DataFrame(rows, columns=["字段", "内容"])
    error_log = pd.DataFrame(error_rows, columns=["error_type", "detail"])

    if df.empty:
        error_log = pd.concat(
            [
                error_log,
                pd.DataFrame(
                    [
                        {
                            "error_type": "empty_result",
                            "detail": "主营介绍区域解析结果为空",
                        }
                    ]
                ),
            ],
            ignore_index=True,
        )

    actual_fields: set[str] = set(df["字段"].tolist()) if not df.empty else set()
    missing_fields: set[str] = OPERATE_EXPECTED_FIELDS - actual_fields
    if missing_fields:
        missing_df = pd.DataFrame(
            [
                {
                    "error_type": "missing_expected_field",
                    "detail": f"缺少字段: {field_name}",
                }
                for field_name in sorted(missing_fields)
            ]
        )
        error_log = pd.concat([error_log, missing_df], ignore_index=True)

    if not df.empty:
        dup_fields = df.loc[df["字段"].duplicated(keep=False), "字段"].drop_duplicates()
        if not dup_fields.empty:
            duplicate_df = pd.DataFrame(
                [
                    {
                        "error_type": "duplicate_field",
                        "detail": f"字段重复出现: {field_name}",
                    }
                    for field_name in dup_fields.tolist()
                ]
            )
            error_log = pd.concat([error_log, duplicate_df], ignore_index=True)

    return df, error_log


def build_operate_result(df: pd.DataFrame) -> dict[str, Any]:
    """
    将主营明细表转换为结构化结果。
    """
    mapping: dict[str, Any] = dict(zip(df["字段"], df["内容"])) if not df.empty else {}
    result: dict[str, Any] = {
        "主营业务": mapping.get("主营业务", ""),
        "产品类型": mapping.get("产品类型", ""),
        "产品名称": mapping.get("产品名称", ""),
        "经营范围": mapping.get("经营范围", ""),
        "产品类型_list": split_items(mapping.get("产品类型", "")),
        "产品名称_list": split_items(mapping.get("产品名称", "")),
    }
    return result


def handle_single_operate_stock(
    session: Session,
    stock_code: str,
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    """
    单股主营处理。
    """
    html_text = fetch_operate_html(session, stock_code)
    parsed_df, parse_error_log = parse_operate_intro(html_text)

    warning_records: list[dict[str, Any]] = []
    if not parse_error_log.empty:
        parse_error_log = parse_error_log.copy()
        parse_error_log["source"] = "ths_operate"
        parse_error_log["severity"] = "WARN"
        parse_error_log["stock_code"] = stock_code
        parse_error_log["error_message"] = parse_error_log["detail"]
        parse_error_log = parse_error_log.loc[
            :,
            ["source", "severity", "stock_code", "error_type", "error_message"],
        ]
        warning_records = parse_error_log.to_dict(orient="records")

    if parsed_df.empty:
        logger.warning("同花顺主营抓取为空：%s", stock_code)
        return None, warning_records

    operate_result = build_operate_result(parsed_df)
    operate_result["fetch_code"] = normalize_ths_fetch_code(stock_code)
    operate_result["产品类型_list"] = serialize_string_list(
        operate_result["产品类型_list"]
    )
    operate_result["产品名称_list"] = serialize_string_list(
        operate_result["产品名称_list"]
    )
    return operate_result, warning_records


def build_any_non_empty_text_mask(df: pd.DataFrame, columns: list[str]) -> pd.Series:
    """
    统一判断一组文本字段中是否至少有一个非空（纯向量化实现）。

    Args:
        df: 输入 DataFrame。
        columns: 待检查的列名列表。

    Returns:
        布尔 Series，True 表示至少有一个字段非空。
    """
    if df.empty:
        return pd.Series(dtype="bool")

    # 纯向量化：使用 applymap 替代 apply(lambda col: col.str.strip())
    # 注意：applymap 在 pandas 2.1+ 已被 map 替代，但为兼容性保留 applymap
    try:
        # pandas >= 2.1.0 使用 map
        stripped = df.loc[:, columns].fillna("").astype("string").map(lambda x: str(x).strip())
    except AttributeError:
        # pandas < 2.1.0 使用 applymap
        stripped = df.loc[:, columns].fillna("").astype("string").applymap(lambda x: str(x).strip())

    return stripped.ne("").any(axis=1)


def has_non_empty_concept_list(df: pd.DataFrame) -> pd.Series:
    """
    判断概念补数结果是否命中。
    """
    return df["concept_list"].astype("string").str.strip().ne("")


def has_non_empty_operate_fields(df: pd.DataFrame) -> pd.Series:
    """
    判断主营补数结果是否命中。
    """
    operate_core_columns: list[str] = ["主营业务", "产品类型", "产品名称", "经营范围"]
    return build_any_non_empty_text_mask(df, operate_core_columns)


def convert_announcement_time_to_beijing(announcement_time: pd.Series) -> pd.Series:
    """
    将巨潮公告毫秒时间戳转换为北京时间本地时间（naive datetime）。
    不依赖 tz_convert / tz_localize，避免 WPS 容器的 zoneinfo 权限问题。
    """
    ts_ms = pd.to_numeric(announcement_time, errors="coerce")
    beijing_ms = ts_ms + 8 * 60 * 60 * 1000
    return pd.to_datetime(beijing_ms, unit="ms", errors="coerce")


def normalize_records(records: list[dict[str, Any]]) -> pd.DataFrame:
    """
    将 records 标准化为 DataFrame。
    """
    if not records:
        return pd.DataFrame(
            columns=[
                "announcementId",
                "secCode",
                "secName",
                "announcementTitle",
                "announcement_title_clean",
                "announcementTime",
                "announcement_date",
                "adjunctUrl",
                "pdf_url",
                "adjunctType",
                "adjunctSize",
                "columnId",
                "pageColumn",
                "orgId",
                "announcementType",
                "ths_concept_code",
                "ths_operate_code",
                "concept_supported",
            ]
        )

    df = pd.DataFrame.from_records(records)

    required_cols: list[str] = [
        "announcementId",
        "secCode",
        "secName",
        "announcementTitle",
        "announcementTime",
        "adjunctUrl",
        "adjunctType",
        "adjunctSize",
        "columnId",
        "pageColumn",
        "orgId",
        "announcementType",
    ]
    for col in required_cols:
        if col not in df.columns:
            df[col] = pd.NA

    df["announcement_title_clean"] = df["announcementTitle"].map(strip_html_em)

    df["adjunctUrl"] = df["adjunctUrl"].fillna("").astype(str)
    df["pdf_url"] = STATIC_BASE_URL + df["adjunctUrl"].str.lstrip("/")

    df["announcement_datetime"] = convert_announcement_time_to_beijing(
        df["announcementTime"]
    )
    df["announcement_date"] = df["announcement_datetime"].dt.strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    df["pageColumn"] = df["pageColumn"].astype("string").str.strip().str.upper()
    df["orgId"] = df["orgId"].astype("string").str.strip()
    route_df = build_stock_route_frame(
        stock_code_series=df["secCode"],
        page_column_series=df["pageColumn"],
        org_id_series=df["orgId"],
    )
    df["secCode"] = route_df["secCode"].astype("string")
    df["ths_concept_code"] = route_df["ths_concept_code"].astype("string")
    df["ths_operate_code"] = route_df["ths_operate_code"].astype("string")
    df["concept_supported"] = route_df["concept_supported"].astype("boolean")
    df["secName"] = df["secName"].astype("string").str.strip()

    if "announcementId" in df.columns:
        df = df.drop_duplicates(subset=["announcementId"], keep="first").copy()
    else:
        df = df.drop_duplicates(keep="first").copy()

    return df


def batch_get_stock_concepts(
    stock_codes: list[str],
    cache_sheet: str = CONCEPT_CACHE_SHEET,
    sleep_seconds: float = CONCEPT_SLEEP_SECONDS,
    max_workers: int = 5,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    批量抓取证券代码对应的同花顺"涉及概念"，并复用工作表缓存。

    Args:
        stock_codes: 证券代码列表。
        cache_sheet: 缓存工作表名称。
        sleep_seconds: 请求间隔秒数。
        max_workers: 最大并发线程数，默认 5。

    Returns:
        (concept_result_df, integrity_report, error_log) 元组。
    """
    requested_code_df, initial_error_log = prepare_requested_codes(
        stock_codes=stock_codes,
        source="ths_concept",
        severity="ERROR",
        key_column="fetch_code",
        code_normalizer=normalize_ths_fetch_code,
    )
    unique_codes = requested_code_df["fetch_code"].astype(str).tolist()
    error_records: list[dict[str, Any]] = initial_error_log.to_dict(orient="records")
    cache_df = load_concept_cache(cache_sheet)
    cache_exists: bool = sheet_exists(cache_sheet)

    concept_result_df, miss_codes, cache_hit_count = (
        select_requested_results_from_cache(
            requested_code_df=requested_code_df,
            cache_df=cache_df,
            result_columns=CONCEPT_CACHE_COLUMNS,
            hit_mask_builder=has_non_empty_concept_list,
            key_column="fetch_code",
        )
    )

    cache_miss_count: int = len(miss_codes)
    cache_hit_rate: float = (
        round(cache_hit_count / len(unique_codes) * 100, 2) if unique_codes else 0.0
    )
    logger.info(
        "概念缓存状态：sheet_exists=%s, unique_codes=%s, hit=%s, miss=%s, hit_rate=%s%%",
        cache_exists,
        len(unique_codes),
        cache_hit_count,
        cache_miss_count,
        cache_hit_rate,
    )
    if unique_codes and cache_miss_count == 0:
        logger.info("概念缓存已全部命中，无需请求同花顺概念页面。")

    success_records, fetch_error_records = collect_miss_code_results(
        miss_codes=miss_codes,
        session_builder=build_concept_session,
        per_code_handler=handle_single_concept_stock,
        source="ths_concept",
        severity="ERROR",
        sleep_seconds=sleep_seconds,
        success_log_message="同花顺概念抓取成功：%s",
        failure_log_message="同花顺概念抓取失败：%s",
        max_workers=max_workers,
    )
    error_records.extend(fetch_error_records)

    fresh_df = normalize_concept_cache(pd.DataFrame(success_records))
    merged_cache_df = merge_and_persist_cache(
        cache_df=cache_df,
        fresh_df=fresh_df,
        normalizer=normalize_concept_cache,
        cache_sheet=cache_sheet,
        required_columns=CONCEPT_CACHE_COLUMNS,
    )

    concept_result_df, _, _ = select_requested_results_from_cache(
        requested_code_df=requested_code_df,
        cache_df=merged_cache_df,
        result_columns=CONCEPT_CACHE_COLUMNS,
        hit_mask_builder=has_non_empty_concept_list,
        key_column="fetch_code",
    )

    if unique_codes and concept_result_df.empty:
        raise RuntimeError(
            "未成功抓取任何证券的涉及概念，请检查缓存、网络或同花顺页面结构。"
        )

    error_log = pd.DataFrame(error_records)
    null_concept_list_count: int = (
        int(concept_result_df["concept_list"].isna().sum())
        if "concept_list" in concept_result_df.columns
        else 0
    )
    duplicated_fetch_code_count: int = (
        int(concept_result_df["fetch_code"].duplicated().sum())
        if "fetch_code" in concept_result_df.columns
        else 0
    )
    integrity_report = pd.DataFrame(
        build_cache_integrity_rows(
            prefix="concept",
            input_count=len(stock_codes),
            unique_count=len(unique_codes),
            cache_exists=cache_exists,
            cache_hit_count=cache_hit_count,
            cache_miss_count=cache_miss_count,
            success_count=int(len(concept_result_df)),
            error_count=int(len(error_log)),
            extra_rows=[
                {
                    "metric": "duplicated_fetch_code_count",
                    "value": duplicated_fetch_code_count,
                },
                {"metric": "null_concept_list_count", "value": null_concept_list_count},
            ],
        )
    )

    return concept_result_df.reset_index(drop=True), integrity_report, error_log


def batch_get_stock_operate(
    stock_codes: list[str],
    cache_sheet: str = OPERATE_CACHE_SHEET,
    sleep_seconds: float = OPERATE_SLEEP_SECONDS,
    max_workers: int = 5,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    批量抓取证券代码对应的同花顺主营介绍，并复用工作表缓存。

    Args:
        stock_codes: 证券代码列表。
        cache_sheet: 缓存工作表名称。
        sleep_seconds: 请求间隔秒数。
        max_workers: 最大并发线程数，默认 5。

    Returns:
        (operate_result_df, integrity_report, error_log) 元组。
    """
    requested_code_df, initial_error_log = prepare_requested_codes(
        stock_codes=stock_codes,
        source="ths_operate",
        severity="WARN",
        key_column="fetch_code",
        code_normalizer=normalize_ths_fetch_code,
    )
    unique_codes = requested_code_df["fetch_code"].astype(str).tolist()
    error_records: list[dict[str, Any]] = initial_error_log.to_dict(orient="records")
    cache_df = load_operate_cache(cache_sheet)
    cache_exists: bool = sheet_exists(cache_sheet)

    operate_result_df, miss_codes, cache_hit_count = (
        select_requested_results_from_cache(
            requested_code_df=requested_code_df,
            cache_df=cache_df,
            result_columns=OPERATE_CACHE_COLUMNS,
            hit_mask_builder=has_non_empty_operate_fields,
            key_column="fetch_code",
        )
    )

    cache_miss_count: int = len(miss_codes)
    cache_hit_rate: float = (
        round(cache_hit_count / len(unique_codes) * 100, 2) if unique_codes else 0.0
    )
    logger.info(
        "主营缓存状态：sheet_exists=%s, unique_codes=%s, hit=%s, miss=%s, hit_rate=%s%%",
        cache_exists,
        len(unique_codes),
        cache_hit_count,
        cache_miss_count,
        cache_hit_rate,
    )
    if unique_codes and cache_miss_count == 0:
        logger.info("主营缓存已全部命中，无需请求同花顺主营页面。")

    success_records, fetch_error_records = collect_miss_code_results(
        miss_codes=miss_codes,
        session_builder=build_concept_session,
        per_code_handler=handle_single_operate_stock,
        source="ths_operate",
        severity="WARN",
        sleep_seconds=sleep_seconds,
        success_log_message="同花顺主营抓取成功：%s",
        failure_log_message="同花顺主营抓取失败：%s",
        max_workers=max_workers,
    )
    error_records.extend(fetch_error_records)

    fresh_df = normalize_operate_cache(pd.DataFrame(success_records))
    merged_cache_df = merge_and_persist_cache(
        cache_df=cache_df,
        fresh_df=fresh_df,
        normalizer=normalize_operate_cache,
        cache_sheet=cache_sheet,
        required_columns=OPERATE_CACHE_COLUMNS,
    )

    operate_result_df, _, _ = select_requested_results_from_cache(
        requested_code_df=requested_code_df,
        cache_df=merged_cache_df,
        result_columns=OPERATE_CACHE_COLUMNS,
        hit_mask_builder=has_non_empty_operate_fields,
        key_column="fetch_code",
    )

    error_log = pd.DataFrame(error_records)
    operate_missing_expected_field_count: int = (
        int(error_log["error_type"].eq("missing_expected_field").sum())
        if not error_log.empty and "error_type" in error_log.columns
        else 0
    )

    integrity_report = pd.DataFrame(
        build_cache_integrity_rows(
            prefix="operate",
            input_count=len(stock_codes),
            unique_count=len(unique_codes),
            cache_exists=cache_exists,
            cache_hit_count=cache_hit_count,
            cache_miss_count=cache_miss_count,
            success_count=int(len(operate_result_df)),
            error_count=int(len(error_log)),
            extra_rows=[
                {
                    "metric": "operate_missing_expected_field_count",
                    "value": operate_missing_expected_field_count,
                }
            ],
        )
    )

    return operate_result_df.reset_index(drop=True), integrity_report, error_log


def build_concept_summary(result_df: pd.DataFrame) -> pd.DataFrame:
    """
    汇总概念维度的公司数与公告数。
    """
    summary_columns: list[str] = ["concept_name", "stock_count", "announcement_count"]
    if result_df.empty or "concept_list" not in result_df.columns:
        return pd.DataFrame(columns=summary_columns)

    concept_detail_df = result_df.loc[
        result_df["concept_list"].notna(),
        ["announcementId", "secCode", "concept_list"],
    ].copy()
    if concept_detail_df.empty:
        return pd.DataFrame(columns=summary_columns)

    concept_detail_df["concept_name"] = (
        concept_detail_df["concept_list"].astype("string").str.split("、")
    )
    concept_detail_df = concept_detail_df.explode("concept_name", ignore_index=True)
    concept_detail_df["concept_name"] = (
        concept_detail_df["concept_name"].astype("string").str.strip()
    )
    concept_detail_df = concept_detail_df.loc[
        concept_detail_df["concept_name"].notna()
        & concept_detail_df["concept_name"].ne("")
    ].copy()

    if concept_detail_df.empty:
        return pd.DataFrame(columns=summary_columns)

    concept_detail_df = concept_detail_df.drop_duplicates(
        subset=["announcementId", "secCode", "concept_name"],
        keep="first",
    )

    summary_df = (
        concept_detail_df.groupby("concept_name", as_index=False)
        .agg(
            stock_count=("secCode", "nunique"),
            announcement_count=("announcementId", "nunique"),
        )
        .sort_values(
            ["stock_count", "announcement_count", "concept_name"],
            ascending=[False, False, True],
        )
        .reset_index(drop=True)
    )
    return summary_df


def build_keyword_highlight_pattern(keywords: list[str]) -> re.Pattern[str] | None:
    """
    构建关键词匹配正则，按长度降序避免短词抢占长词。
    """
    normalized_keywords: list[str] = [
        keyword.strip()
        for keyword in keywords
        if isinstance(keyword, str) and keyword.strip()
    ]
    if not normalized_keywords:
        return None

    ordered_keywords: list[str] = sorted(
        set(normalized_keywords), key=len, reverse=True
    )
    return re.compile("|".join(re.escape(keyword) for keyword in ordered_keywords))


def add_keyword_hit_flags(result_df: pd.DataFrame) -> pd.DataFrame:
    """
    为结果增加关键词命中标记列，供结果表样式高亮与人工筛选复核。
    """
    if result_df.empty:
        output_df = result_df.copy()
        output_df["keyword_hit_any"] = False
        return output_df

    result = result_df.copy()
    keyword_pattern = build_keyword_highlight_pattern(KEYWORD_HIGHLIGHT_WORDS)
    if keyword_pattern is None:
        result["keyword_hit_any"] = False
        return result

    hit_cols: list[str] = []

    for column_name in KEYWORD_HIGHLIGHT_COLUMNS:
        if column_name not in result.columns:
            continue

        hit_col = f"hit_{column_name}"
        result[hit_col] = (
            result[column_name]
            .fillna("")
            .astype("string")
            .str.contains(keyword_pattern, regex=True, na=False)
        )
        hit_cols.append(hit_col)

    result["keyword_hit_any"] = result[hit_cols].any(axis=1) if hit_cols else False
    return result


def parse_wps_color_argb(color: str) -> int:
    """
    将 ARGB 十六进制颜色转换为 WPS/Excel 对象模型可接受的整数颜色值。

    这里显式忽略 alpha，只保留 RGB，避免运行时对透明度解释不一致。
    """
    normalized_color = color.strip() if isinstance(color, str) else ""
    if not re.fullmatch(r"[0-9A-Fa-f]{8}", normalized_color):
        raise ValueError(f"非法颜色配置，必须为 8 位 ARGB 十六进制字符串: {color}")

    rgb_hex = normalized_color[2:]
    red = int(rgb_hex[0:2], 16)
    green = int(rgb_hex[2:4], 16)
    blue = int(rgb_hex[4:6], 16)
    return red + (green << 8) + (blue << 16)


def clear_keyword_highlight_style(cell: Any, row_num: int, column_name: str) -> None:
    """
    清理单元格旧格式，避免重跑时残留历史高亮。
    """
    try:
        cell.ClearFormats()
    except Exception as exc:
        raise RuntimeError(
            "清理关键词高亮旧格式失败: "
            f"row={row_num}, column={column_name}"
        ) from exc


def apply_keyword_highlight(sheet_name: str, result_df: pd.DataFrame) -> None:
    """
    按命中布尔列对结果表数据区做关键词局部高亮（仅高亮匹配文本，非整个单元格）。
    使用 Characters() 方法定位匹配字符范围；若不支持则降级为整格高亮。
    """
    if result_df.empty:
        return

    worksheet = get_worksheet(sheet_name)
    if worksheet is None:
        raise RuntimeError(f"关键词高亮失败，工作表不存在：{sheet_name}")

    font_color = parse_wps_color_argb(KEYWORD_HIGHLIGHT_COLOR)
    fill_color = parse_wps_color_argb(KEYWORD_HIGHLIGHT_FILL_COLOR)
    column_positions: dict[str, int] = {
        column_name: index + 1 for index, column_name in enumerate(result_df.columns)
    }

    # 编译关键词正则（用于局部高亮定位）
    keyword_pattern = re.compile(
        '|'.join(re.escape(kw) for kw in KEYWORD_HIGHLIGHT_WORDS)
    )

    for column_name in KEYWORD_HIGHLIGHT_COLUMNS:
        if column_name not in column_positions:
            continue

        hit_col = f"hit_{column_name}"
        if hit_col not in result_df.columns:
            raise KeyError(f"结果表缺少关键词命中列：{hit_col}")

        column_index = column_positions[column_name]
        hit_flags = result_df[hit_col].fillna(False).astype(bool).tolist()
        cell_texts = result_df[column_name].fillna('').astype(str).tolist()

        for row_num, (is_hit, cell_text) in enumerate(zip(hit_flags, cell_texts), start=2):
            try:
                cell = worksheet.Cells(row_num, column_index)
            except Exception as exc:
                raise RuntimeError(
                    "获取关键词高亮目标单元格失败："
                    f"row={row_num}, column={column_name}"
                ) from exc

            clear_keyword_highlight_style(
                cell=cell,
                row_num=row_num,
                column_name=column_name,
            )

            if not is_hit:
                continue

            # 无文本则跳过
            if not cell_text:
                continue

            try:
                # 尝试使用 Characters() 做局部高亮
                matches = list(keyword_pattern.finditer(cell_text))

                if not matches:
                    # hit_flag 为 True 但无匹配，降级为整格高亮
                    cell.Font.Color = font_color
                    cell.Interior.Color = fill_color
                else:
                    # 先设置整格背景色（Characters 不支持 Interior）
                    cell.Interior.Color = fill_color

                    for match in matches:
                        # Characters 使用 1-based 索引
                        start_pos = match.start() + 1
                        length = match.end() - match.start()
                        try:
                            char_obj = cell.Characters(start_pos, length)
                            char_obj.Font.Color = font_color
                            # 可选：加粗关键词
                            char_obj.Font.Bold = True
                        except (AttributeError, IndexError):
                            # WPS 不支持 Characters 方法，降级为整格高亮
                            cell.Font.Color = font_color
                            break
            except Exception as exc:
                raise RuntimeError(
                    "写入关键词高亮样式失败："
                    f"row={row_num}, column={column_name}"
                ) from exc


def apply_secCode_text_format(sheet_name: str, row_count: int) -> None:
    """Apply text format to secCode column to preserve leading zeros.

    Args:
        sheet_name: Name of the worksheet
        row_count: Number of data rows (excluding header)
    """
    worksheet = get_worksheet(sheet_name)
    if worksheet is None:
        raise RuntimeError(f"secCode 文本格式设置失败，工作表不存在：{sheet_name}")

    try:
        # 使用 Columns 对象设置整列为文本格式
        # secCode 是第 3 列（C 列）
        secCode_column = worksheet.Columns.Item(3)
        secCode_column.NumberFormat = "@"

        # 额外确保：遍历每个单元格强制设置（针对已写入的数据）
        for row_num in range(2, row_count + 2):
            try:
                cell = worksheet.Cells(row_num, 3)
                # 如果单元格已有值，先读取再重新写入，强制类型转换
                if cell.Value is not None and cell.Value != "":
                    cell.Value = str(cell.Value)
            except Exception:
                pass  # 跳过个别单元格的错误
    except Exception as exc:
        raise RuntimeError(
            f"设置 secCode 文本格式失败：sheet={sheet_name}"
        ) from exc


def apply_header_auto_filter(sheet_name: str, column_count: int) -> None:
    """Apply AutoFilter to header row for easy filtering.

    Args:
        sheet_name: Name of the worksheet
        column_count: Number of columns in the sheet
    """
    worksheet = get_worksheet(sheet_name)
    if worksheet is None:
        raise RuntimeError(f"AutoFilter 设置失败，工作表不存在：{sheet_name}")

    try:
        # 使用 Rows.Item(1) 获取第 1 行，然后调用 AutoFilter() 方法
        # 官方文档：使用 Range 对象的 AutoFilter 方法打开自动筛选
        first_row = worksheet.Rows.Item(1)
        if first_row is not None:
            first_row.AutoFilter()  # 调用方法，不是设置属性
    except Exception as exc:
        raise RuntimeError(
            f"设置 AutoFilter 失败：sheet={sheet_name}, columns={column_count}"
        ) from exc


def build_stock_base_df(result_df: pd.DataFrame) -> pd.DataFrame:
    """
    统一从公告结果中抽取唯一证券清单。
    """
    if "secCode" not in result_df.columns:
        raise KeyError("公告结果缺少 secCode，无法进行补数。")

    stock_base_df = result_df.copy()
    for required_col in ["secName", "pageColumn", "orgId"]:
        if required_col not in stock_base_df.columns:
            stock_base_df[required_col] = pd.NA

    stock_base_df["pageColumn"] = (
        stock_base_df["pageColumn"].astype("string").str.strip().str.upper()
    )
    stock_base_df["orgId"] = stock_base_df["orgId"].astype("string").str.strip()
    route_df = build_stock_route_frame(
        stock_code_series=stock_base_df["secCode"],
        page_column_series=stock_base_df["pageColumn"],
        org_id_series=stock_base_df["orgId"],
    )

    stock_base_df = stock_base_df.loc[
        :, ["secCode", "secName", "pageColumn", "orgId"]
    ].copy()
    stock_base_df["secCode"] = route_df["secCode"].astype("string")
    stock_base_df["ths_concept_code"] = route_df["ths_concept_code"].astype("string")
    stock_base_df["ths_operate_code"] = route_df["ths_operate_code"].astype("string")
    stock_base_df["concept_supported"] = route_df["concept_supported"].astype("boolean")
    stock_base_df["secName"] = stock_base_df["secName"].astype("string").str.strip()
    stock_base_df = (
        stock_base_df.dropna(subset=["secCode"])
        .drop_duplicates(subset=["secCode"], keep="first")
        .reset_index(drop=True)
    )
    return stock_base_df


def append_missing_enrichment_records(
    error_log: pd.DataFrame,
    stock_df: pd.DataFrame,
    missing_mask: pd.Series,
    source: str,
    severity: str,
    error_type: str,
    error_message: str,
    stock_code_column: str = "secCode",
    compare_code_column: str | None = None,
) -> pd.DataFrame:
    """
    统一补充"补数后仍为空"的日志。
    """
    effective_compare_column = compare_code_column or stock_code_column
    existing_error_codes: set[str] = set()
    if not error_log.empty and "stock_code" in error_log.columns:
        existing_error_codes = set(error_log["stock_code"].dropna().astype(str))

    missing_records: list[dict[str, Any]] = []
    selected_columns: list[str] = [stock_code_column, "secName"]
    if effective_compare_column not in selected_columns:
        selected_columns.append(effective_compare_column)
    missing_df = stock_df.loc[missing_mask, selected_columns].copy()
    for row in missing_df.itertuples(index=False):
        display_code = getattr(row, stock_code_column)
        compare_code = getattr(row, effective_compare_column)
        display_code = "" if pd.isna(display_code) else str(display_code)
        compare_code = "" if pd.isna(compare_code) else str(compare_code)
        if compare_code in existing_error_codes:
            continue
        missing_records.append(
            {
                "source": source,
                "severity": severity,
                "stock_code": display_code,
                "secName": row.secName,
                "error_type": error_type,
                "error_message": error_message,
            }
        )

    return pd.concat(
        [error_log, pd.DataFrame(missing_records)],
        ignore_index=True,
        sort=False,
    )


def build_coverage_integrity_rows(
    prefix: str,
    stock_count: int,
    missing_mask: pd.Series,
    extra_metrics: list[dict[str, Any]] | None = None,
    coverage_base_count: int | None = None,
) -> list[dict[str, Any]]:
    """
    统一生成补数回填阶段的覆盖率指标。
    """
    effective_coverage_base = (
        coverage_base_count if coverage_base_count is not None else stock_count
    )
    missing_count = int(missing_mask.sum())
    coverage_pct = (
        round(
            (effective_coverage_base - missing_count) / effective_coverage_base * 100, 2
        )
        if effective_coverage_base > 0
        else 0.0
    )
    rows: list[dict[str, Any]] = [
        {"metric": f"{prefix}_stock_count", "value": int(stock_count)},
        {"metric": f"{prefix}_missing_count", "value": missing_count},
        {"metric": f"{prefix}_coverage_pct", "value": coverage_pct},
    ]
    if extra_metrics:
        rows.extend(extra_metrics)
    return rows


def enrich_announcements_with_concepts(
    result_df: pd.DataFrame,
    concept_sleep_seconds: float = CONCEPT_SLEEP_SECONDS,
    max_workers: int = 5,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    为公告结果补充同花顺涉及概念，并产出公司概念映射和概念汇总。

    Args:
        result_df: 公告 DataFrame。
        concept_sleep_seconds: 概念抓取间隔秒数。
        max_workers: 最大并发线程数，默认 5。

    Returns:
        (result_df, stock_concepts_df, concept_summary_df, concept_integrity_report, concept_error_log) 元组。
    """
    stock_base_df = build_stock_base_df(result_df)
    concept_request_codes: list[str] = (
        stock_base_df.loc[
            stock_base_df["concept_supported"].fillna(False)
            & stock_base_df["ths_concept_code"].notna()
            & stock_base_df["ths_concept_code"].astype("string").str.strip().ne(""),
            "ths_concept_code",
        ]
        .astype(str)
        .drop_duplicates()
        .tolist()
    )
    concept_result_df, concept_fetch_integrity_report, concept_fetch_error_log = (
        batch_get_stock_concepts(
            concept_request_codes,
            sleep_seconds=concept_sleep_seconds,
            max_workers=max_workers,
        )
    )

    stock_concepts_df = stock_base_df.merge(
        concept_result_df,
        left_on="ths_concept_code",
        right_on="fetch_code",
        how="left",
        validate="many_to_one",
    )
    concept_supported_mask = stock_concepts_df["concept_supported"].fillna(False)
    concept_unsupported_mask = ~concept_supported_mask
    missing_mask = concept_supported_mask & (
        stock_concepts_df["concept_list"].isna()
        | stock_concepts_df["concept_list"].astype("string").str.strip().eq("")
    )

    concept_error_log = append_missing_enrichment_records(
        error_log=concept_fetch_error_log,
        stock_df=stock_concepts_df,
        missing_mask=concept_unsupported_mask,
        source="concept_enrichment",
        severity="WARN",
        error_type="ConceptNotSupportedForHK",
        error_message="纯港股暂无同花顺涉及概念页，已跳过概念补数",
    )
    concept_error_log = append_missing_enrichment_records(
        error_log=concept_error_log,
        stock_df=stock_concepts_df,
        missing_mask=missing_mask,
        source="concept_enrichment",
        severity="ERROR",
        error_type="MissingConceptData",
        error_message="同花顺涉及概念补数后仍为空",
        stock_code_column="secCode",
        compare_code_column="ths_concept_code",
    )

    enriched_df = result_df.merge(
        stock_concepts_df.loc[
            :,
            [
                "secCode",
                "concept_count",
                "concept_list",
                "concept_text_raw",
                "concept_source_url",
            ],
        ],
        on="secCode",
        how="left",
        validate="many_to_one",
    )
    concept_summary_df = build_concept_summary(enriched_df)
    concept_supported_stock_count: int = int(concept_supported_mask.sum())
    concept_unsupported_hk_count: int = int(concept_unsupported_mask.sum())

    concept_integrity_report = pd.concat(
        [
            concept_fetch_integrity_report,
            pd.DataFrame(
                build_coverage_integrity_rows(
                    prefix="concept",
                    stock_count=len(stock_concepts_df),
                    missing_mask=missing_mask,
                    extra_metrics=[
                        {
                            "metric": "concept_supported_stock_count",
                            "value": concept_supported_stock_count,
                        },
                        {
                            "metric": "concept_unsupported_hk_count",
                            "value": concept_unsupported_hk_count,
                        },
                        {
                            "metric": "unique_concept_count",
                            "value": int(len(concept_summary_df)),
                        },
                    ],
                    coverage_base_count=concept_supported_stock_count,
                )
            ),
        ],
        ignore_index=True,
    )

    return (
        enriched_df,
        stock_concepts_df,
        concept_summary_df,
        concept_integrity_report,
        concept_error_log,
    )


def enrich_announcements_with_operate(
    result_df: pd.DataFrame,
    operate_sleep_seconds: float = OPERATE_SLEEP_SECONDS,
    max_workers: int = 5,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    为公告结果补充同花顺主营介绍。

    Args:
        result_df: 公告 DataFrame。
        operate_sleep_seconds: 主营抓取间隔秒数。
        max_workers: 最大并发线程数，默认 5。

    Returns:
        (result_df, stock_operate_df, operate_integrity_report, operate_error_log) 元组。
    """
    stock_base_df = build_stock_base_df(result_df)
    operate_request_codes: list[str] = (
        stock_base_df.loc[
            stock_base_df["ths_operate_code"].notna()
            & stock_base_df["ths_operate_code"].astype("string").str.strip().ne(""),
            "ths_operate_code",
        ]
        .astype(str)
        .drop_duplicates()
        .tolist()
    )
    operate_result_df, operate_integrity_report, operate_error_log = (
        batch_get_stock_operate(
            operate_request_codes,
            sleep_seconds=operate_sleep_seconds,
            max_workers=max_workers,
        )
    )
    stock_operate_df = stock_base_df.merge(
        operate_result_df,
        left_on="ths_operate_code",
        right_on="fetch_code",
        how="left",
        validate="many_to_one",
    )

    operate_core_columns: list[str] = ["主营业务", "产品类型", "产品名称", "经营范围"]
    missing_mask = ~build_any_non_empty_text_mask(
        stock_operate_df, operate_core_columns
    )
    operate_error_log = append_missing_enrichment_records(
        error_log=operate_error_log,
        stock_df=stock_operate_df,
        missing_mask=missing_mask,
        source="operate_enrichment",
        severity="WARN",
        error_type="MissingOperateData",
        error_message="同花顺主营补数后仍为空",
        stock_code_column="secCode",
        compare_code_column="ths_operate_code",
    )

    enriched_df = result_df.merge(
        stock_operate_df.loc[
            :,
            [
                "secCode",
                "主营业务",
                "产品类型",
                "产品名称",
                "经营范围",
                "产品类型_list",
                "产品名称_list",
            ],
        ],
        on="secCode",
        how="left",
        validate="many_to_one",
    )

    operate_integrity_report = pd.concat(
        [
            operate_integrity_report,
            pd.DataFrame(
                build_coverage_integrity_rows(
                    prefix="operate",
                    stock_count=len(stock_operate_df),
                    missing_mask=missing_mask,
                )
            ),
        ],
        ignore_index=True,
    )

    return enriched_df, stock_operate_df, operate_integrity_report, operate_error_log


def run_data_integrity_check(
    df: pd.DataFrame, expected_total: int | None
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    数据完整性校验（Data Integrity Check）
    """
    issues: list[dict[str, Any]] = []

    row_count: int = int(len(df))
    unique_announcement_id_count: int = (
        int(df["announcementId"].nunique(dropna=True))
        if "announcementId" in df.columns
        else 0
    )

    duplicate_count: int = (
        int(df.duplicated(subset=["announcementId"]).sum())
        if "announcementId" in df.columns
        else int(df.duplicated().sum())
    )

    critical_cols: list[str] = [
        "announcementId",
        "secCode",
        "secName",
        "announcementTitle",
        "adjunctUrl",
    ]
    existing_critical_cols: list[str] = [c for c in critical_cols if c in df.columns]

    if existing_critical_cols:
        null_rate_series = df[existing_critical_cols].isna().mean().mul(100).round(2)
    else:
        null_rate_series = pd.Series(dtype="float64")

    if row_count == 0:
        issues.append(
            {
                "check_name": "empty_result",
                "severity": "ERROR",
                "detail": "结果集为空，请检查关键词、日期范围、风控状态或接口字段是否变更。",
            }
        )

    if duplicate_count > 0:
        issues.append(
            {
                "check_name": "duplicate_announcement_id",
                "severity": "ERROR",
                "detail": f"announcementId 存在重复，重复数={duplicate_count}",
            }
        )

    for col, rate in null_rate_series.items():
        if rate > 5:
            issues.append(
                {
                    "check_name": f"null_rate_{col}",
                    "severity": "WARN",
                    "detail": f"{col} 空值率为 {rate}%，超过阈值 5%。",
                }
            )

    if "announcement_datetime" in df.columns:
        invalid_time_count: int = int(df["announcement_datetime"].isna().sum())
        if row_count > 0 and invalid_time_count > 0:
            issues.append(
                {
                    "check_name": "invalid_announcement_time",
                    "severity": "WARN",
                    "detail": f"announcementTime 无法解析的记录数={invalid_time_count}",
                }
            )

    if expected_total is not None and row_count > expected_total:
        issues.append(
            {
                "check_name": "row_count_exceeds_expected_total",
                "severity": "ERROR",
                "detail": f"抓取行数 {row_count} 大于接口声称总量 {expected_total}",
            }
        )

    summary_rows: list[dict[str, Any]] = [
        {"metric": "row_count", "value": row_count},
        {
            "metric": "unique_announcement_id_count",
            "value": unique_announcement_id_count,
        },
        {"metric": "duplicate_count", "value": duplicate_count},
        {
            "metric": "expected_total",
            "value": expected_total if expected_total is not None else pd.NA,
        },
    ]
    for col, rate in null_rate_series.items():
        summary_rows.append({"metric": f"null_rate_{col}_pct", "value": rate})

    integrity_report = pd.DataFrame(summary_rows)
    error_log = pd.DataFrame(issues)

    return integrity_report, error_log


def crawl_cninfo(
    config: CrawlConfig,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    主流程：
    - 分页抓取
    - 标准化
    - 完整性校验
    """
    session = build_session(config.search_key)

    page_num: int = 1
    expected_total: int | None = None
    total_pages: int | None = None

    all_frames: list[pd.DataFrame] = []
    page_error_records: list[dict[str, Any]] = []

    while True:
        try:
            payload = fetch_page(session, config, page_num)

            if expected_total is None:
                if isinstance(payload.get("totalAnnouncement"), int):
                    expected_total = int(payload["totalAnnouncement"])
                elif isinstance(payload.get("totalRecordNum"), int):
                    expected_total = int(payload["totalRecordNum"])

                if expected_total is not None:
                    total_pages = math.ceil(expected_total / config.page_size)
                    logger.info(
                        "接口总量=%s, 预计总页数=%s",
                        expected_total,
                        total_pages,
                    )

            records: Any = payload.get("announcements")
            if not isinstance(records, list):
                raise KeyError(
                    f"payload 中不存在 announcements 列表，当前顶层 keys={list(payload.keys())}"
                )

            logger.info("第 %s 页返回 %s 条记录。", page_num, len(records))

            if len(records) == 0:
                logger.info("第 %s 页为空，结束抓取。", page_num)
                break

            page_df = normalize_records(records)
            page_df["page_num"] = page_num
            all_frames.append(page_df)

        except Exception as exc:
            logger.exception("第 %s 页抓取失败。", page_num)
            page_error_records.append(
                make_error_record(
                    source="cninfo_page_fetch",
                    severity="ERROR",
                    error_type=type(exc).__name__,
                    message=str(exc),
                    error_code="CNINFO_PAGE_FETCH_FAILED",
                    retryable=True,
                    page_num=page_num,
                    action="跳过当前页并继续后续分页",
                )
            )

        if total_pages is not None and page_num >= total_pages:
            break

        page_num += 1
        time.sleep(
            random.uniform(config.sleep_min_seconds, config.sleep_max_seconds)
        )

    if not all_frames:
        raise RuntimeError(
            "所有页面均未成功抓取；请重点检查 Referer、Cookie 初始化、网络权限、访问频率，"
            "以及接口字段是否发生变化。"
        )

    result_df = pd.concat(all_frames, ignore_index=True)

    if "announcementId" in result_df.columns:
        result_df = result_df.drop_duplicates(
            subset=["announcementId"], keep="first"
        ).copy()

    integrity_report, integrity_error_log = run_data_integrity_check(
        result_df,
        expected_total=expected_total,
    )

    page_error_df = pd.DataFrame(page_error_records)
    final_error_log = pd.concat(
        [page_error_df, integrity_error_log],
        ignore_index=True,
        sort=False,
    )

    return result_df, integrity_report, final_error_log


def build_run_summary(
    run_id: str,
    config: CrawlConfig,
    result_df: pd.DataFrame,
    error_log: pd.DataFrame,
    integrity_report: pd.DataFrame,
) -> pd.DataFrame:
    """
    生成单次运行摘要，便于在 WPS 工作表快速复核。
    """
    executed_at = datetime.utcnow() + timedelta(hours=8)
    error_count = 0 if error_log.empty else len(error_log)
    blocking_error_count = (
        0
        if error_log.empty
        else int(
            error_log["severity"].astype("string").str.upper().eq("ERROR").sum()
        )
    )

    return pd.DataFrame(
        [
            {"metric": "run_id", "value": run_id},
            {
                "metric": "executed_at_beijing",
                "value": executed_at.strftime("%Y-%m-%d %H:%M:%S"),
            },
            {"metric": "search_key", "value": config.search_key},
            {"metric": "start_date", "value": config.start_date},
            {"metric": "end_date", "value": config.end_date},
            {"metric": "page_size", "value": config.page_size},
            {"metric": "result_row_count", "value": len(result_df)},
            {"metric": "error_count", "value": error_count},
            {"metric": "blocking_error_count", "value": blocking_error_count},
            {"metric": "integrity_metric_count", "value": len(integrity_report)},
        ]
    )


def write_outputs_to_wps(
    run_summary_df: pd.DataFrame,
    result_df: pd.DataFrame,
    error_log: pd.DataFrame,
    integrity_report: pd.DataFrame,
    concept_summary_df: pd.DataFrame,
) -> None:
    """
    将最终结果直接写回 WPS 在线表格。
    """
    export_cols: list[str] = [
        "page_num",
        "announcementId",
        "secCode",
        "secName",
        "concept_count",
        "concept_list",
        "主营业务",
        "产品类型",
        "产品名称",
        "经营范围",
        "产品类型_list",
        "产品名称_list",
        "announcement_title_clean",
        "announcement_date",
        "pdf_url",
        "adjunctType",
        "adjunctSize",
        "columnId",
        "pageColumn",
        "announcementType",
        "concept_text_raw",
        "concept_source_url",
    ]
    existing_export_cols: list[str] = [c for c in export_cols if c in result_df.columns]

    export_df = result_df.loc[:, existing_export_cols].copy()
    if "secCode" in export_df.columns:
        # 转换为字符串，并添加前导单引号强制 WPS 识别为文本
        # 单引号在 WPS/Excel 中是文本前缀标记，不会显示在单元格中
        export_df["secCode"] = (
            export_df["secCode"]
            .astype("string")
            .apply(lambda x: "'" + str(x) if pd.notna(x) else "")
        )

    export_df = add_keyword_hit_flags(export_df)

    clear_sheet_formats(RESULT_SHEET)
    write_sheet_df(export_df, RESULT_SHEET)
    apply_secCode_text_format(RESULT_SHEET, len(export_df))
    apply_header_auto_filter(RESULT_SHEET, len(export_df.columns))
    apply_keyword_highlight(RESULT_SHEET, export_df)
    write_sheet_df(error_log, ERROR_LOG_SHEET)
    write_sheet_df(integrity_report, INTEGRITY_REPORT_SHEET)
    write_sheet_df(concept_summary_df, CONCEPT_SUMMARY_SHEET)
    write_sheet_df(run_summary_df, RUN_SUMMARY_SHEET)


def has_blocking_errors(error_log: pd.DataFrame) -> bool:
    """
    判断是否存在需要中止交付的阻断性错误。
    """
    if error_log.empty:
        return False

    if "severity" not in error_log.columns:
        return True

    return error_log["severity"].astype("string").str.upper().eq("ERROR").any()


def main() -> None:
    """
    脚本入口。
    """
    assert_wps_runtime()
    run_id = build_run_id()
    config, config_error_log, config_integrity_report = load_runtime_config()

    result_df, integrity_report, error_log = crawl_cninfo(config)
    (
        result_df,
        _stock_concepts_df,
        concept_summary_df,
        concept_integrity_report,
        concept_error_log,
    ) = enrich_announcements_with_concepts(
        result_df,
        concept_sleep_seconds=config.concept_sleep_seconds,
        max_workers=config.max_concurrent_workers,
    )
    result_df, _stock_operate_df, operate_integrity_report, operate_error_log = (
        enrich_announcements_with_operate(
            result_df,
            operate_sleep_seconds=config.operate_sleep_seconds,
            max_workers=config.max_concurrent_workers,
        )
    )
    integrity_report = pd.concat(
        [
            config_integrity_report,
            integrity_report,
            concept_integrity_report,
            operate_integrity_report,
        ],
        ignore_index=True,
        sort=False,
    )
    error_log = pd.concat(
        [config_error_log, error_log, concept_error_log, operate_error_log],
        ignore_index=True,
        sort=False,
    )
    error_log = normalize_error_log(error_log, run_id=run_id)
    run_summary_df = build_run_summary(
        run_id=run_id,
        config=config,
        result_df=result_df,
        error_log=error_log,
        integrity_report=integrity_report,
    )

    logger.info("开始输出结果：run_id=%s, result_rows=%s", run_id, len(result_df))
    logger.info("完整性校验摘要：\n%s", integrity_report.to_string(index=False))

    if not error_log.empty:
        logger.warning(
            "存在异常日志，请人工复核：\n%s", error_log.to_string(index=False)
        )

    write_outputs_to_wps(
        run_summary_df=run_summary_df,
        result_df=result_df,
        error_log=error_log,
        integrity_report=integrity_report,
        concept_summary_df=concept_summary_df,
    )

    if has_blocking_errors(error_log):
        raise RuntimeError("存在阻断性错误，详见 error_log 工作表，请人工复核后重试。")


if __name__ == "__main__":
    main()
