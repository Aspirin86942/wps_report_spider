from __future__ import annotations

import pandas as pd

import wps_report_spider as spider


def test_normalize_config_sheet_accepts_chinese_headers() -> None:
    config_df = pd.DataFrame(
        {
            "配置项": ["search_key", "page_size"],
            "值": ["内部控制", "50"],
        }
    )

    result = spider.normalize_config_sheet(config_df)

    assert result.to_dict(orient="records") == [
        {"key": "search_key", "value": "内部控制"},
        {"key": "page_size", "value": "50"},
    ]


def test_load_runtime_config_falls_back_to_defaults_when_sheet_missing(
    monkeypatch,
) -> None:
    monkeypatch.setattr(spider, "sheet_exists", lambda sheet_name: False)

    config, error_log, integrity_report = spider.load_runtime_config()

    assert config == spider.DEFAULT_CRAWL_CONFIG
    assert error_log.loc[0, "error_code"] == "CONFIG_SHEET_MISSING"
    metrics = dict(zip(integrity_report["metric"], integrity_report["value"]))
    assert metrics["config_sheet_exists"] is False


def test_load_runtime_config_parses_sheet_values(monkeypatch) -> None:
    monkeypatch.setattr(spider, "sheet_exists", lambda sheet_name: True)
    monkeypatch.setattr(
        spider,
        "read_sheet_df",
        lambda *args, **kwargs: pd.DataFrame(
            {
                "参数": ["search_key", "page_size", "is_fulltext"],
                "参数值": ["工业控制", "50", "true"],
            }
        ),
    )

    config, error_log, _ = spider.load_runtime_config()

    assert error_log.empty
    assert config.search_key == "工业控制"
    assert config.page_size == 50
    assert config.is_fulltext is True


def test_load_runtime_config_invalid_sleep_window_resets_defaults(
    monkeypatch,
) -> None:
    monkeypatch.setattr(spider, "sheet_exists", lambda sheet_name: True)
    monkeypatch.setattr(
        spider,
        "read_sheet_df",
        lambda *args, **kwargs: pd.DataFrame(
            {
                "key": ["sleep_min_seconds", "sleep_max_seconds"],
                "value": ["2", "1"],
            }
        ),
    )

    config, error_log, _ = spider.load_runtime_config()

    assert config.sleep_min_seconds == spider.DEFAULT_CRAWL_CONFIG.sleep_min_seconds
    assert config.sleep_max_seconds == spider.DEFAULT_CRAWL_CONFIG.sleep_max_seconds
    assert "CONFIG_SLEEP_WINDOW_INVALID" in error_log["error_code"].tolist()
