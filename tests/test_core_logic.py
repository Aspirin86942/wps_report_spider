from __future__ import annotations

import pandas as pd
import pytest

import wps_report_spider as spider


def test_normalize_error_log_unifies_columns() -> None:
    error_log = pd.DataFrame(
        [
            {
                "source": "config",
                "severity": "warn",
                "error_type": "Bad Value",
                "detail": "配置非法",
            }
        ]
    )

    normalized = spider.normalize_error_log(error_log, run_id="run-1")

    assert list(normalized.columns) == spider.ERROR_LOG_COLUMNS
    assert normalized.loc[0, "run_id"] == "run-1"
    assert normalized.loc[0, "severity"] == "WARN"
    assert normalized.loc[0, "error_code"] == "BAD_VALUE"
    assert normalized.loc[0, "message"] == "配置非法"


def test_build_stock_route_frame_handles_a_share_hk_and_ah() -> None:
    route_df = spider.build_stock_route_frame(
        stock_code_series=pd.Series(["600031", "267", "267"]),
        page_column_series=pd.Series(["szse", "HKZB", "HKZB"]),
        org_id_series=pd.Series(["gssh600031", "gssh0600519", "gshk000267"]),
    )

    assert route_df.loc[0, "ths_concept_code"] == "600031"
    assert bool(route_df.loc[1, "concept_supported"]) is True
    assert route_df.loc[1, "ths_operate_code"] == "600519"
    assert pd.isna(route_df.loc[2, "ths_concept_code"])
    assert route_df.loc[2, "ths_operate_code"] == "HK0267"


def test_parse_operate_intro_reports_duplicates_and_missing_fields() -> None:
    html_text = """
    <div id="intro">
      <ul class="main_intro_list">
        <li><span>主营业务：</span><p>激光设备</p></li>
        <li><span>主营业务：</span><p>焊接机器人</p></li>
      </ul>
    </div>
    """

    result_df, error_log = spider.parse_operate_intro(html_text)

    assert len(result_df) == 2
    assert "duplicate_field" in error_log["error_type"].tolist()
    assert "missing_expected_field" in error_log["error_type"].tolist()


def test_run_data_integrity_check_reports_row_counts_and_null_rates() -> None:
    input_df = pd.DataFrame(
        {
            "announcementId": ["a1", "a1"],
            "secCode": ["600031", None],
            "secName": ["三一重工", "三一重工"],
            "announcementTitle": ["标题1", "标题2"],
            "adjunctUrl": ["/a.pdf", None],
            "announcement_datetime": [pd.Timestamp("2026-01-01"), pd.NaT],
        }
    )

    integrity_report, error_log = spider.run_data_integrity_check(
        input_df,
        expected_total=1,
    )

    metrics = dict(zip(integrity_report["metric"], integrity_report["value"]))
    assert metrics["row_count"] == 2
    assert metrics["duplicate_count"] == 1
    assert "null_rate_secCode_pct" in metrics
    assert "row_count_exceeds_expected_total" in error_log["check_name"].tolist()


def test_read_sheet_df_strict_raises_on_xl_failure(monkeypatch) -> None:
    def raise_xl(**kwargs: object) -> pd.DataFrame:
        raise RuntimeError("boom")

    monkeypatch.setattr(spider, "sheet_exists", lambda sheet_name: True)
    monkeypatch.setattr(spider, "xl", raise_xl)

    with pytest.raises(RuntimeError, match="读取工作表失败"):
        spider.read_sheet_df("config", strict=True)
