from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd
import pytest

import wps_report_spider as spider


@dataclass
class FakeFont:
    Color: int | None = None


@dataclass
class FakeInterior:
    Color: int | None = None


@dataclass
class FakeCell:
    Font: FakeFont = field(default_factory=FakeFont)
    Interior: FakeInterior = field(default_factory=FakeInterior)
    clear_count: int = 0

    def ClearFormats(self) -> None:
        self.Font.Color = None
        self.Interior.Color = None
        self.clear_count += 1


class FakeWorksheet:
    def __init__(self) -> None:
        self._cells: dict[tuple[int, int], FakeCell] = {}

    def Cells(self, row: int, column: int) -> FakeCell:
        return self._cells.setdefault((row, column), FakeCell())


def test_add_keyword_hit_flags_marks_target_columns() -> None:
    result_df = pd.DataFrame(
        {
            "secName": ["激光股份", "普通公司"],
            "announcement_title_clean": ["自动化改造公告", None],
            "concept_list": ["工控、机器人", ""],
        }
    )

    highlighted_df = spider.add_keyword_hit_flags(result_df)

    assert highlighted_df["hit_secName"].tolist() == [True, False]
    assert highlighted_df["hit_announcement_title_clean"].tolist() == [True, False]
    assert highlighted_df["hit_concept_list"].tolist() == [True, False]
    assert highlighted_df["keyword_hit_any"].tolist() == [True, False]


def test_add_keyword_hit_flags_handles_empty_frame() -> None:
    result_df = pd.DataFrame(columns=["secName"])

    highlighted_df = spider.add_keyword_hit_flags(result_df)

    assert highlighted_df.empty
    assert "keyword_hit_any" in highlighted_df.columns


def test_add_keyword_hit_flags_handles_null_values() -> None:
    result_df = pd.DataFrame(
        {
            "secName": [None],
            "announcement_title_clean": [pd.NA],
            "concept_list": [None],
        }
    )

    highlighted_df = spider.add_keyword_hit_flags(result_df)

    assert highlighted_df["hit_secName"].tolist() == [False]
    assert highlighted_df["hit_announcement_title_clean"].tolist() == [False]
    assert highlighted_df["hit_concept_list"].tolist() == [False]
    assert highlighted_df["keyword_hit_any"].tolist() == [False]


def test_parse_wps_color_argb_accepts_valid_argb() -> None:
    assert spider.parse_wps_color_argb("FFFF0000") == 255
    assert spider.parse_wps_color_argb("FFFFF2CC") == 13_431_551


@pytest.mark.parametrize("color", ["", "FFF", "GGFF0000", "FFFF000"])
def test_parse_wps_color_argb_rejects_invalid_argb(color: str) -> None:
    with pytest.raises(ValueError, match="ARGB"):
        spider.parse_wps_color_argb(color)


def test_apply_keyword_highlight_colors_hit_cells_and_clears_old_style(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worksheet = FakeWorksheet()
    worksheet.Cells(2, 1).Font.Color = 123
    worksheet.Cells(3, 1).Interior.Color = 456

    result_df = pd.DataFrame(
        {
            "secName": ["激光股份", "普通公司"],
            "hit_secName": [True, False],
            "keyword_hit_any": [True, False],
        }
    )

    monkeypatch.setattr(spider, "get_worksheet", lambda sheet_name: worksheet)

    spider.apply_keyword_highlight(spider.RESULT_SHEET, result_df)

    hit_cell = worksheet.Cells(2, 1)
    miss_cell = worksheet.Cells(3, 1)
    assert hit_cell.clear_count == 1
    assert hit_cell.Font.Color == spider.parse_wps_color_argb(
        spider.KEYWORD_HIGHLIGHT_COLOR
    )
    assert hit_cell.Interior.Color == spider.parse_wps_color_argb(
        spider.KEYWORD_HIGHLIGHT_FILL_COLOR
    )
    assert miss_cell.clear_count == 1
    assert miss_cell.Font.Color is None
    assert miss_cell.Interior.Color is None


def test_apply_keyword_highlight_skips_missing_export_column(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worksheet = FakeWorksheet()
    result_df = pd.DataFrame(
        {
            "announcementId": ["a1"],
            "keyword_hit_any": [False],
        }
    )

    monkeypatch.setattr(spider, "get_worksheet", lambda sheet_name: worksheet)

    spider.apply_keyword_highlight(spider.RESULT_SHEET, result_df)

    assert worksheet._cells == {}


def test_apply_keyword_highlight_raises_when_hit_column_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worksheet = FakeWorksheet()
    result_df = pd.DataFrame({"secName": ["激光股份"], "keyword_hit_any": [True]})

    monkeypatch.setattr(spider, "get_worksheet", lambda sheet_name: worksheet)

    with pytest.raises(KeyError, match="hit_secName"):
        spider.apply_keyword_highlight(spider.RESULT_SHEET, result_df)
