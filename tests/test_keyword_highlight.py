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
class FakeCharacters:
    font: FakeFont = field(default_factory=FakeFont)
    interior: FakeInterior = field(default_factory=FakeInterior)
    call_args: tuple[int, int] | None = None

    @property
    def Font(self) -> FakeFont:
        return self.font

    @property
    def Interior(self) -> FakeInterior:
        return self.interior


@dataclass
class FakeCell:
    Font: FakeFont = field(default_factory=FakeFont)
    Interior: FakeInterior = field(default_factory=FakeInterior)
    clear_count: int = 0
    characters_calls: list[FakeCharacters] = field(default_factory=list)

    def ClearFormats(self) -> None:
        self.Font.Color = None
        self.Interior.Color = None
        self.clear_count += 1

    def Characters(self, start: int, length: int) -> FakeCharacters:
        chars = FakeCharacters()
        chars.call_args = (start, length)
        self.characters_calls.append(chars)
        return chars


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
    # "激光股份" contains "激光", so Characters() should be called for partial highlighting
    assert len(hit_cell.characters_calls) == 1
    assert hit_cell.characters_calls[0].Font.Color == spider.parse_wps_color_argb(
        spider.KEYWORD_HIGHLIGHT_COLOR
    )
    assert hit_cell.characters_calls[0].Interior.Color == spider.parse_wps_color_argb(
        spider.KEYWORD_HIGHLIGHT_FILL_COLOR
    )
    assert miss_cell.clear_count == 1
    assert miss_cell.Font.Color is None
    assert miss_cell.Interior.Color is None
    assert len(miss_cell.characters_calls) == 0


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


def test_apply_keyword_highlight_falls_back_to_cell_level_when_characters_unsupported(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test fallback to cell-level highlighting when Characters() is not supported."""

    @dataclass
    class FakeCellNoCharacters:
        Font: FakeFont = field(default_factory=FakeFont)
        Interior: FakeInterior = field(default_factory=FakeInterior)
        clear_count: int = 0

        def ClearFormats(self) -> None:
            self.Font.Color = None
            self.Interior.Color = None
            self.clear_count += 1

    class FakeWorksheetNoCharacters:
        def __init__(self) -> None:
            self._cells: dict[tuple[int, int], FakeCellNoCharacters] = {}

        def Cells(self, row: int, column: int) -> FakeCellNoCharacters:
            return self._cells.setdefault((row, column), FakeCellNoCharacters())

    worksheet = FakeWorksheetNoCharacters()
    result_df = pd.DataFrame(
        {
            "secName": ["激光股份"],
            "hit_secName": [True],
            "keyword_hit_any": [True],
        }
    )

    monkeypatch.setattr(spider, "get_worksheet", lambda sheet_name: worksheet)

    spider.apply_keyword_highlight(spider.RESULT_SHEET, result_df)

    hit_cell = worksheet.Cells(2, 1)
    # Should fall back to cell-level highlighting
    assert hit_cell.Font.Color == spider.parse_wps_color_argb(
        spider.KEYWORD_HIGHLIGHT_COLOR
    )
    assert hit_cell.Interior.Color == spider.parse_wps_color_argb(
        spider.KEYWORD_HIGHLIGHT_FILL_COLOR
    )


def test_apply_keyword_highlight_falls_back_when_no_keyword_match_in_hit_text(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test fallback to cell-level when hit_flag is True but text has no keyword match."""
    worksheet = FakeWorksheet()
    # Text "测试公司" doesn't contain any keywords, but hit_flag is True
    result_df = pd.DataFrame(
        {
            "secName": ["测试公司"],
            "hit_secName": [True],  # Manually set to True
            "keyword_hit_any": [True],
        }
    )

    monkeypatch.setattr(spider, "get_worksheet", lambda sheet_name: worksheet)

    spider.apply_keyword_highlight(spider.RESULT_SHEET, result_df)

    hit_cell = worksheet.Cells(2, 1)
    # Should fall back to cell-level highlighting since no keyword match
    assert len(hit_cell.characters_calls) == 0
    assert hit_cell.Font.Color == spider.parse_wps_color_argb(
        spider.KEYWORD_HIGHLIGHT_COLOR
    )
    assert hit_cell.Interior.Color == spider.parse_wps_color_argb(
        spider.KEYWORD_HIGHLIGHT_FILL_COLOR
    )
