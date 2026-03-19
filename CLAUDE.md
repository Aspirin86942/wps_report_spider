# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

WPS Report Spider - A web scraper for CNINFO (巨潮资讯) announcement data with WPS Online Spreadsheet integration. Fetches announcements about "内部控制评价" (internal control evaluation) for Chinese A-share and HK-listed companies, enriches with THS (同花顺) concept and business scope data, and writes results back to WPS.

## Commands

```bash
# Run tests (project uses conda env named "test")
conda run -n wps pytest

# Run specific test file
conda run -n wps pytest tests/test_core_logic.py

# Run specific test function
conda run -n wps pytest tests/test_keyword_highlight.py::test_add_keyword_hit_flags_marks_target_columns

# Run with coverage
conda run -n wps pytest --cov=wps_report_spider

# Lint (if ruff configured)
conda run -n wps ruff check wps_report_spider.py tests/
```

## Architecture

### Entry Point
- `wps_report_spider.py:main()` - Script entry point that orchestrates the full pipeline

### Data Flow
```
load_runtime_config() → crawl_cninfo() → enrich_with_concepts() → enrich_with_operate() → write_outputs_to_wps()
```

### Core Components

**Configuration Layer**
- `load_runtime_config()` - Reads from `config` sheet or falls back to `DEFAULT_CRAWL_CONFIG`
- `CrawlConfig` dataclass - Runtime configuration (search_key, date range, page_size, sleep settings)
- `normalize_config_sheet()` - Normalizes Chinese/English column aliases to key/value pairs

**CNINFO Crawler**
- `crawl_cninfo()` - Paginated scraping with session management
- `build_session()` - Creates requests Session with retry adapter and Referer warmup
- `fetch_page()` - Fetches single page from CNINFO API
- `normalize_records()` - Transforms API response to DataFrame

**THS Enrichment**
- `enrich_announcements_with_concepts()` - Fetches stock concept data from 同花顺
- `enrich_announcements_with_operate()` - Fetches business scope (主营业务) from 同花顺
- `build_concept_session()` - Separate session for THS requests

**Data Integrity**
- `run_data_integrity_check()` - Validates row counts, duplicates, null rates on critical columns
- `normalize_error_log()` - Unified error schema with severity, error_code, retryable flags
- `build_run_summary()` - Execution metadata for audit trail

**WPS Integration**
- `xl()` / `write_xl()` - WPS built-in functions (injected at runtime)
- `read_sheet_df()` / `write_sheet_df()` - DataFrame ↔ WPS sheet I/O
- `assert_wps_runtime()` - Fails fast if not running in WPS PY editor

**Keyword Highlighting**
- `add_keyword_hit_flags()` - Marks rows matching `KEYWORD_HIGHLIGHT_WORDS` (激光，自动化，etc.)
- `apply_keyword_highlight()` - Applies WPS cell formatting (font/fill color)

### Worksheets
| Sheet | Purpose |
|-------|---------|
| `config` | Runtime parameters (key/value) |
| `result` | Final enriched announcement data |
| `error_log` | Structured error records with run_id |
| `integrity_report` | Data quality metrics |
| `concept_summary` | THS concept cache summary |
| `run_summary` | Execution metadata |
| `_cache_concept` | THS concept cache (internal) |
| `_cache_operate` | THS business scope cache (internal) |

### Testing
- Tests use `monkeypatch` to mock WPS runtime (`xl`, `write_xl`, `Application`)
- `tests/conftest.py` adds project root to `sys.path`
- Test files: `test_core_logic.py`, `test_runtime_config.py`, `test_keyword_highlight.py`
- Fake WPS objects defined in test files for format testing

## Key Constraints

- **WPS Runtime Required**: Must run in WPS Online Spreadsheet PY editor; `assert_wps_runtime()` enforces this
- **UTF-8 Encoding**: All text files must use UTF-8
- **Audit-First Design**: All errors logged with severity levels (ERROR/WARN), retryable flags, and run_id for traceability
- **No Silent Failures**: Missing config sheet logs WARN and falls back to defaults; data integrity checks emit ERROR for critical issues

## Optimizations (2026-03-18)

### Performance
- **Concurrent Scraping**: `collect_miss_code_results()` now supports `ThreadPoolExecutor` with configurable `max_workers` (default 5)
- **Vectorized Operations**: `build_any_non_empty_text_mask()` uses `map()` instead of `apply()` for pandas compatibility
- **Configurable Timeout**: `per_request_timeout` in `CrawlConfig` (default 30s)

### Security
- **Input Validation**: `load_runtime_config()` validates date format (YYYY-MM-DD), page_size range (1-100), search_key non-empty
- **Response Size Check**: `fetch_page()` rejects responses > 5MB (DoS protection)
- **Content-Type Validation**: All HTTP responses validated for expected content type (JSON/HTML)

### Configuration
New config sheet keys supported:
| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `max_concurrent_workers` | int | 5 | Maximum concurrent threads for THS scraping |
| `per_request_timeout` | float | 30.0 | Single request timeout in seconds |

### Code Quality
- Enhanced docstrings with Args/Returns/Raises sections
- Type hints completed for all modified functions
- Error handling improved in concurrent execution paths
