import os
import pytest
from unittest.mock import patch
from common.env import get_env_or_default, get_env_int_or_default
from common.string_utils import to_lower_copy, is_valid_utf8, sanitize_utf8_lossy
from common.time_utils import now_utc_iso8601
from common.logging import parse_log_level
import spdlog

# ----------------------------------------------------------------------
# Environment tests
# ----------------------------------------------------------------------
def test_get_env_or_default_missing():
    with patch.dict(os.environ, {}, clear=True):
        assert get_env_or_default("MISSING", "fallback") == "fallback"

def test_get_env_or_default_present():
    with patch.dict(os.environ, {"TEST_ENV": "value"}):
        assert get_env_or_default("TEST_ENV", "fallback") == "value"

def test_get_env_int_or_default_missing():
    with patch.dict(os.environ, {}, clear=True):
        assert get_env_int_or_default("MISSING", 7) == 7

def test_get_env_int_or_default_valid():
    with patch.dict(os.environ, {"TEST_INT": "42"}):
        assert get_env_int_or_default("TEST_INT", 7) == 42

def test_get_env_int_or_default_invalid():
    with patch.dict(os.environ, {"TEST_INT": "12x"}):
        assert get_env_int_or_default("TEST_INT", 7) == 7

# ----------------------------------------------------------------------
# String tests
# ----------------------------------------------------------------------
def test_to_lower_copy():
    assert to_lower_copy("HeLLo123") == "hello123"
    assert to_lower_copy("already_lower") == "already_lower"

def test_is_valid_utf8():
    assert is_valid_utf8("plain ascii")
    assert is_valid_utf8("hello \xd0\xbc\xd0\xb8\xd1\x80")  # Russian

def test_sanitize_utf8_lossy():
    bad = "abc" + bytes([0x8A]).decode('utf-8', errors='ignore') + "z"
    # Actually, we need to construct a bytes string with invalid UTF-8
    # Let's use bytes then decode with replace
    import sys
    if sys.version_info >= (3,):
        b = b"abc\x8Az"
        sanitized = sanitize_utf8_lossy(b.decode('utf-8', errors='replace'))
    else:
        b = b"abc\x8Az"
        sanitized = sanitize_utf8_lossy(b.decode('utf-8', errors='replace'))
    assert is_valid_utf8(sanitized)

# ----------------------------------------------------------------------
# Time tests
# ----------------------------------------------------------------------
def test_now_utc_iso8601_format():
    ts = now_utc_iso8601()
    # basic check: length and characters
    assert len(ts) == 20  # YYYY-MM-DDTHH:MM:SSZ
    assert ts[4] == '-' and ts[7] == '-' and ts[10] == 'T'
    assert ts[13] == ':' and ts[16] == ':' and ts[19] == 'Z'

# ----------------------------------------------------------------------
# Logging tests
# ----------------------------------------------------------------------
def test_parse_log_level_aliases():
    assert parse_log_level("trace", spdlog.level.warn) == spdlog.level.trace
    assert parse_log_level("DEBUG", spdlog.level.warn) == spdlog.level.debug
    assert parse_log_level("info", spdlog.level.warn) == spdlog.level.info
    assert parse_log_level("warning", spdlog.level.warn) == spdlog.level.warn
    assert parse_log_level("err", spdlog.level.warn) == spdlog.level.err
    assert parse_log_level("crit", spdlog.level.warn) == spdlog.level.critical
    assert parse_log_level("off", spdlog.level.warn) == spdlog.level.off

def test_parse_log_level_fallback():
    assert parse_log_level("nope", spdlog.level.info) == spdlog.level.info
    assert parse_log_level("", spdlog.level.info) == spdlog.level.info
