"""
Unit tests for datetime utilities
"""
import pytest
from datetime import datetime, timezone, timedelta

from src.utils.datetime_utils import (
    ensure_utc,
    add_hours,
    truncate_to_hour,
    get_current_hour_start,
    get_previous_hour_start,
    iter_hours,
)


class TestEnsureUtc:
    """Test ensure_utc function"""

    def test_none_returns_none(self):
        assert ensure_utc(None) is None

    def test_naive_datetime_gets_utc(self):
        naive = datetime(2024, 1, 1, 10, 30, 0)
        result = ensure_utc(naive)
        
        assert result.tzinfo == timezone.utc
        assert result.hour == 10

    def test_utc_datetime_unchanged(self):
        utc_dt = datetime(2024, 1, 1, 10, 30, 0, tzinfo=timezone.utc)
        result = ensure_utc(utc_dt)
        
        assert result == utc_dt

    def test_other_timezone_converted_to_utc(self):
        # Create datetime in UTC+5
        tz_plus5 = timezone(timedelta(hours=5))
        dt = datetime(2024, 1, 1, 15, 0, 0, tzinfo=tz_plus5)
        
        result = ensure_utc(dt)
        
        assert result.tzinfo == timezone.utc
        assert result.hour == 10  # 15:00 UTC+5 = 10:00 UTC


class TestAddHours:
    """Test add_hours function"""

    def test_add_positive_hours(self):
        dt = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        result = add_hours(dt, 3)
        
        assert result.hour == 13

    def test_add_negative_hours(self):
        dt = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        result = add_hours(dt, -3)
        
        assert result.hour == 7

    def test_add_hours_crosses_midnight(self):
        dt = datetime(2024, 1, 1, 23, 0, 0, tzinfo=timezone.utc)
        result = add_hours(dt, 2)
        
        assert result.day == 2
        assert result.hour == 1

    def test_subtract_hours_crosses_midnight(self):
        dt = datetime(2024, 1, 2, 1, 0, 0, tzinfo=timezone.utc)
        result = add_hours(dt, -3)
        
        assert result.day == 1
        assert result.hour == 22

    def test_handles_naive_datetime(self):
        naive = datetime(2024, 1, 1, 10, 0, 0)
        result = add_hours(naive, 1)
        
        assert result.tzinfo == timezone.utc
        assert result.hour == 11


class TestTruncateToHour:
    """Test truncate_to_hour function"""

    def test_truncates_minutes_seconds(self):
        dt = datetime(2024, 1, 1, 10, 45, 30, 123456, tzinfo=timezone.utc)
        result = truncate_to_hour(dt)
        
        assert result.minute == 0
        assert result.second == 0
        assert result.microsecond == 0
        assert result.hour == 10

    def test_already_truncated_unchanged(self):
        dt = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        result = truncate_to_hour(dt)
        
        assert result == dt


class TestIterHours:
    """Test iter_hours function"""

    def test_iterates_hours(self):
        start = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        end = datetime(2024, 1, 1, 13, 0, 0, tzinfo=timezone.utc)
        
        hours = list(iter_hours(start, end))
        
        assert len(hours) == 3
        assert hours[0].hour == 10
        assert hours[1].hour == 11
        assert hours[2].hour == 12

    def test_empty_when_start_equals_end(self):
        dt = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        
        hours = list(iter_hours(dt, dt))
        
        assert len(hours) == 0

    def test_empty_when_start_after_end(self):
        start = datetime(2024, 1, 1, 15, 0, 0, tzinfo=timezone.utc)
        end = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        
        hours = list(iter_hours(start, end))
        
        assert len(hours) == 0

    def test_crosses_midnight(self):
        start = datetime(2024, 1, 1, 22, 0, 0, tzinfo=timezone.utc)
        end = datetime(2024, 1, 2, 2, 0, 0, tzinfo=timezone.utc)
        
        hours = list(iter_hours(start, end))
        
        assert len(hours) == 4
        assert hours[0].hour == 22
        assert hours[3].hour == 1
        assert hours[3].day == 2
