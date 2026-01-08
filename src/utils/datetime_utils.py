"""
Datetime utilities for consistent timezone handling across the pipeline
"""
from datetime import datetime, timedelta, timezone
from typing import Optional


def ensure_utc(dt: Optional[datetime | str]) -> Optional[datetime]:
    """
    Ensure datetime is timezone-aware with UTC timezone.
    
    Args:
        dt: Datetime to convert (can be None, naive/aware datetime, or ISO string)
    
    Returns:
        UTC-aware datetime or None if input is None
    """
    if dt is None:
        return None
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def add_hours(dt: datetime, hours: int) -> datetime:
    """
    Safely add or subtract hours from a datetime using timedelta.
    
    Args:
        dt: Base datetime
        hours: Number of hours to add (can be negative)
    
    Returns:
        New datetime with hours added
    """
    dt = ensure_utc(dt)
    return dt + timedelta(hours=hours)


def truncate_to_hour(dt: datetime) -> datetime:
    """
    Truncate datetime to the start of the hour.
    
    Args:
        dt: Datetime to truncate
    
    Returns:
        Datetime with minute, second, microsecond set to 0
    """
    dt = ensure_utc(dt)
    return dt.replace(minute=0, second=0, microsecond=0)


def get_current_hour_start() -> datetime:
    """
    Get the start of the current hour in UTC.
    
    Returns:
        Current hour start as UTC datetime
    """
    return truncate_to_hour(datetime.now(timezone.utc))


def get_previous_hour_start() -> datetime:
    """
    Get the start of the previous hour in UTC.
    
    Returns:
        Previous hour start as UTC datetime
    """
    return add_hours(get_current_hour_start(), -1)


def iter_hours(start: datetime, end: datetime):
    """
    Iterate over hours between start and end (exclusive).
    
    Args:
        start: Start datetime (inclusive)
        end: End datetime (exclusive)
    
    Yields:
        Each hour between start and end
    """
    start = ensure_utc(truncate_to_hour(start))
    end = ensure_utc(truncate_to_hour(end))
    
    current = start
    while current < end:
        yield current
        current = add_hours(current, 1)
