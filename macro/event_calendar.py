"""
macro/event_calendar.py
=======================
Fetches the day's economic events and computes Tier-1 blackout windows.

Architecture
------------
Called once at 08:45 AM during pre-market init.
Results are stored in memory and passed to pre_execution_gate.

Tier-1 events (RBI policy, US Fed, Indian CPI/WPI, Repo Rate)
    → 5-minute pre-event blackout
    → 15-minute post-event blackout
    → Telegram INFO alert when blackout activates

Tier-2 events (all others)
    → Logged only, no trading impact

API source
----------
Primary: Configured via MACRO_CALENDAR_URL in .env
         Expected to return JSON in the format defined in _parse_response().
Fallback: MACRO_EVENTS_JSON env var — JSON array of manually specified events
          for days when the external API is unavailable.

If both fail: system continues with zero events (no blackouts) — logged.

Manual event format (MACRO_EVENTS_JSON env var):
[
  {"time": "10:00", "description": "RBI Monetary Policy Decision"},
  {"time": "18:30", "description": "US Federal Reserve FOMC Statement"}
]
Time is in IST (HH:MM, 24-hour).

Tier-1 keyword detection
-------------------------
Any event description containing these substrings (case-insensitive) is Tier-1:
    rbi, repo rate, monetary policy, federal reserve, fomc,
    fed rate, cpi, wpi, gdp, inflation data, rate decision

Everything else is Tier-2.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import List, Optional, Tuple

import aiohttp
from loguru import logger
from decouple import config

from config.constants import MACRO_BLACKOUT_PRE, MACRO_BLACKOUT_POST
from monitoring.alerting import alert_macro_event_blackout, send_alert


# ---------------------------------------------------------------------------
# Tier-1 classification keywords
# ---------------------------------------------------------------------------

TIER1_KEYWORDS = [
    "rbi",
    "repo rate",
    "monetary policy",
    "federal reserve",
    "fomc",
    "fed rate",
    "fed funds",
    "cpi",
    "wpi",
    "gdp",
    "inflation data",
    "rate decision",
    "rate hike",
    "rate cut",
    "interest rate",
    "policy rate",
]


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class MacroEvent:
    timestamp: datetime  # Full datetime (date + time) in IST
    description: str
    tier: int  # 1 or 2
    pre_blackout_start: datetime = field(init=False)
    post_blackout_end: datetime = field(init=False)

    def __post_init__(self):
        self.pre_blackout_start = self.timestamp - timedelta(minutes=MACRO_BLACKOUT_PRE)
        self.post_blackout_end = self.timestamp + timedelta(minutes=MACRO_BLACKOUT_POST)

    @property
    def blackout_window(self) -> Tuple[datetime, datetime]:
        """Returns the full blackout window: (pre_start, post_end)."""
        return (self.pre_blackout_start, self.post_blackout_end)


# ---------------------------------------------------------------------------
# Calendar engine
# ---------------------------------------------------------------------------


class EventCalendar:
    """
    Fetches and classifies economic events for the current trading session.
    Builds Tier-1 blackout windows and passes them to pre_execution_gate.
    """

    def __init__(self):
        self._events: List[MacroEvent] = []
        self._tier1_events: List[MacroEvent] = []
        self._blackout_windows: List[Tuple[datetime, datetime]] = []
        self._fetch_success: bool = False

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def fetch_and_classify(self, session_date: date) -> bool:
        """
        Master method called at 08:45 AM.
        Fetches events, classifies into Tier-1/2, builds blackout windows.

        Returns True always — calendar failure is non-fatal.
        System continues with zero events (no blackouts) if fetch fails.
        """
        logger.info(
            "[MACRO CALENDAR] Fetching economic calendar for {}...", session_date
        )

        raw_events = await self._fetch_events(session_date)

        if not raw_events:
            logger.warning(
                "[MACRO CALENDAR] No events fetched. "
                "System continues with zero macro blackouts."
            )
            self._fetch_success = False
            return True

        self._classify_events(raw_events, session_date)
        self._build_blackout_windows()
        self._log_summary()
        self._fetch_success = True
        return True

    def load_into_pre_execution_gate(self) -> None:
        """
        Passes computed blackout windows to pre_execution_gate.
        Called after fetch_and_classify() completes.
        """
        from risk.pre_execution import pre_execution_gate

        pre_execution_gate.set_blackout_windows(self._blackout_windows)
        logger.info(
            "[MACRO CALENDAR] {} Tier-1 blackout windows loaded "
            "into pre-execution gate.",
            len(self._blackout_windows),
        )

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------

    async def _fetch_events(self, session_date: date) -> list:
        """
        Attempts to fetch events from the configured API URL.
        Falls back to MACRO_EVENTS_JSON env var if API fails.
        Returns list of raw event dicts: [{"time": "HH:MM", "description": "..."}]
        """
        api_url: str = config("MACRO_CALENDAR_URL", default="")

        if api_url:
            result = await self._fetch_from_api(api_url, session_date)
            if result is not None:
                logger.info(
                    "[MACRO CALENDAR] API fetch successful. {} raw events.", len(result)
                )
                return result
            logger.warning(
                "[MACRO CALENDAR] API fetch failed. Trying MACRO_EVENTS_JSON fallback."
            )

        return self._load_from_env()

    async def _fetch_from_api(self, url: str, session_date: date) -> Optional[list]:
        """
        Fetches from a configured external API endpoint.
        The endpoint must return JSON in one of two shapes:
            Shape A: [{"time": "HH:MM", "description": "..."}]
            Shape B: {"events": [...]}  (unwrapped automatically)

        Query parameter `date=YYYY-MM-DD` is appended automatically.

        Returns parsed list on success, None on any failure.
        """
        try:
            params = {"date": session_date.isoformat()}
            timeout = aiohttp.ClientTimeout(total=15)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url, params=params) as resp:
                    if resp.status != 200:
                        logger.warning(
                            "[MACRO CALENDAR] API returned HTTP {}.", resp.status
                        )
                        return None
                    data = await resp.json(content_type=None)
                    return self._parse_response(data)

        except aiohttp.ClientError as exc:
            logger.warning("[MACRO CALENDAR] API network error: {}", exc)
            return None
        except Exception as exc:  # noqa: BLE001
            logger.warning("[MACRO CALENDAR] API parse error: {}", exc)
            return None

    def _parse_response(self, data) -> Optional[list]:
        """
        Normalises API response to [{"time": "HH:MM", "description": "..."}].
        Handles common response shapes from economic calendar APIs.
        """
        # Shape A: flat list
        if isinstance(data, list):
            return [e for e in data if "time" in e and "description" in e]

        # Shape B: {"events": [...]}
        if isinstance(data, dict) and "events" in data:
            return self._parse_response(data["events"])

        # Shape C: {"data": [...]}
        if isinstance(data, dict) and "data" in data:
            return self._parse_response(data["data"])

        logger.warning("[MACRO CALENDAR] Unrecognised API response shape.")
        return None

    def _load_from_env(self) -> list:
        """
        Loads events from MACRO_EVENTS_JSON env var.
        Expected format:
            [{"time": "HH:MM", "description": "RBI Monetary Policy"}]
        Returns empty list on parse failure.
        """
        raw_json: str = config("MACRO_EVENTS_JSON", default="[]")
        try:
            events = json.loads(raw_json)
            if not isinstance(events, list):
                raise ValueError("MACRO_EVENTS_JSON must be a JSON array.")
            logger.info(
                "[MACRO CALENDAR] Loaded {} events from MACRO_EVENTS_JSON env var.",
                len(events),
            )
            return events
        except (json.JSONDecodeError, ValueError) as exc:
            logger.error(
                "[MACRO CALENDAR] MACRO_EVENTS_JSON parse error: {}. "
                "No events loaded.",
                exc,
            )
            return []

    # ------------------------------------------------------------------
    # Classification
    # ------------------------------------------------------------------

    def _classify_events(self, raw_events: list, session_date: date) -> None:
        """
        Classifies each raw event as Tier-1 or Tier-2.
        Builds MacroEvent objects with full timestamps.
        """
        self._events.clear()
        self._tier1_events.clear()

        for raw in raw_events:
            time_str = str(raw.get("time", "")).strip()
            description = str(raw.get("description", "")).strip()

            if not time_str or not description:
                continue

            # Parse HH:MM time string
            try:
                hour, minute = (int(p) for p in time_str.split(":")[:2])
                ts = datetime(
                    session_date.year,
                    session_date.month,
                    session_date.day,
                    hour,
                    minute,
                )
            except (ValueError, AttributeError):
                logger.debug(
                    "[MACRO CALENDAR] Could not parse time '{}' for event '{}'. Skipped.",
                    time_str,
                    description,
                )
                continue

            tier = self._classify_tier(description)
            event = MacroEvent(timestamp=ts, description=description, tier=tier)
            self._events.append(event)

            if tier == 1:
                self._tier1_events.append(event)
                logger.info(
                    "[MACRO CALENDAR] Tier-1 event detected: '{}' at {}",
                    description,
                    ts.strftime("%H:%M"),
                )
            else:
                logger.debug(
                    "[MACRO CALENDAR] Tier-2 event: '{}' at {}",
                    description,
                    ts.strftime("%H:%M"),
                )

    def _classify_tier(self, description: str) -> int:
        """Returns 1 if description matches any Tier-1 keyword, else 2."""
        lower = description.lower()
        for keyword in TIER1_KEYWORDS:
            if keyword in lower:
                return 1
        return 2

    # ------------------------------------------------------------------
    # Blackout window construction
    # ------------------------------------------------------------------

    def _build_blackout_windows(self) -> None:
        """
        Builds Tier-1 blackout windows from classified events.
        Each window covers [event_time - PRE_MINUTES, event_time + POST_MINUTES].
        Overlapping windows are merged.
        """
        raw_windows = [event.blackout_window for event in self._tier1_events]

        # Sort by start time, then merge overlapping intervals
        raw_windows.sort(key=lambda w: w[0])
        merged: List[Tuple[datetime, datetime]] = []

        for start, end in raw_windows:
            if merged and start <= merged[-1][1]:
                # Overlapping — extend the current window
                merged[-1] = (merged[-1][0], max(merged[-1][1], end))
            else:
                merged.append((start, end))

        self._blackout_windows = merged

        for start, end in self._blackout_windows:
            logger.info(
                "[MACRO CALENDAR] Blackout window: {} — {}",
                start.strftime("%H:%M"),
                end.strftime("%H:%M"),
            )

    # ------------------------------------------------------------------
    # Logging helpers
    # ------------------------------------------------------------------

    def _log_summary(self) -> None:
        tier1_count = len(self._tier1_events)
        tier2_count = len(self._events) - tier1_count

        logger.info(
            "[MACRO CALENDAR] Classification complete. "
            "Total: {} | Tier-1: {} | Tier-2: {} | Blackout windows: {}",
            len(self._events),
            tier1_count,
            tier2_count,
            len(self._blackout_windows),
        )

        if tier1_count == 0:
            logger.info(
                "[MACRO CALENDAR] No Tier-1 events today. No blackouts scheduled."
            )

    # ------------------------------------------------------------------
    # Properties — read by scheduler for alerting
    # ------------------------------------------------------------------

    @property
    def tier1_events(self) -> List[MacroEvent]:
        return list(self._tier1_events)

    @property
    def blackout_windows(self) -> List[Tuple[datetime, datetime]]:
        return list(self._blackout_windows)

    @property
    def tier1_count(self) -> int:
        return len(self._tier1_events)

    @property
    def total_count(self) -> int:
        return len(self._events)

    @property
    def fetch_succeeded(self) -> bool:
        return self._fetch_success


# Single instance
event_calendar = EventCalendar()
