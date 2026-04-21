"""
Connect to GCN via Kafka, parse incoming alerts,
filter and check visibility with vis_check

This is the script you need to run:
    python gcn_connect.py

"""

import json
import warnings
import pytz
from datetime import datetime, date, timedelta
from gcn_kafka import Consumer
from event_handle import parse_gcn_message, parse_circular, GCNEvent
from colibri import lookup_grb_name
from vis_check import passes_filters, is_ever_visible, check_visibility, plot_visibility, TIMEZONE
from alert_discord import (send_all_alert, send_filtered_alert, send_heartbeat_alert,
                           send_daily_summary, send_counterpart_alert)
from db import (init_db, insert_notice, insert_circular, daily_summary, cleanup_old_records,
                promote_to_grb_event, insert_pending, get_due_pending, bump_pending_retry,
                remove_pending, MAX_PENDING_ATTEMPTS, check_counterpart_circular)
from astropy.time.core import TimeDeltaMissingUnitWarning
from astropy.coordinates.baseframe import NonRotationTransformationWarning
import os
from dotenv import load_dotenv

load_dotenv()
warnings.filterwarnings("ignore", category=TimeDeltaMissingUnitWarning)
warnings.filterwarnings("ignore", category=NonRotationTransformationWarning)
warnings.filterwarnings("ignore", message="no explicit representation of timezones")

CLIENT_SECRET = os.getenv("CLIENT_SECRET")
CLIENT_ID     = os.getenv("CLIENT_ID")

TOPICS = [
    "gcn.classic.voevent.FERMI_GBM_ALERT",
    "gcn.classic.voevent.FERMI_GBM_FIN_POS",
    "gcn.classic.voevent.FERMI_GBM_FLT_POS",
    "gcn.classic.voevent.FERMI_GBM_GND_POS",
    "gcn.classic.voevent.FERMI_LAT_OFFLINE",
    "gcn.classic.voevent.FERMI_LAT_POS_UPD",
    "gcn.notices.svom.voevent.grm",
    "gcn.notices.svom.voevent.eclairs",
    #"gcn.notices.svom.voevent.mxt",
    "gcn.notices.einstein_probe.wxt.alert",
    "gcn.circulars",
]


def create_consumer():
    consumer = Consumer(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        # change auto.offset.reset to "earliest" to start from the beginning for testing
        config={"broker.address.family": "v4", "auto.offset.reset": "latest"},
    )
    consumer.subscribe(TOPICS)
    return consumer


HOBART_TZ      = pytz.timezone(TIMEZONE)
HEARTBEAT_HOUR = 17
PENDING_CHECK_INTERVAL = 60


_PENDING_RETRY_SCHEDULE = [
    timedelta(minutes=40),
    timedelta(hours=1),
    timedelta(hours=2),
    None,
    timedelta(hours=24),
]


def _next_5pm_hobart() -> str:
    from datetime import timezone as _tz
    now = datetime.now(HOBART_TZ)
    target = now.replace(hour=HEARTBEAT_HOUR, minute=0, second=0, microsecond=0)
    if now >= target:
        target += timedelta(days=1)
    return target.astimezone(_tz.utc).isoformat()


def _retry_after_for_attempt(attempt: int) -> str:
    from datetime import timezone as _tz
    delta = _PENDING_RETRY_SCHEDULE[attempt]
    if delta is None:
        return _next_5pm_hobart()
    return (datetime.now(_tz.utc) + delta).isoformat()


def _promote_from_pending(conn, pending: dict, grb_name: str, magnitude: float = None):
    vis   = json.loads(pending["vis_json"])
    event = GCNEvent(
        source=pending["source"], topic="", event_id=pending["event_id"],
        trigger_time=pending["trigger_time"], ra=pending["ra"], dec=pending["dec"],
        instrument=pending["instrument"], grb_name=grb_name,
    )
    try:
        promote_to_grb_event(conn, event, vis, magnitude)
        print(f"[PROMOTION] {grb_name} added to observable events table (from pending)")
    except Exception as exc:
        print(f"[db/promotion] {exc}")

    remove_pending(conn, pending["id"])


def main():
    consumer = create_consumer()
    conn     = init_db()
    print(f"Listening on {len(TOPICS)} topics ...")

    last_heartbeat_date:     date | None = None
    last_daily_summary_date: date | None = None
    last_pending_check:      datetime | None = None

    try:
        send_heartbeat_alert()
    except Exception as exc:
        print(f"[discord/heartbeat] {exc}")
    else:
        last_heartbeat_date = datetime.now(HOBART_TZ).date()
        print("[HEARTBEAT] startup heartbeat sent")

    while True:
        now_hobart = datetime.now(HOBART_TZ)

        if now_hobart.hour >= HEARTBEAT_HOUR:
            today_str = now_hobart.strftime("%Y-%m-%d")
            if last_daily_summary_date != now_hobart.date():
                try:
                    send_daily_summary(daily_summary(conn, today_str))
                    last_daily_summary_date = now_hobart.date()
                except Exception as exc:
                    print(f"[discord/daily_summary] {exc}")
                try:
                    deleted = cleanup_old_records(conn)
                    print(f"[CLEANUP] removed {deleted['notices']} notices, {deleted['circulars']} circulars older than 7 days")
                except Exception as exc:
                    print(f"[db/cleanup] {exc}")
            if last_heartbeat_date != now_hobart.date():
                try:
                    send_heartbeat_alert()
                except Exception as exc:
                    print(f"[discord/heartbeat] {exc}")
                else:
                    last_heartbeat_date = now_hobart.date()
                    print(f"[HEARTBEAT] sent at {now_hobart.strftime('%Y-%m-%d %H:%M %Z')}")

        if last_pending_check is None or (now_hobart - last_pending_check).total_seconds() >= PENDING_CHECK_INTERVAL:
            for pending in get_due_pending(conn):
                col_data = lookup_grb_name(pending["event_id"])
                if col_data["name"]:
                    print(f"[PENDING] resolved {pending['source'].upper()} {pending['event_id']} → {col_data['name']}")
                    _promote_from_pending(conn, pending, col_data["name"], col_data["magnitude"])
                elif pending["attempts"] >= MAX_PENDING_ATTEMPTS:
                    remove_pending(conn, pending["id"])
                    print(f"[PENDING] gave up on {pending['source'].upper()} {pending['event_id']} after all retries")
                else:
                    retry_at = _retry_after_for_attempt(pending["attempts"])
                    bump_pending_retry(conn, pending["id"], retry_at)
                    print(f"[PENDING] no name yet for {pending['source'].upper()} {pending['event_id']} (attempt {pending['attempts'] + 1}/{MAX_PENDING_ATTEMPTS})")
            last_pending_check = now_hobart

        for msg in consumer.consume(timeout=1):
            topic = msg.topic()
            raw   = msg.value().decode("utf-8")

            if topic == "gcn.circulars":
                try:
                    data   = parse_circular(topic, raw)
                    grb_id = insert_circular(conn, data)
                    if grb_id is None:
                        continue  # Not related to any event we track
                    counterpart = check_counterpart_circular(conn, data)
                    print(f"[CIRCULAR] #{data.get('circular_number')} — {(data.get('subject') or '')[:60]}")
                    if counterpart:
                        print(f"[CIRCULAR] counterpart keywords {counterpart['keywords']} — alerting all channel")
                        send_counterpart_alert(counterpart)
                except Exception as exc:
                    print(f"[db/circular] {exc}")
                continue

            event = parse_gcn_message(topic, raw)
            if event is None:
                continue

            if event.role == "test":
                continue

            if event.retraction_of:
                print(f"[RETRACTION] {event.source.upper()} retracts {event.retraction_of}")
                continue

            ever_vis = is_ever_visible(event) if event.has_position() else False
            if event.has_position() and not ever_vis:
                continue

            notice_id = None
            try:
                notice_id = insert_notice(conn, event, raw)
            except Exception as exc:
                print(f"[db/notice] {exc}")

            if ever_vis:
                try:
                    send_all_alert(event)
                    print(f"[ALL] {event.source.upper()} {event.instrument} visible above horizon")
                except Exception as exc:
                    print(f"[discord/all] {exc}")

            if not passes_filters(event):
                continue

            vis = check_visibility(event)
            if not vis["is_observable"]:
                continue

            magnitude = None
            if event.grb_name is None and event.event_id:
                col_data = lookup_grb_name(event.event_id)
                if col_data["name"]:
                    event.grb_name = col_data["name"]
                    magnitude      = col_data["magnitude"]
                    print(f"[COLIBRI] Found name: {event.grb_name} for ID {event.event_id}")

            if event.grb_name:
                try:
                    promote_to_grb_event(conn, event, vis, magnitude)
                    print(f"[PROMOTION] {event.grb_name} added to observable events table")
                except Exception as exc:
                    print(f"[db/promotion] {exc}")
            elif notice_id:
                try:
                    insert_pending(conn, event, vis, notice_id)
                    print(f"[PENDING] {event.source.upper()} {event.event_id} queued for name lookup")
                except Exception as exc:
                    print(f"[db/pending] {exc}")

            print(
                f"[FILTERED] {event.source.upper()} {event.instrument} | {event.grb_name} "
                f"RA={event.ra} Dec={event.dec} "
                f"airmass={vis['best_airmass']} "
                f"window={vis['observable_hours']}h"
            )
            plots = plot_visibility(event)
            try:
                send_filtered_alert(event, vis, plot_files=plots or None)
            except Exception as exc:
                print(f"[discord/filtered] {exc}")


if __name__ == "__main__":
    main()
