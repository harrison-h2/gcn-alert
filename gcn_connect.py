"""
Connect to GCN via Kafka, parse incoming alerts,
filter and check visibility with vis_check

This is the script you need to run:
    python gcn_connect.py

"""

from gcn_kafka import Consumer
from event_handle import parse_gcn_message
from vis_check import passes_filters, is_ever_visible, check_visibility, plot_visibility, TIMEZONE
from alert_discord import send_all_alert, send_filtered_alert, send_retraction_alert, send_heartbeat_alert
import warnings
import pytz
from datetime import datetime, date
from astropy.time.core import TimeDeltaMissingUnitWarning
from astropy.coordinates.baseframe import NonRotationTransformationWarning
import os
from dotenv import load_dotenv, dotenv_values 

load_dotenv() 
warnings.filterwarnings("ignore", category=TimeDeltaMissingUnitWarning)
warnings.filterwarnings("ignore", category=NonRotationTransformationWarning)
warnings.filterwarnings("ignore", message="no explicit representation of timezones")


#  Config
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
CLIENT_ID = os.getenv("CLIENT_ID")

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
]



def create_consumer():
    consumer = Consumer(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        # change auto.offset.reset to "earliest" to start from the beginning for testing
        config={"broker.address.family": "v4", "auto.offset.reset": "earliest"},
    )
    consumer.subscribe(TOPICS)
    return consumer



HOBART_TZ = pytz.timezone(TIMEZONE)
HEARTBEAT_HOUR = 17  # 5 pm Hobart time


def main():
    consumer = create_consumer()
    print(f"Listening on {len(TOPICS)} topics ...")

    last_heartbeat_date: date | None = None

    try:
        send_heartbeat_alert()
        last_heartbeat_date = datetime.now(HOBART_TZ).date()
        print("[HEARTBEAT] startup heartbeat sent")
    except Exception as exc:
        print(f"[discord/heartbeat] {exc}")

    while True:
        now_hobart = datetime.now(HOBART_TZ)
        if now_hobart.hour >= HEARTBEAT_HOUR and now_hobart.date() != last_heartbeat_date:
            try:
                send_heartbeat_alert()
                last_heartbeat_date = now_hobart.date()
                print(f"[HEARTBEAT] sent at {now_hobart.strftime('%Y-%m-%d %H:%M %Z')}")
            except Exception as exc:
                print(f"[discord/heartbeat] {exc}")

        for msg in consumer.consume(timeout=1):
            topic = msg.topic()
            event = parse_gcn_message(topic, msg.value().decode("utf-8"))
            if event is None:
                continue

            # Retractions, notify both channels immediately, skip  processing
            if event.role == "retraction":
                try:
                    send_retraction_alert(event)
                    print(f"[RETRACTION] {event.source.upper()} {event.event_id}")
                except Exception as exc:
                    print(f"[discord/retraction] {exc}")
                continue

            # All-alerts webhook real observations currently above the horizon
            # (no SNR / error / moon constraints as long as it is visible in the sky
            if event.is_real_observation() and is_ever_visible(event):
                try:
                    send_all_alert(event)
                    print(f"[ALL] {event.source.upper()} {event.instrument} visible above horizon")
                except Exception as exc:
                    print(f"[discord/all] {exc}")

            # Filtered webhook filters and nighttime observability
            if not passes_filters(event):
                continue

            vis = check_visibility(event)
            if not vis["is_observable"]:
                continue

            print(
                f"[FILTERED] {event.source.upper()} {event.instrument} "
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
