"""
alert_discord.py

Send GCN alerts to Discord with webhooks.
"""

import json
import requests
from datetime import datetime
import os
from dotenv import load_dotenv, dotenv_values 

load_dotenv() 
 
# colours 
COLOUR_NEW        = 0x3498DB
COLOUR_OBSERVABLE = 0x2ECC71
COLOUR_NOT_OBS    = 0xE74C3C

# Webhook URLs
WEBHOOK_ALL      = os.getenv("WEBHOOK_ALL")
WEBHOOK_FILTERED = os.getenv("WEBHOOK_FILTERED")



def field(name, value, inline=True):
    """Create a Discord embed field."""
    return {"name": name, "value": value, "inline": inline}


def main_fields(event):
    """Event fields used in every embed"""
    return [
        field("Source",     event.source.upper()),
        field("Instrument", event.instrument or "unknown"),
        field("Event ID",   event.event_id or "N/A"),
        field("Trigger",    event.trigger_time or "N/A"),
        field("RA",         f"{event.ra}°" if event.ra is not None else "N/A"),
        field("Dec",        f"{event.dec}°" if event.dec is not None else "N/A"),
        field("Error",      f"±{event.ra_dec_error}°" if event.ra_dec_error is not None else "N/A"),
        field("SNR",        str(event.snr) if event.snr is not None else "N/A"),
    ]


def make_embed(title, color, fields, topic=None, description=None, footer_suffix=""):
    """Build a Discord embed dictionary object"""
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    return {
        "title": title,
        "description": description if description is not None else f"Topic: `{topic}`",
        "color": color,
        "fields": fields,
        "footer": {"text": f"Received {timestamp}{footer_suffix}"},
    }


def post(webhook_url, payload, files=None):
    """POST to a Discord webhook."""
    if files:
        resp = requests.post(webhook_url, data={"payload_json": json.dumps(payload)}, files=files, timeout=15)
    else:
        resp = requests.post(webhook_url, json=payload, timeout=15)

    if not resp.ok:
        print(f"[discord] webhook error {resp.status_code}: {resp.text[:200]}")


def send_heartbeat_alert():
    """Send a daily heartbeat to the filtered channel."""
    embed = make_embed(
        title       = "Heartbeat",
        description = "Still active",
        color       = COLOUR_NEW,
        fields      = [],
        footer_suffix = " | Greenhill Observatory",
    )
    post(WEBHOOK_FILTERED, {"embeds": [embed]})


def send_all_alert(event):
    """Send a basic alert to the all-alerts channel."""
    embed = make_embed(
        title  = f"New Alert — {event.source.upper()} / {event.instrument or '?'}",
        topic  = event.topic,
        color  = COLOUR_NEW,
        fields = main_fields(event),
    )
    post(WEBHOOK_ALL, {"embeds": [embed]})


def send_filtered_alert(event, vis, plot_files=None):
    """Send a detailed alert with visibility info to the filtered channel."""
    observable = vis.get("is_observable", False)

    vis_fields = [
        field("Observable",   str(observable)),
        field("Best airmass", str(vis.get("best_airmass", "N/A"))),
        field("Obs window",   f"{vis.get('observable_hours', 0.0)} h"),
        field("Night start",  vis.get("night_start") or "N/A"),
        field("Night end",    vis.get("night_end") or "N/A"),
    ]

    embed = make_embed(
        title         = f"Filtered Alert — {event.source.upper()} / {event.instrument or '?'}",
        topic         = event.topic,
        color         = COLOUR_OBSERVABLE if observable else COLOUR_NOT_OBS,
        fields        = main_fields(event) + [field("\u200b", "**Visibility**", inline=False)] + vis_fields,
        footer_suffix = " | Greenhill Observatory",
    )
    payload = {"embeds": [embed]}

    if not plot_files:
        post(WEBHOOK_FILTERED, payload)
        return

    # Attach plot images
    files = []
    for i, path in enumerate(plot_files[:10]):
        try:
            files.append((f"file{i}", (path, open(path, "rb"), "image/jpeg")))
        except OSError as exc:
            print(f"[discord] could not open plot {path}: {exc}")

    try:
        post(WEBHOOK_FILTERED, payload, files=files or None)
    finally:
        for _, (_, fh, _) in files:
            fh.close()
