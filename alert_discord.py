"""
alert_discord.py

Send GCN alerts to Discord with webhooks.
"""

import json
import requests
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

COLOUR_NEW        = 0x3498DB
COLOUR_OBSERVABLE = 0x2ECC71
COLOUR_NOT_OBS    = 0xE74C3C

FOOTER_SUFFIX = " | Greenhill Observatory"

WEBHOOK_ALL       = os.getenv("WEBHOOK_ALL")
WEBHOOK_FILTERED  = os.getenv("WEBHOOK_FILTERED")
WEBHOOK_HEARTBEAT = os.getenv("WEBHOOK_HEARTBEAT")


def _coord(v, prefix=""):
    return f"{prefix}{v}°" if v is not None else "N/A"


def field(name, value, inline=True):
    return {"name": name, "value": value, "inline": inline}


def main_fields(event):
    return [
        field("Source",     event.source.upper()),
        field("Instrument", event.instrument or "unknown"),
        field("Event ID",   event.event_id or "N/A"),
        field("Trigger",    event.trigger_time or "N/A"),
        field("RA",         _coord(event.ra)),
        field("Dec",        _coord(event.dec)),
        field("Error",      _coord(event.ra_dec_error, "±")),
        field("SNR",        str(event.snr) if event.snr is not None else "N/A"),
    ]


def make_embed(title, color, fields, topic=None, description=None, footer_suffix=""):
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    return {
        "title": title,
        "description": description if description is not None else f"Topic: `{topic}`",
        "color": color,
        "fields": fields,
        "footer": {"text": f"Received {timestamp}{footer_suffix}"},
    }


def post(webhook_url, payload, files=None):
    if not webhook_url:
        return
    if files:
        resp = requests.post(webhook_url, data={"payload_json": json.dumps(payload)}, files=files, timeout=15)
    else:
        resp = requests.post(webhook_url, json=payload, timeout=15)
    if not resp.ok:
        print(f"[discord] webhook error {resp.status_code}: {resp.text[:200]}")


def send_heartbeat_alert():
    if not WEBHOOK_HEARTBEAT:
        raise RuntimeError("WEBHOOK_HEARTBEAT is not set")
    embed = make_embed(title="Heartbeat", description="Still active", color=COLOUR_NEW, fields=[], footer_suffix=FOOTER_SUFFIX)
    post(WEBHOOK_HEARTBEAT, {"embeds": [embed]})


def send_daily_summary(summary: dict):
    if not WEBHOOK_HEARTBEAT:
        raise RuntimeError("WEBHOOK_HEARTBEAT is not set")
    top = summary.get("top_event")
    top_str = (
        f"{top['event_id']}  RA={top['ra_deg']:.3f}°  Dec={top['dec_deg']:.3f}°  airmass={top['best_airmass']}"
        if top else "none"
    )
    by_source_str = ", ".join(f"{k}: {v}" for k, v in summary.get("by_source", {}).items())
    fields = [
        field("Total events",      str(summary["total"])),
        field("By source",         by_source_str or "—"),
        field("Observable",        str(summary["observable"])),
        field("Retractions",       str(summary["retractions"])),
        field("Circulars matched", str(summary["circulars_matched"])),
        field("Top event",         top_str, inline=False),
    ]
    embed = make_embed(
        title=f"Daily Summary — {summary['date']}",
        description="Events recorded in the last 24 h",
        color=COLOUR_NEW,
        fields=fields,
        footer_suffix=FOOTER_SUFFIX,
    )
    post(WEBHOOK_HEARTBEAT, {"embeds": [embed]})


def send_counterpart_alert(info: dict):
    matched = info.get("matched_notice")
    fields  = [
        field("Keywords", ", ".join(info["keywords"])),
        field("Circular", f"#{info['circular_number']}"),
    ]
    if matched:
        fields += [
            field("Source",       (matched.get("source") or "?").upper()),
            field("Instrument",   matched.get("instrument") or "?"),
            field("Event ID",     matched.get("event_id") or "N/A"),
            field("Trigger time", matched.get("trigger_time") or "N/A"),
            field("RA",           _coord(matched.get("ra"))),
            field("Dec",          _coord(matched.get("dec"))),
            field("Error",        _coord(matched.get("ra_dec_error"), "±")),
        ]
        description = "Matched to a previous notice."
    else:
        description = "No matching notice."

    embed = make_embed(
        title=f"Counterpart Report — {(info['subject'] or '')[:80]}",
        description=description,
        color=COLOUR_NEW,
        fields=fields,
        footer_suffix=FOOTER_SUFFIX,
    )
    post(WEBHOOK_ALL, {"embeds": [embed]})


def send_all_alert(event):
    embed = make_embed(
        title  = f"New Alert — {event.source.upper()} / {event.instrument or '?'}",
        topic  = event.topic,
        color  = COLOUR_NEW,
        fields = main_fields(event),
    )
    post(WEBHOOK_ALL, {"embeds": [embed]})


def send_filtered_alert(event, vis, plot_files=None):
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
        footer_suffix = FOOTER_SUFFIX,
    )
    payload = {"embeds": [embed]}

    if not plot_files:
        post(WEBHOOK_FILTERED, payload)
        return

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
