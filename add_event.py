"""
add_event.py

Manually add a known GRB/transient event to the database and send a Discord alert.

Usage:
    # Provide final event name and coordinates manually
    python add_event.py --event GRB250501A --ra 120.5 --dec -15.3

    # Let Astro-COLIBRI resolve coords from an official event name
    python add_event.py --event AT2026lru

    # Let Astro-COLIBRI resolve official name + coords from a trigger/name
    python add_event.py --trigger 754610296
"""

import argparse
import re
import sys
from datetime import datetime, timezone

import db
import colibri
from event_handle import GCNEvent
from vis_check import check_visibility, plot_visibility


def _normalize_name(name: str) -> str:
    """Normalise GRB names to 'GRB YYMMDDX' format; leave all other names unchanged."""
    stripped = re.sub(r'\s+', '', name).upper()
    if stripped.startswith("GRB"):
        return "GRB " + stripped[3:]
    return name.strip()


def resolve(args):
    """Resolve event name, ra, dec from CLI args. Returns (event_name, ra, dec, magnitude)."""
    event_name = _normalize_name(args.event) if args.event else None
    ra         = args.ra
    dec        = args.dec
    magnitude = None

    if event_name and ra is not None and dec is not None:
        return event_name, ra, dec, magnitude

    if event_name:
        print(f"[add_event] Querying Astro-COLIBRI for event {event_name}...")
        details = colibri.lookup_event_details(event_name, query_param="source_name")
        ra, dec = details['ra'], details['dec']
        magnitude = details.get('magnitude')

        if details['name']:
            event_name = _normalize_name(details['name'])
            print(f"[add_event] Astro-COLIBRI returned name {event_name}")

        if ra is not None and dec is not None:
            print(f"[add_event] Astro-COLIBRI returned RA={ra} Dec={dec}")
        else:
            print("[add_event] Astro-COLIBRI did not return coordinates for this event")

    if args.trigger:
        print(f"[add_event] Querying Astro-COLIBRI for trigger {args.trigger}...")
        details = colibri.lookup_event_details(args.trigger)
        event_name = _normalize_name(details['name']) if details['name'] else None
        ra, dec = details['ra'], details['dec']
        magnitude = details.get('magnitude')

        if event_name:
            print(f"[add_event] Astro-COLIBRI returned name {event_name}")
        else:
            print("[add_event] Astro-COLIBRI did not return an official event name")

        if ra is not None and dec is not None:
            print(f"[add_event] Astro-COLIBRI returned RA={ra} Dec={dec}")
        else:
            print("[add_event] Astro-COLIBRI did not return coordinates for this trigger")

    if event_name is None or ra is None or dec is None:
        print(
            "[add_event] Error: could not resolve an official event name and coordinates.\n"
            "Use --trigger for Astro-COLIBRI lookup, or --event with --ra and --dec for a manual override."
        )
        sys.exit(1)

    return event_name, ra, dec, magnitude


def main():
    parser = argparse.ArgumentParser(description="Manually add a GRB/transient event to the database and send a Discord alert")
    parser.add_argument('--trigger', help='Astro-COLIBRI trigger or unresolved event name to resolve')
    parser.add_argument('--event',   help='Official event name to resolve, or final event name for manual coordinate override')
    parser.add_argument('--ra',      type=float, help='Right ascension in degrees for manual override')
    parser.add_argument('--dec',     type=float, help='Declination in degrees for manual override')
    parser.add_argument('--error',   type=float, default=0.0, help='Position error radius in degrees (default: 0.0)')
    args = parser.parse_args()

    has_manual_coords = args.ra is not None or args.dec is not None
    if args.trigger and (args.event or has_manual_coords):
        parser.error("--trigger cannot be combined with --event, --ra, or --dec")
    if has_manual_coords and not args.event:
        parser.error("--ra and --dec require --event")
    if args.event and ((args.ra is None) != (args.dec is None)):
        parser.error("Manual override requires both --ra and --dec")
    if not args.trigger and not args.event:
        parser.error("Provide --trigger, or --event with --ra and --dec")

    conn = db.init_db()
    event_name, ra, dec, magnitude = resolve(args)

    event = GCNEvent(
        source      = "Manually Added",
        topic       = "manual",
        grb_name    = event_name,
        event_id    = event_name,
        ra          = ra,
        dec         = dec,
        ra_dec_error= args.error,
        received_at = datetime.now(timezone.utc),
    )

    print(f"\n--- {event_name} ---")
    print(f"  RA={ra}°  Dec={dec}°  error=±{args.error}°")

    print("[add_event] Checking visibility...")
    vis = check_visibility(event)

    print(f"  Observable:    {vis['is_observable']}")
    print(f"  Best airmass:  {vis.get('best_airmass', 'N/A')}")
    print(f"  Obs window:    {vis.get('observable_hours', 0.0)} h")
    print(f"  Night start:   {vis.get('night_start', 'N/A')}")
    print(f"  Night end:     {vis.get('night_end', 'N/A')}")

    db.promote_to_grb_event(conn, event, vis, magnitude)
    print(f"[add_event] Stored {event_name} in grb_events")

    print("[add_event] Generating visibility plots...")
    plot_files = plot_visibility(event, filename_prefix=event_name.replace(' ', '_'))

    print("[add_event] Sending Discord alert...")
    from alert_discord import send_filtered_alert
    send_filtered_alert(event, vis, plot_files)
    print("[add_event] Done.")


if __name__ == "__main__":
    main()
