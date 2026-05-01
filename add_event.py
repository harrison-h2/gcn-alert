"""
add_event.py

Manually add a known GRB/transient event to the database and send a Discord alert.

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


def _parse_coords_from_text(text: str):
    """Extract decimal RA/Dec from a GCN circular body. Returns (ra, dec) or (None, None)."""
    dec_m = re.search(r'Dec\b[^\n]*\(=?\s*([-+]?[0-9]{1,2}\.[0-9]{1,6})\s*deg', text, re.IGNORECASE)
    if ra_m and dec_m:
        return float(ra_m.group(1)), float(dec_m.group(1))

    if ra_m and dec_m:
        return float(ra_m.group(1)), float(dec_m.group(1))

    return None, None


def _coords_from_circulars(conn, grb_name: str):
    """Search local circulars DB for coordinates matching grb_name."""
    circulars = db.get_circulars_for_grb_name(conn, grb_name)
    for circ in circulars:
        body = circ.get('body') or ''
        ra, dec = _parse_coords_from_text(body)
        if ra is not None and dec is not None:
            print(f"[add_event] Coordinates found in circular #{circ.get('circular_number')}")
            return ra, dec
    return None, None


def _coords_from_simbad(grb_name: str):
    """Query SIMBAD for coordinates by name. Returns (ra, dec) or (None, None)."""
    try:
        from astroquery.simbad import Simbad
        from astropy.coordinates import SkyCoord
        import astropy.units as u
    except ImportError:
        return None, None

    try:
        result = Simbad.query_object(grb_name)
        if result is None or len(result) == 0:
            return None, None
        coord = SkyCoord(ra=result['RA'][0], dec=result['DEC'][0], unit=(u.hourangle, u.deg), frame='icrs')
        return round(float(coord.ra.deg), 5), round(float(coord.dec.deg), 5)
    except Exception as e:
        print(f"[add_event] SIMBAD lookup failed: {e}")
        return None, None


def resolve(args, conn):
    """Resolve grb_name, ra, dec from CLI args. Returns (grb_name, ra, dec, magnitude)."""
    grb_name  = _normalize_name(args.grb) if args.grb else None
    ra        = args.ra
    dec       = args.dec
    magnitude = None

    # Coords provided directly — no network call needed
    if ra is not None and dec is not None:
        if grb_name is None:
            print("[add_event] Error: --grb is required when providing --ra/--dec manually")
            sys.exit(1)
        return grb_name, ra, dec, magnitude

    # Trigger ID → query Colibri for name + coords
    if args.trigger_id:
        print(f"[add_event] Querying AstroColibri for trigger {args.trigger_id}...")
        details = colibri.lookup_event_details(args.trigger_id)
        if grb_name is None:
            grb_name = _normalize_name(details['name']) if details['name'] else _normalize_name(args.trigger_id)
        if details['ra'] is not None and details['dec'] is not None:
            ra, dec = details['ra'], details['dec']
            print(f"[add_event] Colibri returned RA={ra} Dec={dec}")
        else:
            print("[add_event] Colibri did not return coordinates for this trigger ID")
        magnitude = details.get('magnitude')

    if grb_name is None:
        print("[add_event] Error: could not determine GRB name. Provide --grb or a valid --trigger-id")
        sys.exit(1)

    if ra is None or dec is None:
        print(f"[add_event] Searching local circulars for {grb_name}...")
        ra, dec = _coords_from_circulars(conn, grb_name)

    if ra is None or dec is None:
        # Try SIMBAD
        print(f"[add_event] Trying SIMBAD for {grb_name}...")
        ra, dec = _coords_from_simbad(grb_name)
        if ra is not None:
            print(f"[add_event] SIMBAD returned RA={ra} Dec={dec}")

    if ra is None or dec is None:
        print(
            f"[add_event] Error: could not find coordinates for {grb_name}.\n"
            "Provide --ra and --dec, or --trigger-id to fetch from AstroColibri."
        )
        sys.exit(1)

    return grb_name, ra, dec, magnitude


def main():
    parser = argparse.ArgumentParser(description="Manually add a GRB event to the database and send a Discord alert")
    parser.add_argument('--grb',        help='GRB name, e.g. GRB250501A')
    parser.add_argument('--trigger-id', dest='trigger_id', help='AstroColibri trigger ID')
    parser.add_argument('--ra',         type=float, help='Right ascension in degrees')
    parser.add_argument('--dec',        type=float, help='Declination in degrees')
    parser.add_argument('--error',      type=float, default=0.0, help='Position error radius in degrees (default: 0.0)')
    args = parser.parse_args()

    if not args.grb and not args.trigger_id:
        parser.error("Provide at least --grb or --trigger-id")

    conn = db.init_db()
    grb_name, ra, dec, magnitude = resolve(args, conn)

    event = GCNEvent(
        source      = "Manually Added",
        topic       = "manual",
        grb_name    = grb_name,
        event_id    = grb_name,
        ra          = ra,
        dec         = dec,
        ra_dec_error= args.error,
        received_at = datetime.now(timezone.utc),
    )

    print(f"\n--- {grb_name} ---")
    print(f"  RA={ra}°  Dec={dec}°  error=±{args.error}°")

    print("[add_event] Checking visibility...")
    vis = check_visibility(event)

    print(f"  Observable:    {vis['is_observable']}")
    print(f"  Best airmass:  {vis.get('best_airmass', 'N/A')}")
    print(f"  Obs window:    {vis.get('observable_hours', 0.0)} h")
    print(f"  Night start:   {vis.get('night_start', 'N/A')}")
    print(f"  Night end:     {vis.get('night_end', 'N/A')}")

    db.promote_to_grb_event(conn, event, vis, magnitude)
    print(f"[add_event] Stored {grb_name} in grb_events")

    print("[add_event] Generating visibility plots...")
    plot_files = plot_visibility(event, filename_prefix=grb_name.replace(' ', '_'))

    print("[add_event] Sending Discord alert...")
    from alert_discord import send_filtered_alert
    send_filtered_alert(event, vis, plot_files)
    print("[add_event] Done.")


if __name__ == "__main__":
    main()
