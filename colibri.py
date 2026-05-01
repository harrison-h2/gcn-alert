import requests

URL = "https://astro-colibri.science"


def _fetch_event_data(event_id):
    """Fetch and unwrap a Colibri event response. Returns parsed dict or None on any error."""
    if not event_id:
        return None
    try:
        response = requests.get(f"{URL}/event?trigger_id={event_id}", timeout=10)
        if response.status_code != 200:
            return None
        data = response.json()
        if isinstance(data, list):
            if not data or 'error' in data[0]:
                return None
            data = data[0]
        return data
    except Exception as e:
        print(f"[colibri] fetch error for {event_id}: {e}")
    return None


def _extract_magnitude(data):
    magnitude = data.get('magnitude')
    if magnitude is None:
        phot = data.get('photometry', {})
        if isinstance(phot, dict) and phot:
            magnitude = phot.get('mag')
    return magnitude


def lookup_grb_name(event_id):
    """
    Look up an official GRB name and magnitude from the Astro-COLIBRI API.
    Ref: https://astro-colibri.science/apidoc
    """
    data = _fetch_event_data(event_id)
    if data is None:
        return {"name": None, "magnitude": None}

    source_name    = data.get('source_name')
    classification = data.get('classification', '')
    event_type     = data.get('type', '')

    final_name = None
    if source_name and source_name.upper().startswith("GRB"):
        final_name = source_name.strip()
    elif "GRB" in classification.upper() or "GRB" in event_type.upper():
        if source_name and not source_name.startswith("AC "):
            final_name = source_name.strip()

    return {"name": final_name, "magnitude": _extract_magnitude(data)}


def lookup_event_details(event_id):
    """
    Look up name, coordinates, and magnitude for an event from Astro-COLIBRI.
    Returns {"name", "ra", "dec", "magnitude"} — any field may be None.
    """
    data = _fetch_event_data(event_id)
    if data is None:
        return {"name": None, "ra": None, "dec": None, "magnitude": None}

    source_name = data.get('source_name')
    final_name  = source_name.strip() if source_name else None

    ra  = data.get('ra')  or data.get('raj2000')  or data.get('ra_deg')
    dec = data.get('dec') or data.get('decj2000') or data.get('dec_deg')
    try:
        ra  = float(ra)  if ra  is not None else None
        dec = float(dec) if dec is not None else None
    except (TypeError, ValueError):
        ra = dec = None

    return {"name": final_name, "ra": ra, "dec": dec, "magnitude": _extract_magnitude(data)}
