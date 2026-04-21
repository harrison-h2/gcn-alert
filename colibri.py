import requests

URL = "https://astro-colibri.science"

def lookup_grb_name(event_id):
    """
    Look up an official GRB name and magnitude from the Astro-COLIBRI API.
    Ref: https://astro-colibri.science/apidoc
    """
    if not event_id:
        return {"name": None, "magnitude": None}
    
    try:
        response = requests.get(f"{URL}/event?trigger_id={event_id}", timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, list):
                if len(data) > 0 and 'error' in data[0]:
                    return {"name": None, "magnitude": None}
                elif len(data) > 0:
                    data = data[0]
                else:
                    return {"name": None, "magnitude": None}

            source_name = data.get('source_name')
            classification = data.get('classification', '')
            event_type = data.get('type', '')
            
            # Extract magnitude if available
            magnitude = data.get('magnitude')
            if magnitude is None:
                # Try to find it in photometry if it exists
                phot = data.get('photometry', {})
                if isinstance(phot, dict) and phot:
                    # Just an example of how we might find it
                    magnitude = phot.get('mag')
            
            final_name = None
            if source_name and source_name.upper().startswith("GRB"):
                final_name = source_name.strip()
            elif "GRB" in classification.upper() or "GRB" in event_type.upper():
                if source_name and not source_name.startswith("AC "):
                    final_name = source_name.strip()
            
            return {"name": final_name, "magnitude": magnitude}
                
        elif response.status_code == 404:
            pass
            
    except Exception as e:
        print(f"[colibri] lookup error for {event_id}: {e}")
        
    return {"name": None, "magnitude": None}
