"""
event_handle.py

Parse GCN notices from Einstein Probe (JSON), Fermi (VOEvent XML),
and SVOM (VOEvent XML) into a GCNEvent dataclass.
"""

import json
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import Optional
from datetime import datetime

# XML namespace used in VOEvent files
NS = {"voe": "http://www.ivoa.net/xml/VOEvent/v2.0"}


@dataclass
class GCNEvent:
    source: str
    topic: str
    received_at: datetime = field(default_factory=datetime.utcnow)
    ra: Optional[float] = None
    dec: Optional[float] = None
    ra_dec_error: Optional[float] = None
    trigger_time: Optional[str] = None
    snr: Optional[float] = None
    importance: Optional[float] = None
    event_id: Optional[str] = None
    instrument: Optional[str] = None

    def has_position(self) -> bool:
        return self.ra is not None and self.dec is not None

    def __str__(self):
        return (
            f"[{self.source.upper()}] {self.instrument or '?'} | "
            f"trigger={self.trigger_time} | RA={self.ra} Dec={self.dec} "
            f"±{self.ra_dec_error}° | SNR={self.snr} | ID={self.event_id}"
        )


# Helpers

def _parse_xml(xml_str: str) -> ET.Element:
    ET.register_namespace("voe", NS["voe"])
    return ET.fromstring(xml_str)


def _param(root: ET.Element, name: str) -> Optional[str]:
    """Find a <Param name="..."> anywhere in the tree and return its value."""
    el = root.find(f".//*[@name='{name}']")
    return el.get("value") if el is not None else None


def _group(root: ET.Element, group_name: str) -> dict:
    """Return all <Param> children of a named <Group> as {name: value}."""
    group = root.find(f".//*[@name='{group_name}']")
    if group is None:
        return {}
    return {p.get("name"): p.get("value") for p in group}


def _position(root: ET.Element) -> tuple:
    """Extract (RA, Dec, error_radius) from the VOEvent WhereWhen block."""
    ra  = root.find(".//C1")
    dec = root.find(".//C2")
    err = root.find(".//Error2Radius")
    return (
        float(ra.text)  if ra  is not None else None,
        float(dec.text) if dec is not None else None,
        float(err.text) if err is not None else None,
    )


def _iso_time(root: ET.Element) -> Optional[str]:
    el = root.find(".//ISOTime")
    return el.text if el is not None else None



# Parsers — Einstein Probe (JSON), Fermi & SVOM (VOEvent XML)

def parse_einstein_probe(topic: str, value_str: str) -> "GCNEvent":
    """Parse an Einstein Probe WXT JSON alert."""
    d = json.loads(value_str)
    return GCNEvent(
        source="einstein_probe",
        topic=topic,
        instrument=d.get("instrument"),
        trigger_time=d.get("trigger_time"),
        ra=d.get("ra"),
        dec=d.get("dec"),
        ra_dec_error=d.get("ra_dec_error"),
        snr=d.get("image_snr"),
        event_id=(d.get("id") or [None])[0],
    )


def parse_fermi(topic: str, value_str: str) -> "GCNEvent":
    """Parse a Fermi GBM/LAT VOEvent XML alert."""
    root = _parse_xml(value_str)

    # GRB probability lives on <Why> or its <Inference> child
    why = root.find(".//Why")
    importance = float(why.get("importance", 0.0)) if why is not None else 0.0
    inference = root.find(".//Inference")
    if inference is not None:
        importance = float(inference.get("probability", importance))

    ra, dec, err = _position(root)

    # Try several possible SNR param names in priority order
    snr_raw = (
        _param(root, "Trig_Signif")
        or _param(root, "Data_Signif")
        or _param(root, "Burst_Signif")
    )

    return GCNEvent(
        source="fermi",
        topic=topic,
        instrument="LAT" if "LAT" in topic else "GBM",
        trigger_time=_iso_time(root),
        event_id=_param(root, "TrigID"),
        ra=ra, dec=dec, ra_dec_error=err,
        snr=float(snr_raw) if snr_raw else None,
        importance=importance,
    )


def parse_svom(topic: str, value_str: str) -> "GCNEvent":
    """Parse an SVOM ECLAIRs/GRM/MXT VOEvent XML alert."""
    root = _parse_xml(value_str)
    det  = _group(root, "Detection_Info")
    ra, dec, err = _position(root)

    return GCNEvent(
        source="svom",
        topic=topic,
        instrument=_param(root, "Instrument") or topic.split(".")[-1].upper(),
        trigger_time=_iso_time(root),
        event_id=_group(root, "Svom_Identifiers").get("Burst_Id"),
        ra=ra, dec=dec, ra_dec_error=err,
        snr=float(det["SNR"]) if "SNR" in det else None,
    )



# Main entry point

def parse_gcn_message(topic: str, value_str: str) -> Optional[GCNEvent]:
    """Route a raw GCN message to the right parser. Returns None on failure."""
    try:
        if "einstein_probe" in topic:
            return parse_einstein_probe(topic, value_str)
        if "fermi" in topic.lower():
            return parse_fermi(topic, value_str)
        if "svom" in topic.lower():
            return parse_svom(topic, value_str)
    except Exception as e:
        print(f"[event_handle] parse error topic={topic}: {e}")
    return None
