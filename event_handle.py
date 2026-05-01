"""
event_handle.py

Parse GCN notices from Einstein Probe (JSON), Fermi (VOEvent XML),
and SVOM (VOEvent XML) into a GCNEvent dataclass.
"""

import json
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import Optional
from datetime import datetime, timezone

NS = {"voe": "http://www.ivoa.net/xml/VOEvent/v2.0"}


@dataclass
class GCNEvent:
    source: str
    topic: str
    received_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    role: str = "observation"
    ra: Optional[float] = None
    dec: Optional[float] = None
    ra_dec_error: Optional[float] = None
    trigger_time: Optional[str] = None
    snr: Optional[float] = None
    importance: Optional[float] = None
    event_id: Optional[str] = None
    instrument: Optional[str] = None
    ivorn: Optional[str] = None           # this event's unique identifier
    retraction_of: Optional[str] = None  # IVORN cited with cite="retraction"
    grb_name: Optional[str] = None

    def has_position(self) -> bool:
        return self.ra is not None and self.dec is not None

    def __str__(self):
        return (
            f"[{self.source.upper()}] {self.instrument or '?'} | "
            f"{self.grb_name + ' | ' if self.grb_name else ''}"
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


def _local_tag(tag: str) -> str:
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def _group(root: ET.Element, group_name: str) -> dict:
    """Return all <Param> children of a named <What/Group> as {name: value} (VOEvent v2.0 What/Group)."""
    for el in root.iter():
        if _local_tag(el.tag) != "Group" or el.get("name") != group_name:
            continue
        out = {}
        for p in el:
            if _local_tag(p.tag) == "Param" and p.get("name") is not None:
                out[p.get("name")] = p.get("value")
        return out
    return {}


def _position(root: ET.Element) -> tuple:
    """Extract (RA, Dec, error_radius) from WhereWhen (VOEvent v2.0; works with default XML namespace)."""
    ra = dec = err = None
    for el in root.iter():
        t = _local_tag(el.tag)
        txt = (el.text or "").strip()
        if not txt:
            continue
        if t == "C1" and ra is None:
            ra = float(txt)
        elif t == "C2" and dec is None:
            dec = float(txt)
        elif t == "Error2Radius" and err is None:
            err = float(txt)
    return ra, dec, err


def _iso_time(root: ET.Element) -> Optional[str]:
    for el in root.iter():
        if _local_tag(el.tag) == "ISOTime" and el.text and el.text.strip():
            return el.text.strip()
    return None


def _retraction_of(root: ET.Element) -> Optional[str]:
    """Return the IVORN cited with cite='retraction', or None."""
    for el in root.iter():
        if _local_tag(el.tag) != "EventIVORN":
            continue
        if el.get("cite") == "retraction" and el.text and el.text.strip():
            return el.text.strip()
    return None


def _fermi_importance(root: ET.Element) -> float:
    """GRB probability from <Why importance> or first <Inference probability> under <Why> (namespaced-safe)."""
    why = None
    for el in root.iter():
        if _local_tag(el.tag) == "Why":
            why = el
            break
    if why is None:
        for el in root.iter():
            if _local_tag(el.tag) != "Inference":
                continue
            prob = el.get("probability")
            if prob is not None:
                return float(prob)
            break
        return 0.0
    imp_attr = why.get("importance")
    importance = float(imp_attr) if imp_attr is not None else 0.0
    for ch in why:
        if _local_tag(ch.tag) != "Inference":
            continue
        prob = ch.get("probability")
        if prob is not None:
            importance = float(prob)
        break
    return importance


def _clean_grb_name(name: Optional[str]) -> Optional[str]:
    if not name:
        return None
    name = name.strip().upper()
    if not name.startswith("GRB"):
        name = f"GRB {name}"
    return " ".join(name.split())


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

    importance = _fermi_importance(root)

    ra, dec, err = _position(root)

    # Try several possible SNR param names in priority order
    snr_raw = (
        _param(root, "Trig_Signif")
        or _param(root, "Data_Signif")
        or _param(root, "Burst_Signif")
    )

    grb_name_raw = _param(root, "GRB_NAME") or _param(root, "Burst_Name")

    return GCNEvent(
        source="fermi",
        topic=topic,
        role=root.get("role", "observation"),
        instrument="LAT" if "LAT" in topic else "GBM",
        trigger_time=_iso_time(root),
        event_id=_param(root, "TrigID"),
        ra=ra, dec=dec, ra_dec_error=err,
        snr=float(snr_raw) if snr_raw else None,
        importance=importance,
        ivorn=root.get("ivorn"),
        retraction_of=_retraction_of(root),
        grb_name=_clean_grb_name(grb_name_raw),
    )


def parse_svom(topic: str, value_str: str) -> "GCNEvent":
    """Parse an SVOM ECLAIRs/GRM/MXT VOEvent XML alert."""
    root = _parse_xml(value_str)
    det  = _group(root, "Detection_Info")
    ids  = _group(root, "Svom_Identifiers")
    ra, dec, err = _position(root)

    return GCNEvent(
        source="svom",
        topic=topic,
        role=root.get("role", "observation"),
        instrument=_param(root, "Instrument") or topic.split(".")[-1].upper(),
        trigger_time=_iso_time(root),
        event_id=ids.get("Burst_Id"),
        ra=ra, dec=dec, ra_dec_error=err,
        snr=float(det["SNR"]) if "SNR" in det else None,
        ivorn=root.get("ivorn"),
        retraction_of=_retraction_of(root),
        grb_name=_clean_grb_name(ids.get("Burst_Name")),
    )



def parse_circular(value_str: str) -> dict:
    """Parse a GCN Circular JSON message."""
    d = json.loads(value_str)
    return {
        "circular_number": d.get("circularId"),
        "subject":         d.get("subject"),
        "body":            d.get("body"),
        "received_at":     datetime.now(timezone.utc).isoformat(),
        "created_on":      d.get("createdOn"),
        "event_id":        d.get("eventId"),
    }


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
