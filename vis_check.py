"""
vis_check.py

Filtering and visibility checking for GCN events, observed from
Greenhill Observatory.

"""

import numpy as np
import pytz
import matplotlib

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker
from matplotlib.collections import LineCollection
import astropy.units as u
from astropy.time import Time
from astropy.coordinates import SkyCoord, EarthLocation, get_body
from astroplan import Observer, FixedTarget
from astroplan.constraints import (
    AltitudeConstraint,
    AtNightConstraint,
    MoonSeparationConstraint,
    is_observable,
    observability_table,
)
from astroplan.plots import plot_airmass
matplotlib.use("Agg")

__all__ = [
    "passes_filters", "is_ever_visible", "check_visibility", "plot_visibility",
    "create_target", "mask_airmass", "fmt_local", "night_window"
]


# Observatory

TIMEZONE = "Australia/Hobart"

LOCATION = EarthLocation.from_geodetic(
    lon=147.28772 * u.deg,
    lat=-42.239562347532264 * u.deg,
    height=646 * u.m,
)

OBSERVER = Observer(
    location=LOCATION,
    name="Greenhill Observatory",
    timezone=TIMEZONE,
)
HOBART_TZ = pytz.timezone(TIMEZONE)

# Filter thresholds

MAX_ERROR_DEG = 1.0  # ignore large deg err
MIN_ALTITUDE = 30.0  # minimum observable altitude (degrees)
MIN_MOON_SEP = 30.0  # minimum moon separation (degrees)
# Observability constraints (astroplan handles the maths)
BASIC_CONSTRAINTS = [
    AltitudeConstraint(min=MIN_ALTITUDE * u.deg),
    AtNightConstraint.twilight_astronomical(),
]
CONSTRAINTS = BASIC_CONSTRAINTS + [MoonSeparationConstraint(min=MIN_MOON_SEP * u.deg)]

# Helper functions


def create_target(event):
    """Create an astroplan FixedTarget from a GCN event."""
    coord = SkyCoord(ra=event.ra * u.deg, dec=event.dec * u.deg, frame="icrs")
    return FixedTarget(coord=coord, name=event.event_id or "GRB")


def mask_airmass(airmass, altaz):
    """Set airmass to NaN when the target is below the horizon or airmass > 5."""
    return np.where((altaz.alt.deg > 0) & (airmass < 5), airmass, np.nan)


def fmt_local(time_value, tz):
    """Format a UTC time value as a string in the specified local timezone."""
    try:
        return mdates.num2date(time_value).astimezone(tz).strftime("%H:%M")
    except Exception:
        return ""


# Filtering


def passes_filters(event) -> bool:
    """
    Instantly reject events that arent worth following up.
    Events with no RA and DEC and missing fields are rejected.
    """
    if event.ra is None or event.dec is None:
        return False

    if event.ra == 0.0 and event.dec == 0.0: 
        return False

    if event.ra_dec_error is not None and event.ra_dec_error > MAX_ERROR_DEG:
        return False

    return True


# Visibility


def night_window(t_ref=None):
    """Return (night_start, night_end) Time objects for the next astronomical night."""
    t_ref = t_ref or Time.now()
    night_start = OBSERVER.twilight_evening_astronomical(t_ref, which="next")
    night_end = OBSERVER.twilight_morning_astronomical(t_ref, which="next")
    if night_end < night_start:  # already dark — use now as start
        night_start = t_ref
    return night_start, night_end


def is_ever_visible(event) -> bool:
    """
    Return True if the event's sky position will be above MIN_ALTITUDE
    at any point during tonight's astronomical night. No moon or other
    constraints — used for the all-alerts channel.
    """
    if event.ra is None or event.dec is None:
        return False

    night_start, night_end = night_window()
    target = create_target(event)
    time_range = Time([night_start, night_end])
    return is_observable(BASIC_CONSTRAINTS, OBSERVER, target, time_range=time_range)[0]


def check_visibility(event, obs_time=None):
    """
    Check whether an event is observable from Greenhill during the
    next night, using astroplan constraints.

    The window is always the next full night, so an alert received during the day will still check
    tonights observability.
    """
    if not passes_filters(event):
        return {"is_observable": False, "best_airmass": None, "observable_hours": 0.0,
                "moon_separation": None, "night_start": None, "night_end": None}

    night_start, night_end = night_window(obs_time)
    time_range = Time([night_start, night_end])
    target = create_target(event)
    base = {
        "night_start": night_start.to_datetime(timezone=HOBART_TZ).strftime("%Y-%m-%d %H:%M %Z"),
        "night_end":   night_end.to_datetime(timezone=HOBART_TZ).strftime("%Y-%m-%d %H:%M %Z"),
    }

    mid = night_start + (night_end - night_start) * 0.5
    moon = get_body("moon", mid, location=LOCATION)
    target_coord = OBSERVER.altaz(mid, target)
    moon_coord   = OBSERVER.altaz(mid, moon)
    moon_sep = round(float(target_coord.separation(moon_coord).deg), 1)

    # Use the constraints to check observability and calculate observable hours and best airmass during the night
    if not is_observable(CONSTRAINTS, OBSERVER, target, time_range=time_range)[0]:
        return {"is_observable": False, "best_airmass": None, "observable_hours": 0.0,
                "moon_separation": moon_sep, **base}

    night_hours = (night_end - night_start).to(u.hour).value
    frac = float(observability_table(CONSTRAINTS, OBSERVER, [target], time_range=time_range)
                 ["fraction of time observable"][0])
    obs_hours = round(frac * night_hours, 2)

    times = night_start + (night_end - night_start) * np.linspace(0, 1, 200)
    altaz = OBSERVER.altaz(times, target)
    above = altaz.alt.deg >= MIN_ALTITUDE
    best_airmass = round(float(np.min(altaz.secz[above])), 2) if np.any(above) else None

    return {"is_observable": True, "best_airmass": best_airmass, "observable_hours": obs_hours,
            "moon_separation": moon_sep, **base}


# Plotting


def plot_visibility(event, filename_prefix="grb", obs_time=None, window_hours=None):
    """Generate airmass and altitude plots for a GCN event."""
    if not passes_filters(event):
        return []

    t_start = obs_time or Time.now()
    t_end = t_start + (window_hours or 24) * u.hour
    times = t_start + (t_end - t_start) * np.linspace(0, 1, 200)
    target = create_target(event)

    plot_title = (
        f"{event.event_id or 'GRB'}  —  {t_start.datetime.strftime('%Y-%m-%d')}\n"
        f"RA {event.ra:.4f}°,  Dec {event.dec:+.4f}°"
    )

    night_s, night_e = night_window(t_start)

    target_altaz = OBSERVER.altaz(times, target)
    moon_altaz   = OBSERVER.altaz(times, get_body("moon", times, location=LOCATION))
    target_am    = mask_airmass(target_altaz.secz.value, target_altaz)
    target_alt   = np.where(target_altaz.alt.deg > 0, target_altaz.alt.deg, np.nan)
    moon_am      = mask_airmass(moon_altaz.secz.value, moon_altaz)
    moon_alt     = np.where(moon_altaz.alt.deg > 0, moon_altaz.alt.deg, np.nan)
    am_cutoff    = 1.0 / np.sin(np.deg2rad(MIN_ALTITUDE))

    time_num  = mdates.date2num(times.datetime)
    is_dark   = (times.unix >= night_s.unix) & (times.unix <= night_e.unix)
    edges     = np.where(np.diff(np.concatenate([[False], (target_altaz.alt.deg >= MIN_ALTITUDE) & is_dark, [False]])))[0]

    def setup_ax(ax):
        plot_airmass(target, OBSERVER, times, altitude_yaxis=False, brightness_shading=True, ax=ax)
        for line in list(ax.lines):
            line.remove()
        ax.set_title(plot_title, fontsize=10)
        ax.set_xlabel("UTC")

    def add_limit_segs(ax, y_val):
        segs = [[(time_num[s], y_val), (time_num[e - 1], y_val)] for s, e in zip(edges[::2], edges[1::2])]
        if segs:
            ax.add_collection(LineCollection(segs, colors="red", linewidth=2.0, zorder=6))

    def add_local_time_axis(ax):
        time_value = ax.get_xticks()
        ax_top = ax.twiny()
        ax_top.set_xlim(ax.get_xlim())
        ax_top.set_xticks(time_value)
        ax_top.set_xticklabels([fmt_local(t, HOBART_TZ) for t in time_value])
        ax_top.set_xlabel("Hobart Local Time")

    def make_plot(target_data, moon_data, y_limit, path, ylabel=None, ylim=None, yfmt=None):
        fig, ax = plt.subplots(figsize=(15, 5))
        setup_ax(ax)
        ax.plot(times.datetime, target_data, color="steelblue", linewidth=2.5, zorder=4, label="Target")
        ax.axhline(y_limit, color="red", linestyle="--", linewidth=1.0, zorder=5, label=f"{MIN_ALTITUDE:.0f}° limit")
        add_limit_segs(ax, y_limit)
        ax.plot(times.datetime, moon_data, color="gray", linestyle=":", linewidth=2, label="Moon")
        if ylabel:
            ax.set_ylabel(ylabel)
        if ylim:
            ax.set_ylim(*ylim)
        if yfmt:
            ax.yaxis.set_major_formatter(matplotlib.ticker.FormatStrFormatter(yfmt))
        add_local_time_axis(ax)
        ax.legend(loc="best")
        fig.tight_layout()
        fig.savefig(path, dpi=150)
        plt.close(fig)

    am_path  = f"{filename_prefix}_airmass.jpg"
    alt_path = f"{filename_prefix}_altitude.jpg"
    make_plot(target_am,  moon_am,  am_cutoff,    am_path)
    make_plot(target_alt, moon_alt, MIN_ALTITUDE, alt_path, ylabel="Altitude (deg)", ylim=(0, 90), yfmt="%.0f°")
    return [am_path, alt_path]
