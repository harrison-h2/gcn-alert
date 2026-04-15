# gcn-alert
A real-time GRB alert bot that streams GCN Kafka notices from Fermi, SVOM, and Einstein Probe and posts Discord alerts with visibility for Greenhill Observatory.

## What it does
- subscribes to GCN Kafka topics for FERMI GBM/LAt, SVOM ECLAIRs/GRm and the Einstein probe WXT.
- Parses VOEvent XML and JSON alerts fomrats into single event structure
- Checks the observability of GRB alerts using astroplan, by default it is set to Greenhill Observatory
- Sends alerts to discord via webhooks
- Create Airmass and altitude plots for targets that are visible.
- Two seperate channels, All-alerts any real event currently above the horizon and Filtered events that pass quality cuts and are observable tonight, with airmass/altitude plots attached

## Layout

**gcn_connect.py** Entry point, the Kafka consumer loop

**event_handle.py**  Parses raw GCN messages into GCNEvent dataclass

**vis_check.py**  Filtering, visibility, and plot generation (astroplan)

**alert_discord.py**  Discord webhook formatting and posting


## Setup

### 1. Clone and create the enviroment
I have created a conda enviroment

```python

git clone https://github.com/harrison-h2/gcn_alerts.git

cd gcn_alerts

conda env create -f environment.yml

conda activate gcn_alert

```

### 2. Configure Credentials
You will need to configure your KAFKA credentials and webhook links


```bash

cp .env.example .env

```

Edit `.env` and fill in your credentials:

`CLIENT_ID` / `CLIENT_SECRET` come from the gcn [gcn.nasa.gov](https://gcn.nasa.gov) -> Sign in -> Client Credentials and add new
 Webhooks links can be created in a dicord server of your choosing: Discord server -> Settings -> Integrations -> Webhooks

### 3. Run
We access the script all through the gcn_alerts.py

```bash

conda activate gcn_alerts

python gcn_connect.py

```

## Configuration and modifications

Observatory location and filter thresholds are set at the top of `vis_check.py`:


| Filter | Default | Description |
|---|---|---|
| `MAX_ERROR_DEG` | 1.0° | Maximum position error radius |
| `MIN_ALTITUDE` | 30° | Minimum target altitude |
| `MIN_MOON_SEP` | 30° | Minimum separation from the Moon |


  
