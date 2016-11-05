from collections import namedtuple

time_windows= {
  '30s': 30,
  '1m': 60,
  '5m': 300,
  '15m': 900,
  '60m': 3600,
  '120m': 7200,
}

Reading=namedtuple("Reading","id ts value property plug_id hh_id h_id")
State=namedtuple("State","last_ts load count")
