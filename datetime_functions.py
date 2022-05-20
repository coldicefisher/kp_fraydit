from datetime import datetime, timezone
import pytz

def now_as_string():        
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%d %H:%M:%S")

def today_as_string():        
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%d")

def utc_now_as_long():
    now = datetime.now(tz=timezone.utc)
    new_val = now.strftime("%Y%m%d%H%M%S")
    return int(new_val)

def newyork_now_as_long():
    now = datetime.now(tz=pytz.timezone('America/New_York'))
    new_val = now.strftime("%Y%m%d%H%M%S")
    return int(new_val)

