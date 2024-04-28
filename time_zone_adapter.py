from datetime import datetime
from tzlocal import get_localzone
import pytz

#to adjust the timezone according to UTC +3 -> comply with the time-based analysis outputs
class TimeZoneAdapter:
    @staticmethod
    def convert_to_utc3():
        local_tz = get_localzone()
        local_time = datetime.now(local_tz)
        utc_plus3_tz = pytz.timezone('Etc/GMT-3') 
        utc_plus3_time = local_time.astimezone(utc_plus3_tz)
    
        return utc_plus3_time
    

#adjust incoming binance API time-series data based on timezone -> convert to UTC +3
    def adjust_time(self,unix_time):
        time=unix_time/1000
        dt_obj = datetime.fromtimestamp(time)
        expected_timezone = pytz.timezone('Etc/GMT-3')
        expected_date = dt_obj.astimezone(expected_timezone)

        return expected_date.strftime("%Y-%m-%d %H:%M:%S")   



