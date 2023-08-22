from pyopensky import OpenskyImpalaWrapper
import pandas as pd
from datetime import datetime, timedelta
import numpy as np

import matplotlib.pyplot as plt


def get_data(serial_input):
    opensky = OpenskyImpalaWrapper()

    time_col = "s.mintime"

    serial = serial_input

    time_mid = '2023-01-01T00:00' #'%Y-%m-%dT%H:%M'
    time_mid = datetime.strptime(time_mid, '%Y-%m-%dT%H:%M')
    duration = 48 #hour
    duration *= 60
    interval = 10 #minutes

    series_opensky = pd.date_range(start=(time_mid).strftime('%Y-%m-%d %H:%M:%S'),
                                    end=(time_mid + timedelta(minutes=duration)).strftime('%Y-%m-%d %H:%M:%S'), freq="1min").strftime('%Y-%m-%d %H:%M:%S')

    print(series_opensky)

    nb_data = len(series_opensky) - 1

    df = []

    for i in range(0, nb_data, interval):
        print(series_opensky[i], series_opensky[i+1])
        
        ts_start = pd.Timestamp(series_opensky[i], tz="utc").timestamp()
        ts_end = pd.Timestamp(series_opensky[i+1], tz="utc").timestamp()
        hour_start = ts_start // 3600 * 3600
        hour_end = (ts_end // 3600 + 1) * 3600
        
        query = "SELECT * FROM position_data4, position_data4.sensors s WHERE " 
        # query += "lat<={} AND lat>={} AND lon<={} AND lon>={} ".format(lat1, lat2, lon1, lon2)
        query += "hour>={} ".format(hour_start)
        query += "AND hour<={} ".format(hour_end)
        query += "AND {}>={} ".format(time_col, ts_start)
        query += "AND {}<={} ".format(time_col, ts_end)
        query += "AND s.ITEM.SERIAL = {}".format(serial)
        
        print("Processing: {}/{}".format(i+1,nb_data))
        print(query)
    #     df.append(opensky.rawquery(query));

        df_loc = opensky.rawquery(query)
        
        # if(df_loc is not None):
        #     df_loc.dropna(subset=["callsign"], inplace=True)
        #     df_loc = df_loc[df_loc["onground"] == False]

        df.append(df_loc)

    df = pd.concat(df)

    df.to_csv('pos_table_{}.csv'.format(serial_input))

if __name__ == "__main__":
    # serial_id_list = [1433801924, -1408231729, -1408235490, -1408236264, 1035116639, 696601879, 91621, 835009260, 693642540,
    #                     807483346, 214755386, -1408231797, -1408235040, 13042006, -1408231792, 1344380651, 83165709, -1408234983,
    #                     -1408231781, 1498137442, -1408231854, 1163147025, 91338, -1408231724, -1408237098]

    # widest coverage -1408235277
    serial_id_list = [1433801924, -1408237098]

    for serial in serial_id_list:
        get_data(serial)