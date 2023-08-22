from pyopensky import OpenskyImpalaWrapper
import pandas as pd
from datetime import datetime, timedelta
import numpy as np

import matplotlib.pyplot as plt
from scipy import stats

def get_data(serial_input):
    opensky = OpenskyImpalaWrapper()

    time_col = "time"

    serial = serial_input

    time_mid = '2022-06-23T00:00' #'%Y-%m-%dT%H:%M'
    time_mid = datetime.strptime(time_mid, '%Y-%m-%dT%H:%M')
    duration = 72 #hour
    duration *= 60

    series_opensky = pd.date_range(start=(time_mid).strftime('%Y-%m-%d %H:%M:%S'),
                                    end=(time_mid + timedelta(minutes=duration)).strftime('%Y-%m-%d %H:%M:%S'), freq="1min").strftime('%Y-%m-%d %H:%M:%S')

    print(series_opensky)

    visible_ac = []
    mean_of_all_update_interval = []
    all_update_interval = []

    time_start = []
    time_end = []

    nb_data = len(series_opensky) - 1

    interval = 15

    for i in range(0, nb_data, interval):
        print(series_opensky[i], series_opensky[i+1])
        
        ts_start = pd.Timestamp(series_opensky[i], tz="utc").timestamp()
        ts_end = pd.Timestamp(series_opensky[i+1], tz="utc").timestamp()
        hour_start = ts_start // 3600 * 3600
        hour_end = (ts_end // 3600 + 1) * 3600
        
        query = "SELECT * FROM state_vectors_data4, state_vectors_data4.serials s WHERE " 
        # query += "lat<={} AND lat>={} AND lon<={} AND lon>={} ".format(lat1, lat2, lon1, lon2)
        query += "hour>={} ".format(hour_start)
        query += "AND hour<={} ".format(hour_end)
        query += "AND {}>={} ".format(time_col, ts_start)
        query += "AND {}<={} ".format(time_col, ts_end)
        query += "AND s.ITEM = {}".format(serial)
        
        print("Processing: {}/{}".format(i+1,(len(series_opensky) - 1)))
        print(query)
    #     df.append(opensky.rawquery(query));

        df = opensky.rawquery(query)
        
        if(df is not None):
            df.dropna(subset=["callsign"], inplace=True)
            df = df[df["onground"] == False]

            df_grouped = df.groupby('icao24', group_keys=True).apply(lambda x: x)
            df_grouped['updateinterval'] = df_grouped['lastposupdate'].diff()

            df_grouped = df_grouped[(df_grouped['updateinterval'] > 0) & (df_grouped['updateinterval'] < 50)]

            ungrouped_df = df_grouped.reset_index(drop=True)
            grouped_df = ungrouped_df.groupby('icao24', as_index=False)

            mean_updateinterval_by_icao24 = grouped_df['updateinterval'].mean()

            mylist = mean_updateinterval_by_icao24['updateinterval']
            bin_width = 0.1
            bins=int((max(mylist) - min(mylist)) / bin_width)
            
            visible_ac.append(len(df['icao24'].unique()))
            mean_of_all_update_interval.append(ungrouped_df['updateinterval'].mean())
            all_update_interval.extend(ungrouped_df['updateinterval'])

            time_start.append(ts_start)
            time_end.append(ts_end)

        
    filtered_all_update_interval = [x for x in all_update_interval if x <= 10]


    return visible_ac, mean_of_all_update_interval, time_start, filtered_all_update_interval

def get_figure(serial, visible_ac, mean_of_all_update_interval, time_start):
    print("Get Figure: Visible AC, Mean of Update Interval")

    plt.figure(1)
    plt.clf()
    plt.scatter(time_start, visible_ac)
    plt.title("Sensor ID: {}".format(serial))
    plt.xlabel("Time")
    plt.ylabel("Nb Visible Aircraft")
    plt.savefig('scrap_results/images/time_nbac_{}.png'.format(serial))

    plt.figure(2)
    plt.clf()
    plt.scatter(time_start, mean_of_all_update_interval)
    plt.title("Sensor ID: {}".format(serial))
    plt.xlabel("Time")
    plt.ylabel("Mean of Update Interval")
    plt.savefig('scrap_results/images/time_mean_up_{}.png'.format(serial))

    plt.figure(3)
    plt.clf()
    plt.scatter(visible_ac, mean_of_all_update_interval)
    plt.title("Sensor ID: {}".format(serial))
    plt.xlabel("Nb Visible Aircraft")
    plt.ylabel("Mean of Update Interval")
    plt.savefig('scrap_results/images/nbac_mean_up_{}.png'.format(serial))

def categorize_update_interval(all_update_interval):
    result_dict = {}

    for num in all_update_interval:
        rounded_value = round(num, 1)
        if rounded_value not in result_dict:
            result_dict[rounded_value] = 0
        result_dict[rounded_value] += 1

    result_dict = dict(sorted(result_dict.items()))

    return result_dict

def get_histogram(update_interval_dict):    
    print("Get Figure: Update Interval Histogram")
    # Example usage:
    # result_dict = categorize_floats_into_dictionary(all_update_interval)
    # result_dict = dict(sorted(result_dict.items()))

    cum_freq = []
    cum_freq_one = 0

    for key in update_interval_dict.keys():
        cum_freq_one += update_interval_dict[key]
        cum_freq.append(cum_freq_one)

    x_values = list(update_interval_dict.keys())
    y_values = list(update_interval_dict.values())

    y_values_total = sum(y_values)
    y_values = np.array(y_values)/y_values_total
    cum_freq = np.array(cum_freq)/y_values_total*100

    plt.figure(4)
    plt.clf()
    plt.bar(x_values, y_values, width=0.1, edgecolor='black')
    # Add labels and title
    plt.xlabel('Update Interval [s]')
    plt.ylabel('Frequency [%]')
    plt.title('Sensor ID: {} \n  Update Interval Distribution'.format(serial))
    plt.savefig('scrap_results/images/freq_{}.png'.format(serial))

    plt.figure(5)
    plt.clf()
    plt.plot(x_values, cum_freq, '-o')
    plt.xlabel('Update Interval [s]')
    plt.ylabel('Cumulative Frequency [%]')
    plt.title('Sensor ID: {} \n  Update Interval Distribution'.format(serial))
    plt.savefig('scrap_results/images/cumul_freq_{}.png'.format(serial))

def save_update_interval(serial, update_interval_dict):
    data_1 = list(update_interval_dict.values())
    dict_update_interval_modified = {serial: data_1}
    df_update_interval = pd.DataFrame.from_dict(dict_update_interval_modified, orient='index')
    df_update_interval.to_csv('scrap_results/csv/update_interval_{}.csv'.format(serial))

def save_traf_update_interval(serial, time_start, visible_ac, mean_update_interval):
    data = {'time_start': time_start,
            'visible_ac': visible_ac,
            'mean_update_interval': mean_update_interval}

    df_traf = pd.DataFrame(data)
    df_traf.to_csv('scrap_results/csv/traf_update_interval_{}.csv'.format(serial), index=False)

def save_linear_reg(visible_ac, mean_of_all_update_interval):
    y = mean_of_all_update_interval
    x = visible_ac

    slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)
    data = {
        'serial': [serial],
        'slope': [slope],
        'intercept': [intercept],
        'r_value': [r_value],
        'p_value': [p_value],
        'std_err': [std_err]
    }

    df_lin_reg = pd.DataFrame(data)

    df_lin_reg.to_csv('scrap_results/linear_regression/linear_regression_{}.csv'.format(serial), index=False)

if __name__ == "__main__":
    # serial_id_list = [1433801924, -1408231729, -1408235490, -1408236264, 1035116639, 696601879, 91621, 835009260, 693642540,
    #                     807483346, 214755386, -1408231797, -1408235040, 13042006, -1408231792, 1344380651, 83165709, -1408234983,
    #                     -1408231781, 1498137442, -1408231854, 1163147025, 91338, -1408231724, -1408237098]

    serial_id_list = [1344380651, 83165709, -1408234983, -1408231781, 1498137442]


    for serial in serial_id_list:
        visible_ac, mean_update_interval, time_start, all_update_interval = get_data(serial)

        get_figure(serial, visible_ac, mean_update_interval, time_start)
        update_interval_dict = categorize_update_interval(all_update_interval)

        print(update_interval_dict)

        save_traf_update_interval(serial, time_start, visible_ac, mean_update_interval)
        save_update_interval(serial, update_interval_dict)
        save_linear_reg(visible_ac, mean_update_interval)
        get_histogram(update_interval_dict)

    print("Fin.")
