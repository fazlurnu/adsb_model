import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os

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

    plt.figure(1)
    plt.clf()
    plt.bar(x_values, y_values, width=0.1, edgecolor='black')
    # Add labels and title
    plt.xlabel('Update Interval [s]')
    plt.ylabel('Frequency [%]')
    plt.title('Date: {} \n  Update Interval Distribution'.format(date))
    plt.savefig('scrap_results/images_tudelft/freq_{}.png'.format(date))
    plt.axis([0, 50, 0, 0.165])

    plt.figure(2)
    plt.clf()
    plt.plot(x_values, cum_freq, '-o')
    # Add labels and title
    plt.xlabel('Update Interval [s]')
    plt.ylabel('Frequency [%]')
    plt.title('Date: {} \n  Update Interval Distribution'.format(date))
    plt.savefig('scrap_results/images_tudelft/cumul_freq_{}.png'.format(date))
    plt.axis([0, 50, 0, 100])

if __name__ == "__main__":
    directory = 'adsb_delft/csv'

    files = []

    for filename in os.listdir(directory):
        f = os.path.join(directory, filename)
        # checking if it is a file
        if os.path.isfile(f):
            files.append(f)
            
    files.sort()

    for file in files:
        date = file[28:-4]

        df = pd.read_csv(file)
        df.dropna(subset=["callsign"], inplace=True)

        df_grouped = df.groupby('icao', group_keys=True).apply(lambda x: x)
        df_grouped['updateinterval'] = df_grouped['ts'].diff()
        df_grouped = df_grouped[(df_grouped['updateinterval'] > 0) & (df_grouped['updateinterval'] < 50)]
        ungrouped_df = df_grouped.reset_index(drop=True)

        all_update_interval = ungrouped_df['updateinterval'].tolist()
        update_interval_dict = categorize_update_interval(all_update_interval)

        get_histogram(update_interval_dict)

