import pandas as pd

# Get noise and homeless complaints, group by date
df_all = pd.read_csv('transformed_311_final.csv')
df_noise_only = df_all[df_all['complaint_type'].map(lambda x: 'Noise' in x)]
df_homeless_only = df_all[df_all['complaint_type'].map(lambda x: 'Homeless' in x)]

all_complaints = df_all.groupby('date').count()
noise_complaints = df_noise_only.groupby('date').count()
homeless_complaints = df_homeless_only.groupby('date').count()

# Reformat and merge DataFrames
all_complaints.reset_index(inplace=True)
all_complaints.drop(['complaint_type', 'descriptor'], axis=1, inplace=True)
noise_complaints.reset_index(inplace=True)
noise_complaints.drop(['complaint_type', 'descriptor'], axis=1, inplace=True)
homeless_complaints.reset_index(inplace=True)
homeless_complaints.drop(['complaint_type', 'descriptor'], axis=1, inplace=True)

merged_complaints = all_complaints.merge(noise_complaints, on='date')
merged_complaints = merged_complaints.merge(homeless_complaints, on='date')

# Get dates and counts as lists
dates = merged_complaints['date']
all_counts = merged_complaints['unique_key_x']
noise_counts = merged_complaints['unique_key_y']
homeless_counts = merged_complaints['unique_key']

# Write to output file
with open('noise_complaints_by_day.csv', 'w') as outfile:
    outfile.write('date,all_complaints,noise_complaints,homeless_complaints\n')
    for i in range(len(merged_complaints)):
        outfile.write(dates[i] + ',' + str(all_counts[i]) + ',' + str(noise_counts[i]) + ',' + str(homeless_counts[i]) + '\n')
