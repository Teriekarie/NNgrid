import pandas as pd

# Load the dataset

df = pd.read_csv('C:/Users/terre/Ngrid/revised/result.csv')

# Display the first 5 rows to check it's loaded correctly
#print(df.head(5))

# from tabulate import tabulate

# print(tabulate(df.head(4), headers='keys', tablefmt='psql'))


df.drop(columns=['Unnamed: 0'], inplace=True)

df.rename(columns={'Count_of_Zeros': 'Downtime_ID'}, inplace=True)


# max_capacity = df['TotalGeneration'].max()
# max_capacity_details = df[df['TotalGeneration'] == max_capacity]
# print(max_capacity_details)

hourly_columns = [col for col in df.columns if col.endswith(":00")]  # This tries to match columns that end with ":00"

#print("Hourly columns identified:", hourly_columns)

# Function to find the first downtime
def find_first_downtime(row):
    for col in hourly_columns:
        if row[col] == 0:
            return col  # Return the hour the first downtime occurs
    return None  # Return None if there's no downtime

# Apply the function to each row
df['Start_Downtime'] = df.apply(find_first_downtime, axis=1)



# Function to find the last downtime
def find_last_downtime(row):
    for col in reversed(hourly_columns):  # Reverse the list to start checking from the end
        if row[col] == 0:
            return col  # Return the hour the last downtime occurs
    return None  # Return None if there's no downtime

# Apply the function to each row to create the End_Downtime column
df['End_Downtime'] = df.apply(find_last_downtime, axis=1)


# Convert all hourly columns to numeric, coercing errors to NaN (in case of non-numeric values)
for col in hourly_columns:
    df[col] = pd.to_numeric(df[col], errors='coerce')

#Ensure the function returns a series with the right index
def find_max_power_alternative(row):
    max_value = None
    max_index = None
    for col in hourly_columns:
        if pd.notna(row[col]) and (max_value is None or row[col] > max_value):
            max_value = row[col]
            max_index = col
    return pd.Series([max_index, max_value], index=['Max_power_time', 'Max_power'])

# Apply the function across the DataFrame
df_results = df.apply(find_max_power_alternative, axis=1)

# Directly assign the results to the DataFrame
df['Max_power_time'] = df_results['Max_power_time']
df['Max_power'] = df_results['Max_power']

# print(df.head())

def find_min_power_alternative(row):
    # Initialize min_value with a high value to ensure any real value found is lower
    min_value = float('inf')
    min_index = None
    for col in hourly_columns:
        # Check if the value is greater than 0 to avoid downtime
        if pd.notna(row[col]) and row[col] > 0 and row[col] < min_value:
            min_value = row[col]
            min_index = col
    # Handle case where no non-zero min value is found
    if min_index is None:
        return pd.Series([None, None], index=['Min_power_time', 'Min_power'])
    else:
        return pd.Series([min_index, min_value], index=['Min_power_time', 'Min_power'])

# Apply the function across the DataFrame
df_results_min = df.apply(find_min_power_alternative, axis=1)

# Update the DataFrame with the new columns
df['Min_power_time'] = df_results_min['Min_power_time']
df['Min_power'] = df_results_min['Min_power']

#print(df.head())


# Extracting the type within parentheses and assigning it to a new column 'Type'
df['Type'] = df['Genco'].str.extract(r'\((.*?)\)')


# Removing the type in parentheses from the 'Genco' column
df['Genco'] = df['Genco'].str.replace(r'\s*\([^)]*\)', '', regex=True).str.strip()

# Verify the transformation
#print(df[['Genco', 'Type']].head())
#print(df.head())


def count_downtime_periods(row):
    # Convert hourly generation values to binary (1 for uptime, 0 for downtime)
    binary_sequence = [1 if x > 0 else 0 for x in row[hourly_columns]]
    # Count transitions from 0 to 1, which indicate the end of a downtime period
    transitions = sum([1 for i in range(1, len(binary_sequence)) if binary_sequence[i-1] == 0 and binary_sequence[i] == 1])
    return transitions

hourly_columns = [f'{hour:02d}:00' for hour in range(1, 25)]  # This uses leading zeros for hours

df['Downtime_Periods'] = df.apply(count_downtime_periods, axis=1)

#print(df.head(10))

def calculate_restoration_time(row):
    operational_hours = 0
    is_previous_hour_operational = None

    for hour in hourly_columns:
        is_current_hour_operational = row[hour] > 0

        # If the current hour is operational and either it's the first hour
        # or the previous hour was not operational, count as restoration.
        if is_current_hour_operational and (is_previous_hour_operational == False or is_previous_hour_operational is None):
            operational_hours += 1

        # Additionally, if the current hour is operational and so was the previous,
        # it's a continuation of the same operational period.
        elif is_current_hour_operational and is_previous_hour_operational == True:
            operational_hours += 1
        
        is_previous_hour_operational = is_current_hour_operational

    return operational_hours

# Apply the function across the DataFrame to calculate restoration time
df['Restoration_Time'] = df.apply(calculate_restoration_time, axis=1)

#print(df.head(10))

df.rename(columns={'Restoration_Time': 'Operational_Hours'}, inplace=True)

df['Operational_Hours'] = df['Operational_Hours'].apply(lambda x: f"{x:02d}:00")


def identify_operational_periods(row):
    operational_periods = []  # List to store the duration of each operational period
    current_period_start = None  # Track the start of the current operational period

    for i, col in enumerate(hourly_columns):
        if row[col] > 0:
            if current_period_start is None:
                current_period_start = i  # Operational period starts
        else:
            if current_period_start is not None:
                # Operational period ends, calculate its duration
                operational_periods.append(i - current_period_start)
                current_period_start = None  # Reset for the next operational period
    
    # Check if the last period extends to the end of the day
    if current_period_start is not None:
        operational_periods.append(len(hourly_columns) - current_period_start)
    
    return operational_periods

df['Operational_Periods_Durations'] = df.apply(identify_operational_periods, axis=1)

df['Average_Operational_Period_Duration'] = df['Operational_Periods_Durations'].apply(lambda periods: sum(periods)/len(periods) if periods else 0)

#print(df.head(10))

# Convert "HH:00" format to numeric hours
df['Total_Operational_Hours'] = df['Operational_Hours'].apply(lambda x: int(x.split(':')[0]))

# Assuming 24 hours in a day
df['Total_Downtime'] = 24 - df['Total_Operational_Hours']

def calculate_avg_restoration(row):
    num_downtime_periods = len(row['Operational_Periods_Durations']) - 1
    if num_downtime_periods > 0:
        return row['Total_Downtime'] / num_downtime_periods
    return None  # Return None or a specific value to indicate no downtime or no restoration needed

df['Average_Restoration_Time'] = df.apply(calculate_avg_restoration, axis=1)

####Standardizing the dataframe

def format_hours_to_time_format(hours):
    # Convert numeric hours into "H:00" format, handle None values
    if pd.notna(hours):
        return f"{int(hours)}:00"
    return None

df['Average_Restoration_Time'] = df['Average_Restoration_Time'].apply(format_hours_to_time_format)
df['Total_Downtime'] = df['Total_Downtime'].apply(format_hours_to_time_format)
df['Total_Operational_Hours'] = df['Total_Operational_Hours'].apply(lambda x: f"{x}:00")  # Assuming it's already an integer
df['Average_Operational_Period_Duration'] = df['Average_Operational_Period_Duration'].apply(format_hours_to_time_format)

df['Min_power'] = df['Min_power'].round(2)
df['Max_power'] = df['Max_power'].round(2)


# Get a list of all column names
cols = list(df.columns)
# Remove 'Type' from its current position
cols.remove('Type')
# Insert 'Type' right after 'Genco'
genco_index = cols.index('Genco')
cols.insert(genco_index + 1, 'Type')

# Reindex the DataFrame with the new column order
df = df[cols]

# Assume 'cols' still holds the updated column order from the previous step
# Move 'Downtime_Periods' next to 'Operational_Periods_Durations'
ops_index = cols.index('Operational_Periods_Durations')
if 'Downtime_Periods' in cols:  # Check if 'Downtime_Periods' already in cols to avoid KeyError
    cols.remove('Downtime_Periods')
cols.insert(ops_index + 1, 'Downtime_Periods')

# Reindex the DataFrame with the new column order
df = df[cols]


#print(df.head())

# Save the modified DataFrame
#df.to_csv("C:/Users/terre/Ngrid/revised/master_sheet_3.csv", index=False)

#df.to_excel('C:/Users/terre/Ngrid/revised/master_sheet_4.xlsx', index=False)









