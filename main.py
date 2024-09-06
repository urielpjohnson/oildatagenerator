import pandas as pd
import numpy as np


def generate_wellbore_data(num_rows=1_000_000, chunk_size=100_000, save_csv=True):
    # Column names for the dataset
    columns = [
        'Well_ID', 'Depth_ft', 'Pressure_psi', 'Temperature_F', 'Date_Logged',
        'Status', 'Latitude', 'Longitude', 'Operator', 'Formation',
        'Porosity', 'Permeability', 'Mud_Weight_ppg', 'Casing_Size_in',
        'Cement_Type', 'Spud_Date', 'Completion_Date', 'Last_Inspection',
        'Production_Rate_bbl', 'Water_Cut_percent'
    ]

    def create_chunk(start, end):
        data = {
            'Well_ID': np.random.choice(['W1', 'W2', 'W3', 'W4', 'W5', 'W6', 'W7', 'W8', 'W9', 'W10'], end - start),
            'Depth_ft': np.random.choice([1000, 2500, 5000, 7500, 8500, 9000, 15000, -100, None], end - start),
            'Pressure_psi': np.random.choice([3500, 4500, 8000, 'unknown', 7000, 6000, 12000, -1000, None],
                                             end - start),
            'Temperature_F': np.random.choice([210, 220, 230, 250, 260, 270, 280, 290, 300, None], end - start),
            'Date_Logged': np.random.choice(
                ['2023-01-15', '2023/02/20', '15-03-2023', '2023.04.25', None, '2023-06-30', '30-06-2023', '2023-08-15',
                 '2023/09/01', '2023-10-20'], end - start),
            'Status': np.random.choice(['Active', 'Inactive', 'ACTIVE', 'inactive', None], end - start),
            'Latitude': np.random.uniform(-90, 90, end - start),
            'Longitude': np.random.uniform(-180, 180, end - start),
            'Operator': np.random.choice(['Operator_A', 'Operator_B', 'Operator_C', 'Operator_D', None], end - start),
            'Formation': np.random.choice(['Sandstone', 'Shale', 'Limestone', 'Dolomite', None], end - start),
            'Porosity': np.random.uniform(0, 30, end - start),
            'Permeability': np.random.uniform(0, 1000, end - start),
            'Mud_Weight_ppg': np.random.choice([8.4, 9.2, 10.5, 12.0, 'unknown', None], end - start),
            'Casing_Size_in': np.random.choice([7, 9.625, 13.375, 20, None], end - start),
            'Cement_Type': np.random.choice(['Type_G', 'Type_H', 'Class_A', None], end - start),
            'Spud_Date': np.random.choice(['2022-01-01', '2021-06-15', '2020-11-05', None], end - start),
            'Completion_Date': np.random.choice(['2023-05-01', '2023-04-20', '2023-07-10', None], end - start),
            'Last_Inspection': np.random.choice(['2023-08-01', '2023-07-01', None], end - start),
            'Production_Rate_bbl': np.random.choice([100, 250, 500, 750, 1000, None], end - start),
            'Water_Cut_percent': np.random.choice([0, 10, 20, 30, 40, 50, None], end - start)
        }
        df_chunk = pd.DataFrame(data)

        # Introduce empty cells randomly
        df_chunk.loc[np.random.choice(df_chunk.index, 5000, replace=False), 'Well_ID'] = ''
        df_chunk.loc[np.random.choice(df_chunk.index, 10000, replace=False), 'Depth_ft'] = ''
        df_chunk.loc[np.random.choice(df_chunk.index, 10000, replace=False), 'Pressure_psi'] = ''
        df_chunk.loc[np.random.choice(df_chunk.index, 10000, replace=False), 'Temperature_F'] = ''
        df_chunk.loc[np.random.choice(df_chunk.index, 10000, replace=False), 'Date_Logged'] = ''

        # Introduce duplicates randomly
        duplicate_indices = np.random.choice(df_chunk.index, 5000, replace=False)
        df_chunk = pd.concat([df_chunk, df_chunk.loc[duplicate_indices]])

        # Introduce outliers
        df_chunk.loc[np.random.choice(df_chunk.index, 100, replace=False), 'Depth_ft'] = 50000  # Extreme depth value
        df_chunk.loc[
            np.random.choice(df_chunk.index, 100, replace=False), 'Pressure_psi'] = 100000  # Extreme pressure value
        df_chunk.loc[
            np.random.choice(df_chunk.index, 100, replace=False), 'Temperature_F'] = 1000  # Extreme temperature value

        return df_chunk

    # Generate the dataset in chunks and concatenate
    chunks = [create_chunk(i, i + chunk_size) for i in range(0, num_rows, chunk_size)]
    df_large = pd.concat(chunks, ignore_index=True)

    # Save the dataset to a CSV file
    if save_csv:
        df_large.to_csv('wellbore_data_large.csv', index=False)

    return df_large


# Generate the dataset and save to CSV
df_large = generate_wellbore_data()
