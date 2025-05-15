print("""
Author: Jamil Mendez
Version: 0.1.0
License: MIT
      
OEM Shipments Data Processor

Requires: pandas
Install with:
    pip install pandas

----------------------------------------
""")

__version__ = "v.0.1.0"
__author__ = "Jamil Mendez"

import sys
import subprocess

# --- Library Check ---
# Check if pandas and pathlib are installed
try:
    import pandas as pd
    from pathlib import Path
    import re
    print("Required libraries (pandas, pathlib) are installed.")
except ImportError as e:
    print(f"Error: A required library is not installed.")
    print(f"Details: {e}")
    print("\nPlease install the necessary libraries using the following command:")
    print("pip install pandas") # pathlib is usually built-in
    sys.exit("Exiting. Please install the required libraries and run the script again.")

# --- Data Processing Function ---
def process_data_from_folder(data_folder_path):
    """
    Processes raw data files (Backlog, Open Orders, and Forecast) and a segment lookup file
    from a specified folder, cleans and combines them, and saves the results.

    Args:
        data_folder_path (str): The path to the folder containing the raw data files.
    """
    data_path = Path(data_folder_path)

    segment_lookup = None
    # Find and read the segment lookup Excel file
    print(f"Searching for segment lookup file in: {data_path}")
    segment_files = list(data_path.glob('*Segment.xlsx'))
    if segment_files:
        segment_lookup_path = segment_files[0] # Assuming the first found is the correct one
        print(f"Reading segment lookup file: {segment_lookup_path.name}")
        segment_lookup = pd.read_excel(segment_lookup_path)
    else:
        print("Error: Segment lookup file (*Segment.xlsx) not found in the specified folder.")
        return

    backlog_df = None
    orders_df = None
    forecast_df = None

    # Loop through all CSV files in the specified data path
    print(f"Searching for data CSV files in: {data_path}")
    for i in data_path.glob('*.csv'):
        print(f"Checking file: {i.name}")
        # Check file name to identify the dataset type
        if "Backlog" in i.name:
            print(f"Reading Backlog file: {i.name}")
            backlog_df = pd.read_csv(i ,low_memory= False)
        elif "Open" in i.name:
            print(f"Reading Open Orders file: {i.name}")
            # Reads with default header=True
            orders_df = pd.read_csv(i ,low_memory= False)
        elif "Forecast" in i.name:
            print(f"Reading Forecast file: {i.name}")
            # Reads with no header and drops the first row
            forecast_df = pd.read_csv(i ,header= None ,low_memory= False)
            if not forecast_df.empty:
                forecast_df.drop(index=0, axis= 'rows',inplace= True)
            else:
                 print(f"Warning: Forecast file {i.name} is empty.")
        else:
            # Skip any other CSV files
            print(f"Skipping file: {i.name}")

    # Check if all necessary dataframes were loaded
    if orders_df is None or forecast_df is None or backlog_df is None:
        print("Error: One or more required data files (Backlog, Open, Forecast CSVs) not found or could not be read.")
        # Check specifically which ones are missing
        if backlog_df is None: print("- Backlog file not found or empty.")
        if orders_df is None: print("- Open Orders file not found or empty.")
        if forecast_df is None: print("- Forecast file not found or empty.")
        return

    # --- Open Orders and Forecast Dataset Processing ---

    print("Processing Orders and Forecast data...")
    # Assign columns to forecast_df based on orders_df columns
    # This column assignment assumes 'Open' was read correctly with headers.
    if not orders_df.empty and not forecast_df.empty:
        if len(orders_df.columns) == len(forecast_df.columns):
             forecast_df.columns = list(orders_df.columns)
        else:
             print("Error: Column count mismatch between Open Orders and Forecast dataframes. Cannot assign columns.")
             return
    elif orders_df.empty:
         print("Error: Open Orders dataframe is empty. Cannot assign columns to Forecast.")
         return
    elif forecast_df.empty:
         print("Error: Forecast dataframe is empty. Cannot proceed with concatenation.")
         return


    # Combining the two files
    orders_forecast_df = pd.concat([orders_df,forecast_df],ignore_index=True)

    # Adding mmm-yy in Filler column referencing 'schedule ship date'.
    if 'schedule ship date' in orders_forecast_df.columns:
        try:
            orders_forecast_df['Filler'] = pd.to_datetime(orders_forecast_df['schedule ship date'], format= '%m/%d/%y', errors='coerce').dt.strftime('%b-%y').str.upper().apply(lambda x: f'="{x}"')
        except Exception as e:
            print(f"Warning: Could not convert 'schedule ship date' to datetime. Error: {e}")
            orders_forecast_df['Filler'] = None # Assign None if conversion fails
    else:
        print("Warning: 'schedule ship date' column not found in combined Orders/Forecast data. Cannot add 'Filler' column.")
        orders_forecast_df['Filler'] = None # Ensure column exists


    # Adding column for segment.
    merge_keys_of = ['ect_region','application_code']
    if all(key in orders_forecast_df.columns for key in merge_keys_of) and all(key in segment_lookup.columns for key in merge_keys_of):
        orders_forecast_df = orders_forecast_df.merge(right=segment_lookup, how= 'left', on= merge_keys_of)
    else:
        print(f"Warning: Merge keys {merge_keys_of} not found in both Orders/Forecast and Segment Lookup data. Skipping merge for Orders/Forecast.")
        # Add a placeholder 'Segment' column if merge failed
        if 'Segment' not in orders_forecast_df.columns:
             orders_forecast_df['Segment'] = None

    # Assign 'Intercompany' segment for internal customer types and fillna with 0.
    if 'customer_type' in orders_forecast_df.columns and 'Segment' in orders_forecast_df.columns:
        orders_forecast_df.loc[orders_forecast_df['customer_type'] == 'Internal' ,'Segment'] = 'Intercompany'
        orders_forecast_df['Segment'] = orders_forecast_df['Segment'].fillna(0)
    else:
        print("Warning: 'customer_type' or 'Segment' column not found in Orders/Forecast data. Cannot assign 'Intercompany' segment.")


    # --- Shipment Dataset Processing ---

    print("Processing Shipment data...")
    # Replacing the column name 'World Area' to 'ect_region'.
    if 'World Area' in backlog_df.columns:
        backlog_df.rename(columns= {'World Area':'ect_region'},inplace= True)
    else:
        print("Warning: 'World Area' column not found in Backlog data. Skipping rename.")


    # Adding column for segment.
    merge_keys_bl = ['ect_region','application_code']
    if all(key in backlog_df.columns for key in merge_keys_bl) and all(key in segment_lookup.columns for key in merge_keys_bl):
        backlog_df = backlog_df.merge(right=segment_lookup, how= 'left', on= merge_keys_bl)
    else:
        print(f"Warning: Merge keys {merge_keys_bl} not found in both Backlog and Segment Lookup data. Skipping merge for Backlog.")
        # Add a placeholder 'Segment' column if merge failed
        if 'Segment' not in backlog_df.columns:
             backlog_df['Segment'] = None

    # Assign 'Intercompany' segment for internal customer types and fillna with 0.
    if 'customer_type' in backlog_df.columns and 'Segment' in backlog_df.columns:
        backlog_df.loc[backlog_df['customer_type'] == 'Internal' ,'Segment'] = 'Intercompany'
        backlog_df['Segment'] = backlog_df['Segment'].fillna(0)
    else:
        print("Warning: 'customer_type' or 'Segment' column not found in Backlog data. Cannot assign 'Intercompany' segment.")


    # --- Saving Processed Data ---

    print("Saving processed data...")
    # Save the processed backlog dataframe to a new CSV file
    backlog_df.to_csv(data_path / '0_Clean_Shipments.csv',index= False)
    # Save the combined and processed orders and forecast dataframe to a new CSV file
    orders_forecast_df.to_csv(data_path /'0_Clean_Orders and Forecast.csv',index= False)

    print("Processing complete. Cleaned files saved.")


# Main execution block
if __name__ == "__main__":
    # This check is already done at the top level before function definition
    # If we reached here, libraries are installed.
    pass # No need to repeat the check here

    # Prompt user for the folder path containing the data files and normalize path separators
    folder_path = re.sub("\"|'","",input('Input the folder path:'))
    data_folder = folder_path.replace('\\','/')
    process_data_from_folder(data_folder)