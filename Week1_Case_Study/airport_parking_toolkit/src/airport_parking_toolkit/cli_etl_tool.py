# import argparse
# import os
# import pandas as pd
# import logging

# logging.basicConfig(
#     filename='parking_etl.log',
#     level=logging.INFO,
#     format='%(asctime)s %(levelname)s %(message)s'
# )

# def read_csvs(input_path):
#     logging.info("Reading CSV files...")
#     return {
#         "vehicles": pd.read_csv(os.path.join(input_path, "vehicles.csv")),
#         "parking_zones": pd.read_csv(os.path.join(input_path, "parking_zones.csv")),
#         "parking_events": pd.read_csv(os.path.join(input_path, "parking_events.csv"))
#     }

# def validate_events(df):
#     logging.info("Validating parking_events data...")

#     # Convert to datetime
#     df["entry_time"] = pd.to_datetime(df["entry_time"], errors='coerce')
#     df["exit_time"] = pd.to_datetime(df["exit_time"], errors='coerce')

#     # Drop invalid timestamps
#     df = df.dropna(subset=["entry_time", "exit_time"])

#     # Apply validation rules
#     valid_df = df[
#         (df["exit_time"] >= df["entry_time"]) &
#         (df["paid_amount"] >= 0) &
#         (df["vehicle_id"].notnull()) &
#         (df["zone_id"].notnull())
#     ]

#     logging.info(f"{len(valid_df)} valid records out of {len(df)}")
#     return valid_df

# def write_parquet(df, output_path):
#     os.makedirs(output_path, exist_ok=True)
#     output_file = os.path.join(output_path, "cleaned_parking_events.parquet")
#     df.to_parquet(output_file, index=False)
#     logging.info(f"Written output to {output_file}")

# def main():
#     parser = argparse.ArgumentParser(description="ETL for Airport Parking Data")
#     parser.add_argument("--input", required=True, help="Path to input CSVs")
#     parser.add_argument("--output", required=True, help="Path to output file")

#     args = parser.parse_args()

#     logging.info("Starting ETL job...")

#     dfs = read_csvs(args.input)
#     cleaned_events = validate_events(dfs["parking_events"])
#     write_parquet(cleaned_events, args.output)

#     logging.info("ETL job completed.")

# if __name__ == "__main__":
#     main()


import argparse
import os
import pandas as pd
import logging

# Configure logging
logging.basicConfig(
    filename='parking_etl.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)

def read_csvs(input_path):
    logging.info("Reading CSV files...")
    return {
        "vehicles": pd.read_csv(os.path.join(input_path, "vehicles.csv")),
        "parking_zones": pd.read_csv(os.path.join(input_path, "parking_zones.csv")),
        "parking_events": pd.read_csv(os.path.join(input_path, "parking_events.csv"))
    }

def validate_events(df):
    logging.info("Validating parking_events data...")

    # Convert to datetime
    df["entry_time"] = pd.to_datetime(df["entry_time"], errors='coerce')
    df["exit_time"] = pd.to_datetime(df["exit_time"], errors='coerce')

    # Drop invalid timestamps
    df = df.dropna(subset=["entry_time", "exit_time"])

    # Apply validation rules
    valid_df = df[
        (df["exit_time"] >= df["entry_time"]) &
        (df["paid_amount"] >= 0) &
        (df["vehicle_id"].notnull()) &
        (df["zone_id"].notnull())
    ]

    logging.info(f"{len(valid_df)} valid parking_events records out of {len(df)}")
    return valid_df

def write_parquets(dfs, output_path):
    os.makedirs(output_path, exist_ok=True)

    # Write vehicles
    vehicles_path = os.path.join(output_path, "vehicles.parquet")
    dfs["vehicles"].to_parquet(vehicles_path, index=False)
    logging.info(f"Wrote vehicles.parquet to {vehicles_path}")

    # Write parking_zones
    zones_path = os.path.join(output_path, "parking_zones.parquet")
    dfs["parking_zones"].to_parquet(zones_path, index=False)
    logging.info(f"Wrote parking_zones.parquet to {zones_path}")

    # Write cleaned parking_events
    events_path = os.path.join(output_path, "parking_events.parquet")
    dfs["parking_events"].to_parquet(events_path, index=False)
    logging.info(f"Wrote cleaned parking_events.parquet to {events_path}")

def main():
    parser = argparse.ArgumentParser(description="ETL for Airport Parking Data")
    parser.add_argument("--input", required=True, help="Path to input CSVs")
    parser.add_argument("--output", required=True, help="Path to output folder")

    args = parser.parse_args()

    logging.info("Starting ETL job...")

    dfs = read_csvs(args.input)
    dfs["parking_events"] = validate_events(dfs["parking_events"])
    write_parquets(dfs, args.output)

    logging.info("ETL job completed.")

if __name__ == "__main__":
    main()
