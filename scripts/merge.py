import logging
import os
import pandas as pd


def merge_files(date: str) -> str:
    """
    Merge all files in a given date into a single xlsx file
    :param date: The date to merge files from
    :return: The merged xlsx file path
    """
    try:
        # f"data/raw/{date}/weather_{city}.csv"
        input_dir = f"data/raw/{date}/"
        output_file = f"data/processed/weather_global.csv"
        # Create processed folder if it hasn't been yet
        os.makedirs(os.path.dirname(output_file), exist_ok=True)

        # Load existing data
        if os.path.exists(output_file):
            global_df = pd.read_csv(output_file)
        else:
            global_df = pd.DataFrame()

        # Load new csv files
        new_data = []
        for file in os.listdir(input_dir):
            if file.startswith("weather_") and file.endswith(".csv") or file.startswith("meteo_") and file.endswith(".csv"):
                new_data.append(pd.read_csv(os.path.join(input_dir, file)))
        if not new_data:
            raise ValueError(f"No new data for this date: {date}")

        # Appending new data and remove duplicates if found
        updated_df = pd.concat([global_df] + new_data, ignore_index=True)
        updated_df = updated_df.drop_duplicates(
            subset=["city", "date"],
            keep="last",
        )

        updated_df.to_csv(output_file, index=False)
        updated_df.to_excel(f"data/processed/weather_global.xlsx", index=False, engine="openpyxl")
        print("Output file path : ", output_file)
        return output_file
    except ValueError as e:
        logging.error(f"Merge task failed: {e}")
    except Exception as e:
        logging.error(f"Merge task failed: {e}")
    return "Merge task failed"
