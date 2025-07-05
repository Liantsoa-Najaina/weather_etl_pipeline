import os
import logging
import pandas as pd

def merge_daily_to_historical() -> bool:
    try:
        historical_path = "/home/najaina/PycharmProjects/historical_weather_data/data/processed/global_history_weather.csv"
        daily_path = "/home/najaina/data/processed/weather_global.csv"
        output_path = "./data/processed/weather_tourism_dataset.csv"

        if not os.path.exists(historical_path) or not os.path.exists(daily_path):
            logging.warning("One or both input files are missing.")
            return False

        historical_df = pd.read_csv(historical_path)
        daily_df = pd.read_csv(daily_path)

        # Drop incompatible column
        daily_df.drop(columns=["clouds"], errors="ignore", inplace=True)

        # Merge
        merged_df = pd.concat([historical_df, daily_df], ignore_index=True)

        # Deduplication and sorting
        merged_df.drop_duplicates(subset=["city", "date"], keep="last", inplace=True)
        merged_df.sort_values(by=["city", "date"], inplace=True)
        merged_df.reset_index(drop=True, inplace=True)

        # Save result
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        merged_df.to_csv(output_path, index=False)

        logging.info(f"Merged dataset saved to {output_path} with {len(merged_df)} records.")
        return True

    except Exception as e:
        logging.error(f"Failed to merge daily and historical data: {e}")
        return False