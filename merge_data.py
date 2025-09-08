#!/usr/bin/env python3
"""
Data Merge Script
Merges historical data from data-backup/ with newer data from data/
Uses full history from data-backup and updates with newer dates from data
Writes merged result back to data/
"""

import os
import pandas as pd
from pathlib import Path
import logging
from datetime import datetime

# Setup logging
# 确保日志目录存在
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(
            log_dir / f'merge_log_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt'
        ),
        logging.StreamHandler(),
    ],
)


def merge_csv_files(backup_file, current_file, output_file):
    """
    Merge two CSV files: use full history from backup and update with newer data from current

    Args:
        backup_file: Path to backup CSV file (historical data)
        current_file: Path to current CSV file (newer data)
        output_file: Path to output merged CSV file
    """
    try:
        # Read backup data (historical)
        if os.path.exists(backup_file):
            df_backup = pd.read_csv(backup_file)
            df_backup["date"] = pd.to_datetime(df_backup["date"])
            logging.debug(
                f"Loaded backup data: {len(df_backup)} rows from {backup_file}"
            )
        else:
            df_backup = pd.DataFrame(
                columns=["date", "open", "close", "high", "low", "volume"]
            )
            logging.warning(f"Backup file not found: {backup_file}")

        # Read current data (newer)
        if os.path.exists(current_file):
            df_current = pd.read_csv(current_file)
            df_current["date"] = pd.to_datetime(df_current["date"])
            logging.debug(
                f"Loaded current data: {len(df_current)} rows from {current_file}"
            )
        else:
            df_current = pd.DataFrame(
                columns=["date", "open", "close", "high", "low", "volume"]
            )
            logging.warning(f"Current file not found: {current_file}")

        # If both files are empty, skip
        if len(df_backup) == 0 and len(df_current) == 0:
            logging.warning(f"Both files are empty for {os.path.basename(backup_file)}")
            return False

        # Combine dataframes
        if len(df_backup) == 0:
            df_merged = df_current.copy()
        elif len(df_current) == 0:
            df_merged = df_backup.copy()
        else:
            # Find the latest date in backup data
            latest_backup_date = df_backup["date"].max()

            # Filter current data to only include dates after the latest backup date
            df_current_new = df_current[df_current["date"] > latest_backup_date]

            # Combine backup data with new current data
            df_merged = pd.concat([df_backup, df_current_new], ignore_index=True)

            logging.info(
                f"Merged {len(df_backup)} backup rows + {len(df_current_new)} new rows = {len(df_merged)} total rows"
            )

        # Sort by date
        df_merged = df_merged.sort_values("date").reset_index(drop=True)

        # Remove duplicates (keep last occurrence)
        df_merged = df_merged.drop_duplicates(subset=["date"], keep="last")

        # Convert date back to string format
        df_merged["date"] = df_merged["date"].dt.strftime("%Y-%m-%d")

        # Save merged data
        df_merged.to_csv(output_file, index=False)
        logging.info(f"Saved merged data: {len(df_merged)} rows to {output_file}")

        return True

    except Exception as e:
        logging.error(f"Error merging {backup_file} and {current_file}: {str(e)}")
        return False


def main():
    """Main function to merge all CSV files"""
    backup_dir = Path("data-backup")
    current_dir = Path("data")
    output_dir = Path("data")

    # Ensure directories exist
    if not backup_dir.exists():
        logging.error(f"Backup directory not found: {backup_dir}")
        return

    if not current_dir.exists():
        logging.error(f"Current directory not found: {current_dir}")
        return

    # Get all CSV files from backup directory (excluding non-stock files)
    backup_files = list(backup_dir.glob("*.csv"))
    backup_files = [
        f
        for f in backup_files
        if not f.name.startswith("mktcap_") and f.name != "failed_stocks.txt"
    ]

    logging.info(f"Found {len(backup_files)} CSV files in backup directory")

    # Statistics
    processed = 0
    successful = 0
    failed = 0

    for backup_file in backup_files:
        filename = backup_file.name
        current_file = current_dir / filename
        output_file = output_dir / filename

        logging.info(f"Processing {filename}...")

        success = merge_csv_files(backup_file, current_file, output_file)

        processed += 1
        if success:
            successful += 1
        else:
            failed += 1

        # Progress update every 100 files
        if processed % 100 == 0:
            logging.info(f"Progress: {processed}/{len(backup_files)} files processed")

    # Final summary
    logging.info("=" * 50)
    logging.info("MERGE SUMMARY")
    logging.info("=" * 50)
    logging.info(f"Total files processed: {processed}")
    logging.info(f"Successful merges: {successful}")
    logging.info(f"Failed merges: {failed}")
    logging.info("=" * 50)

    if failed > 0:
        logging.warning(f"{failed} files failed to merge. Check the log for details.")
    else:
        logging.info("All files merged successfully!")


if __name__ == "__main__":
    main()
