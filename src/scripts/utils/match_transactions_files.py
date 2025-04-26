import os
import re
from typing import Optional, List

def list_matching_transaction_files(base_path: str, date: Optional[str] = None, hour: Optional[str] = None) -> List[str]:
    """
    List transaction CSV files matching the pattern transactions_{date}_{hour}.csv
    from a given base directory.

    Args:
        base_path (str): Local directory path (e.g., "/app/shared_storage/data/").
        date (str, optional): Specific date in 'YYYY-MM-DD' format. If None, matches all dates.
        hour (str, optional): Specific hour in 'H' format (e.g., '8'). If None, matches all hours.

    Returns:
        List[str]: List of full file paths prefixed with 'file://'.
    """
    # List all files in the folder
    all_files = [f for f in os.listdir(base_path) if os.path.isfile(os.path.join(base_path, f))]
    
    if date and hour:
        # If both date and hour are given, adapt the pattern
        pattern = re.compile(rf"transactions_{re.escape(date)}_{re.escape(hour)}\.csv")
    elif date:
        # If only date is given, match all hours for that date
        pattern = re.compile(rf"transactions_{re.escape(date)}_\d+\.csv")
    else:
        # If no date is given, match all transactions files
        pattern = re.compile(r"transactions_\d{4}-\d{2}-\d{2}_\d+\.csv")
    
    # Filter files matching the desired pattern
    matching_files = [f for f in all_files if pattern.match(f)]
    
    # Build full 'file://' paths
    full_paths = [f"file://{os.path.join(base_path, f)}" for f in matching_files]
    
    return full_paths
    