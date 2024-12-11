import os
import shutil
from config import *

# List of folders you want to clean up
directories_to_clean   = [
    AVAN_CHECKPOINT_FILE,
    ROLLING_COINT_STOCK_CHECKPOINT_FILE,
    ROLLING_COINT_COIN_CHECKPOINT_FILE,
    CHECKPOINT_JSON_PATH + '/avan_checkpoint.json',
    CHECKPOINT_JSON_PATH + '/coin_calc_pipeline.json',
    CHECKPOINT_JSON_PATH + '/calc_pipeline.json'
    ]

def clean_directory_contents(directory_path):
    if not os.path.exists(directory_path):
        print(f"Directory {directory_path} does not exist.")
        return
    
    for item_name in os.listdir(directory_path):
        item_path = os.path.join(directory_path, item_name)
        try:
            if os.path.isfile(item_path) or os.path.islink(item_path):
                os.unlink(item_path)  # Remove file or symbolic link
                print(f"Deleted file: {item_path}")
            elif os.path.isdir(item_path):
                shutil.rmtree(item_path)  # Remove directory and all its contents
                print(f"Deleted directory: {item_path}")
        except Exception as e:
            print(f"Failed to delete {item_path}. Reason: {e}")

def clean_directories(directories):
    for directory in directories:
        clean_directory_contents(directory)
        print(f"Finished cleaning up contents of {directory}")

if __name__ == "__main__":
    clean_directories(directories_to_clean)
