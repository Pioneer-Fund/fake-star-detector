import sys
import pandas as pd

from fake_star_detector.config import USERS_DATA_FILE_PATH


def delete_all_users_for_repo(repo_name: str, file_path: str = USERS_DATA_FILE_PATH):
    try:
        users_df = pd.read_csv(file_path)
        users_df = users_df[users_df["repo_starred"] != repo_name]
        users_df.to_csv(file_path, index=False)
    except FileNotFoundError as e:
        print(f"File not found: {e.filename}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python delete.py <repo_name>")
    else:
        repo_name = sys.argv[1]
        delete_all_users_for_repo(repo_name)
    print("Deletion complete.")
