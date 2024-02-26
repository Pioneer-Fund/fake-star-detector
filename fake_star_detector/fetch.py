import sys
from github import Github
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

GITHUB_ACCESS_TOKEN = os.getenv("GITHUB_ACCESS_TOKEN")
g = Github(GITHUB_ACCESS_TOKEN)

users_data_file = "build/users_data.csv"
user_repo_mapping_file = "build/user_repo_mapping.csv"


def is_file_empty(file_path):
    """Check if file is empty by confirming if its size is 0 bytes"""
    return not os.path.exists(file_path) or os.stat(file_path).st_size == 0


def append_to_csv(df, file_path, include_header):
    """Append a DataFrame to CSV, controlling header inclusion"""
    df.to_csv(file_path, mode="a", index=False, header=include_header, encoding="utf-8")


def fetch_stargazers_and_update_files(repo_name):
    repo = g.get_repo(repo_name)
    stargazers = repo.get_stargazers_with_dates()

    # Check if files exist and/or are empty to decide on including headers
    users_file_needs_header = is_file_empty(users_data_file)
    mapping_file_needs_header = is_file_empty(user_repo_mapping_file)

    existing_usernames = set()
    if not users_file_needs_header:
        existing_users_df = pd.read_csv(users_data_file)
        existing_usernames = set(existing_users_df["username"])

    existing_mappings = set()
    if not mapping_file_needs_header:
        existing_mappings_df = pd.read_csv(user_repo_mapping_file)
        existing_mappings = set(
            zip(existing_mappings_df["username"], existing_mappings_df["repo_name"])
        )

    for stargazer in stargazers:
        user = stargazer.user
        mapping = (user.login, repo_name)

        if user.login not in existing_usernames:
            user_data = pd.DataFrame(
                [
                    {
                        "username": user.login,
                        "followers": user.followers,
                        "following": user.following,
                        "public_gists": user.public_gists,
                        "public_repos": user.public_repos,
                        "created_at": user.created_at.isoformat(),
                        "updated_at": user.updated_at.isoformat(),
                        "email": user.email,
                        "bio": user.bio,
                        "blog": user.blog,
                        "hireable": user.hireable,
                    }
                ]
            )
            append_to_csv(user_data, users_data_file, users_file_needs_header)
            existing_usernames.add(user.login)
            users_file_needs_header = False  # Only include header on first write

        if mapping not in existing_mappings:
            mapping_data = pd.DataFrame(
                [{"username": user.login, "repo_name": repo_name}]
            )
            append_to_csv(
                mapping_data, user_repo_mapping_file, mapping_file_needs_header
            )
            existing_mappings.add(mapping)
            mapping_file_needs_header = False  # Only include header on first write
            print(f"Added mapping: {user.login} -> {repo_name}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python fetch.py <repo_name>")
    else:
        repo_name = sys.argv[1]
        fetch_stargazers_and_update_files(repo_name)
    print("Fetching complete.")
#   "explodinggradients/ragas"
#   "QuivrHQ/quivr"
# 'atopile/atopile'
