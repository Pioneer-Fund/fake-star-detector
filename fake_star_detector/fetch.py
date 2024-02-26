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
    """Writes header on file creation only."""
    repo = g.get_repo(repo_name)
    stargazers = repo.get_stargazers_with_dates()

    users_file_needs_header = is_file_empty(users_data_file)
    mapping_file_needs_header = is_file_empty(user_repo_mapping_file)

    existing_usernames = set()
    if not users_file_needs_header:
        existing_users_df = pd.read_csv(users_data_file)
        existing_usernames = set(existing_users_df["username"])

    mapping_count = 0
    existing_mappings = set()
    if not mapping_file_needs_header:
        existing_mappings_df = pd.read_csv(user_repo_mapping_file)
        existing_mappings = set(
            zip(existing_mappings_df["username"], existing_mappings_df["repo_name"])
        )
        mapping_count = len(
            existing_mappings_df[existing_mappings_df["repo_name"] == repo_name]
        )

    for stargazer in stargazers:
        user = stargazer.user
        mapping = (user.login, repo_name)

        if user.login not in existing_usernames:
            user_data = pd.DataFrame(
                [
                    {
                        "starred_at": stargazer.starred_at.isoformat(),
                        "created_at": user.created_at.isoformat(),
                        "updated_at": user.updated_at.isoformat(),
                        "last_modified": user.last_modified_datetime.isoformat(),
                        "username": user.login,
                        "user_id": user.id,  # int
                        "bio": user.bio,
                        "blog": user.blog,  # none is '' empty string
                        "email": user.email,
                        "hireable": user.hireable,  # bii==ool
                        "name": user.name,
                        "twitter_username": user.twitter_username,
                        "location": user.location,
                        "repo_starred": repo_name,
                        "plan": user.plan,
                        "followers": user.followers,  # int
                        "following": user.following,  # int
                        "public_gists": user.public_gists,  # int
                        "private_gists": user.private_gists,  # int
                        "public_repos": user.public_repos,
                        "private_repos_owned": user.owned_private_repos,
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

            mapping_count += 1
            print(f"Total mappings for {repo_name}: {mapping_count}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python fetch.py <repo_name>")
    else:
        repo_name = sys.argv[1]
        fetch_stargazers_and_update_files(repo_name)
    print("Fetching complete.")
