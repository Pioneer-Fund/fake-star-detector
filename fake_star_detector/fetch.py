from github import Github
import pandas as pd
import csv
from dotenv import load_dotenv
import os

load_dotenv()

GITHUB_ACCESS_TOKEN = os.getenv("GITHUB_ACCESS_TOKEN")
g = Github(GITHUB_ACCESS_TOKEN)

users_data_file = "build/users_data.csv"
user_repo_mapping_file = "build/user_repo_mapping.csv"


def fetch_stargazers_and_update_files(repo_name):
    repo = g.get_repo(repo_name)
    stargazers = repo.get_stargazers_with_dates()

    try:
        existing_users_df = pd.read_csv(users_data_file)
        existing_usernames = set(existing_users_df["username"])
    except FileNotFoundError:
        existing_usernames = set()
        pd.DataFrame(
            columns=[
                "username",
                "followers",
                "following",
                "public_gists",
                "public_repos",
                "created_at",
                "updated_at",
                "email",
                "bio",
                "blog",
                "hireable",
            ]
        ).to_csv(
            users_data_file,
            index=False,
            quoting=csv.QUOTE_ALL,
            escapechar="\\",
            encoding="utf-8",
        )

    try:
        user_repo_mapping_df = pd.read_csv(user_repo_mapping_file)
        existing_mappings = set(
            zip(user_repo_mapping_df["username"], user_repo_mapping_df["repo_name"])
        )
        # Count the initial number of mappings for the specific repo
        initial_repo_mappings_count = sum(
            user_repo_mapping_df["repo_name"] == repo_name
        )
    except FileNotFoundError:
        user_repo_mapping_df = pd.DataFrame(columns=["username", "repo_name"])
        existing_mappings = set()
        initial_repo_mappings_count = 0

    mapping_count = initial_repo_mappings_count  # Start from the existing number of mappings for the repo

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
                        "created_at": user.created_at,
                        "updated_at": user.updated_at,
                        "email": user.email,
                        "bio": user.bio,
                        "blog": user.blog,
                        "hireable": user.hireable,
                    }
                ]
            )
            user_data.to_csv(
                users_data_file,
                mode="a",
                header=False,
                index=False,
                quoting=csv.QUOTE_ALL,
                escapechar="\\",
                encoding="utf-8",
            )
            existing_usernames.add(user.login)

        if mapping not in existing_mappings:
            new_mapping = pd.DataFrame(
                [{"username": user.login, "repo_name": repo_name}]
            )
            new_mapping.to_csv(
                user_repo_mapping_file,
                mode="a",
                header=False,
                index=False,
                quoting=csv.QUOTE_ALL,
                escapechar="\\",
                encoding="utf-8",
            )
            existing_mappings.add(mapping)
            mapping_count += 1  # Increment based on total mappings to the repo
            print(f"{mapping_count} Added mapping: {user.login} -> {repo_name}")
        else:
            print(f"Mapping already exists: {user.login} -> {repo_name}")

    print(
        f"Updated mapping for {repo_name} with total mappings now at {mapping_count}."
    )


# Example usage
repos = ["explodinggradients/ragas"]
for repo_name in repos:
    fetch_stargazers_and_update_files(repo_name)

# Example usage
#   "explodinggradients/ragas"
#   "QuivrHQ/quivr"
