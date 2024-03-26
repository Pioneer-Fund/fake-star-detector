import sys
from github import Github, GithubException
import pandas as pd
import os
from dotenv import load_dotenv

from fake_star_detector.config import USERS_DATA_FILE_PATH

load_dotenv()

GITHUB_ACCESS_TOKEN = os.getenv("GITHUB_ACCESS_TOKEN")
g = Github(GITHUB_ACCESS_TOKEN)


def is_file_empty(file_path):
    """Check if file is empty by confirming if its size is 0 bytes"""
    return not os.path.exists(file_path) or os.stat(file_path).st_size == 0


def append_to_csv(df, file_path, include_header):
    """Append a DataFrame to CSV, controlling header inclusion"""
    df.to_csv(file_path, mode="a", index=False, header=include_header, encoding="utf-8")


def fetch_stargazers_and_update_files(repo_name, users_data_file=USERS_DATA_FILE_PATH):
    """Writes header on file creation only."""
    try:
        repo = g.get_repo(repo_name)
        stargazers = repo.get_stargazers_with_dates()
        total_stars = repo.stargazers_count
        print(f"Found {total_stars} stargazers for {repo_name}...")
    except GithubException as e:
        print(f"Error fetching repository: {e}")
        return

    users_file_needs_header = is_file_empty(users_data_file)

    # Use a set of tuples to track existing username-repo mappings
    existing_mappings = set()
    if not users_file_needs_header:
        existing_users_df = pd.read_csv(users_data_file)
        existing_mappings = set(
            zip(existing_users_df["username"], existing_users_df["repo_starred"])
        )

    stargazers = repo.get_stargazers_with_dates()
    for i, stargazer in enumerate(stargazers, 1):  # 1-indexed
        user = stargazer.user
        mapping = (user.login, repo_name)

        if user.login not in existing_mappings:
            user_data = pd.DataFrame(
                [
                    {
                        "starred_at": stargazer.starred_at.isoformat(),
                        "created_at": user.created_at.isoformat(),
                        "updated_at": user.updated_at.isoformat(),
                        "username": user.login,
                        "user_id": user.id,  # int
                        "bio": user.bio,
                        "blog": user.blog,  # none is '' empty string
                        "email": user.email,
                        "hireable": user.hireable,  # bool
                        "profile_name": user.name,
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
            existing_mappings.add(mapping)
            users_file_needs_header = False  # Only include header on first write

        print(f"Processing {i}/{total_stars}: {user.login} starred {repo_name}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python fetch.py <repo_name>")
    else:
        repo_name = sys.argv[1]
        fetch_stargazers_and_update_files(repo_name)
    print("Fetching complete.")
