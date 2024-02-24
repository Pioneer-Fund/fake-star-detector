from github import Github
import pandas as pd
import datetime
from dotenv import load_dotenv
import os

load_dotenv()

GITHUB_ACCESS_TOKEN = os.getenv("GITHUB_ACCESS_TOKEN")
g = Github(GITHUB_ACCESS_TOKEN)


def fetch_stargazers(repo_name):
    repo = g.get_repo(repo_name)
    stargazers = repo.get_stargazers_with_dates()
    data = []

    for stargazer in stargazers:
        user = stargazer.user
        user_data = {
            "username": user.login,
            "starred_at": stargazer.starred_at,
            "followers": user.followers,
            "following": user.following,
            "public_repos": user.public_repos,
            # Add more fields as needed
        }
        data.append(user_data)
        print(f"Fetched: {user.login}")

    return pd.DataFrame(data)


# Example usage
repo_name = "posthog/posthog"  # Change this to the repository you want to analyze
stargazers_df = fetch_stargazers(repo_name)
print(stargazers_df.head())

# Save to CSV or further process as needed
stargazers_df.to_csv("stargazers.csv", index=False)
