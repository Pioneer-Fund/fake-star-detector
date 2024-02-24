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
            "public_gists": user.public_gists,  # Include public_gists
            "public_repos": user.public_repos,
            "created_at": user.created_at,  # Ensure you're fetching this if needed
            "updated_at": user.updated_at,  # Ensure you're fetching this if needed
            "email": user.email,  # Include email
            "bio": user.bio,  # Include bio
            "blog": user.blog,  # Include blog
            "hireable": user.hireable,  # Include hireable status
        }
        # {'username': 'bevenky', 'starred_at': datetime.datetime(2020, 2, 20, 22, 49, 36, tzinfo=datetime.timezone.utc), 'followers': 94, 'following': 48, 'public_repos': 27}
        data.append(user_data)
        counter += 1
        print(f"Fetched: {user.login}")

    return pd.DataFrame(data)


# Example usage
repo_name = "posthog/posthog"  # Change this to the repository you want to analyze
stargazers_df = fetch_stargazers(repo_name)
print(stargazers_df.head())

# Save to CSV or further process as needed
stargazers_df.to_csv("stargazers.csv", index=False)
