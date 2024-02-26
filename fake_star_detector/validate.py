import pandas as pd
import datetime
import sys  # Import sys to read command line arguments

# Updated file path
users_data_file = "build/users_data.csv"


def _is_fake_star(row: pd.Series) -> bool:
    """Determines if a user's star might not be genuine based on specific criteria."""
    try:
        created_at = pd.to_datetime(row["created_at"]).date()
        updated_at = pd.to_datetime(row["updated_at"]).date()
        starred_at = pd.to_datetime(row["starred_at"]).date()
        dates_match = starred_at == updated_at == created_at
    except:
        dates_match = False

    no_profile = (
        pd.isnull(row["email"])
        and pd.isnull(row["bio"])
        and pd.isnull(row["blog"])
        and pd.isnull(row["hireable"])
        and pd.isnull(row["location"])
        and pd.isnull(row["profile_name"])
        and pd.isnull(row["plan"])
        and pd.isnull(row["twitter_username"])
    )

    no_activity = (
        (row["followers"] <= 1)
        and (row["following"] <= 10)
        and (row["public_gists"] <= 1)
        and (row["public_repos"] <= 10)
        and (row["private_gists"] <= 1)
        and (row["private_repos_owned"] <= 1)
    )

    return no_profile or no_activity or dates_match


def validate_users_for_repo(repo_name):
    try:
        users_df = pd.read_csv(users_data_file)
        users_df = users_df[users_df["repo_starred"] == repo_name]

        users_df["is_fake_star"] = users_df.apply(_is_fake_star, axis=1)
        display_results(users_df, repo_name)

    except FileNotFoundError as e:
        print(f"File not found: {e.filename}")


def display_results(users_df: pd.DataFrame, repo_name: str) -> None:
    """Displays the validation results for the specified repository."""
    total_users = len(users_df)
    total_is_fake = users_df["is_fake_star"].sum()
    percentage_fake = (total_is_fake / total_users) * 100 if total_users else 0

    print(f"\nStar validity check for repo `{repo_name}`:")
    print(f"Total users: {total_users}")
    print(f"Total fake users identified: {total_is_fake}")
    print(f"Percentage of fake users: {percentage_fake:.2f}%")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        # repo_name = "frasermarlow/tap-bls"  # TEST
        # validate_users_for_repo(repo_name)
        for repo in [
            # "anil-yelken/cyber-security",
            # "explodinggradients/ragas",   #3000
            # "framespot/client-py",
            "frasermarlow/tap-bls",
            "notifo-io/notifo",  # 700
            # "QuivrHQ/quivr",  #21000
            # "stackgpu/Simple-GPU",    # 435
            "venetisgr/space_titanic_basic",  # 118
        ]:
            validate_users_for_repo(repo)

        print("Usage: python validate.py <repo_name>")
    else:
        repo_name = sys.argv[1]
        validate_users_for_repo(repo_name)
