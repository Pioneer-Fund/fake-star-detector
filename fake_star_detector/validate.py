import pandas as pd
import datetime
import sys  # Import sys to read command line arguments

# Updated file path
users_data_file = "build/users_data.csv"

repos_with_fake_stars = [
    "anil-yelken/cyber-security",  # 272
    "framespot/client-py",  # 201
    "frasermarlow/tap-bls",
    "notifo-io/notifo",  # 700
    "stackgpu/Simple-GPU",  # 435
    "venetisgr/space_titanic_basic",  # 118
]
batch_repos = [
    "atopile/atopile",  # 1403
    "danswer-ai/danswer",
    "explodinggradients/ragas",  # 3165
    "QuivrHQ/quivr",  # 28481
]
other_repos = ["posthog/posthog"]  # 15880

REPO_CREATED_AFTER_DATE = datetime.date(2023, 1, 1)


def _is_dates_match(row: pd.Series) -> bool:
    try:
        created_at = pd.to_datetime(row["created_at"]).date()
        updated_at = pd.to_datetime(row["updated_at"]).date()
        starred_at = pd.to_datetime(row["starred_at"]).date()
        dates_match = starred_at == updated_at == created_at
    except:
        dates_match = False

    return dates_match


def _is_no_activity(row: pd.Series) -> bool:
    """
    Criteria based on:
    https://dagster.io/blog/fake-stars
        Followers <=1
        Following <= 1
        Public gists == 0
        Public repos <=4
    """
    return (
        # (row["private_gists"] == 0)
        # (row["private_repos_owned"] == 0)
        # https://dagster.io/blog/fake-stars
        (row["followers"] <= 1)
        and (row["following"] <= 1)
        and (row["public_gists"] == 0)
        and (row["public_repos"] <= 4)
    )


def _is_no_bio(row: pd.Series) -> bool:
    """
    Dagster: "Email, hireable, bio, blog, and twitter username are empty"
    """
    return (
        pd.isnull(row["email"])
        and pd.isnull(row["bio"])
        and pd.isnull(row["blog"])
        and pd.isnull(row["hireable"])
        and pd.isnull(row["location"])
        and pd.isnull(row["profile_name"])
        and pd.isnull(row["plan"])
        and pd.isnull(row["twitter_username"])
    )


def _is_recently_created(row: pd.Series) -> bool:
    """
    dagster: Created in 2022 or later

    todo: use repo creation date?

    """
    try:
        created_at = pd.to_datetime(row["created_at"]).date()
    except:
        return False
    return created_at >= REPO_CREATED_AFTER_DATE


def validate_users_for_repo(repo_name: str):
    """Looks for potentially not real (i.e. purchased) stars
    based on specific criteria.

    Leans towards conservatism. Use specific, discrete checks.

    Profile created, starred, and last date updated all match
    (and created recently?)

    No bio and no activity?

    """
    try:
        users_df = pd.read_csv(users_data_file)
        users_df = users_df[users_df["repo_starred"] == repo_name]

        users_df["_is_dates_match"] = users_df.apply(_is_dates_match, axis=1)
        users_df["_is_no_activity"] = users_df.apply(_is_no_activity, axis=1)
        users_df["_is_no_bio"] = users_df.apply(_is_no_bio, axis=1)
        users_df["_is_recently_created"] = users_df.apply(_is_recently_created, axis=1)
        display_results(users_df, repo_name)

    except FileNotFoundError as e:
        print(f"File not found: {e.filename}")


def display_definitions():
    print()
    print("Definitions:")
    print(" - Dates Match: created_at == starred_at == last_modified_at")
    print(f" - Created recently: created on or after {REPO_CREATED_AFTER_DATE}")
    print(
        f" - No activity: followers <= 1, following <= 1, public gists == 0, public repos <= 4"
    )
    print(f" - No bio: email, hireable, bio, blog, and twitter username are empty")


def display_footer():
    print()
    print(f"**Based on: dagster.io/blog/fake-stars**")


def display_results(users_df: pd.DataFrame, repo_name: str) -> None:
    """Displays the validation results for the specified repository."""
    total_users = len(users_df)
    if total_users == 0:
        print(f"No users found for repo: `{repo_name}`")
        return

    total_dates_match = users_df["_is_dates_match"].sum()
    total_recently_created_and_no_activity_and_no_bio = users_df[
        (users_df["_is_recently_created"])
        & (users_df["_is_no_activity"])
        & (users_df["_is_no_bio"])
    ].shape[0]
    total_recently_created_and_no_activity_and_no_bio_and_dates_match = users_df[
        (users_df["_is_dates_match"])
        & (users_df["_is_no_activity"])
        & (users_df["_is_no_bio"])
        & (users_df["_is_recently_created"])
    ].shape[0]

    percentage_dates_match = (total_dates_match / total_users) * 100
    percentage_recently_created_and_no_activity_and_no_bio = (
        total_recently_created_and_no_activity_and_no_bio / total_users
    ) * 100
    percentage_recently_created_and_no_activity_and_no_bio_and_dates_match = (
        total_recently_created_and_no_activity_and_no_bio_and_dates_match / total_users
    ) * 100

    print(f"\nCheck users who starred `{repo_name}`:")
    print(f" - Total users: {total_users:,}")
    print(
        f" - Created recently AND no activity AND no bio: {total_recently_created_and_no_activity_and_no_bio} ({percentage_recently_created_and_no_activity_and_no_bio:.2f}%)"
    )
    print(
        f" - Dates match (created_at == starred_at == last_modified_at): {total_dates_match} ({percentage_dates_match:.2f}%)"
    )
    print(
        f" - Created recently AND no activity AND no bio AND dates match: {total_recently_created_and_no_activity_and_no_bio_and_dates_match} ({percentage_recently_created_and_no_activity_and_no_bio_and_dates_match:.2f}%)"
    )


def main():
    for repo in [batch_repos, repos_with_fake_stars, other_repos]:
        validate_users_for_repo(repo)
    display_definitions()
    display_footer()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        main()
    else:
        repo_name = sys.argv[1]
        validate_users_for_repo(repo_name)
