import pandas as pd
import datetime
import sys  # Import sys to read command line arguments

# Updated file path
users_data_file = "build/users_data.csv"

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


def display_results(users_df: pd.DataFrame, repo_name: str) -> None:
    """Displays the validation results for the specified repository."""
    total_users = len(users_df)
    if total_users == 0:
        print(f"No users found for repo: `{repo_name}`")
        return

    total_dates_match = users_df["_is_dates_match"].sum()
    total_no_activity = users_df["_is_no_activity"].sum()
    total_no_bio = users_df["_is_no_bio"].sum()
    total_recently_created = users_df["_is_recently_created"].sum()

    # users with _is_dates_match and _is_no_activity and _is_no_bio
    total_no_activity_and_no_bio = users_df[
        (users_df["_is_no_activity"]) & (users_df["_is_no_bio"])
    ].shape[0]
    total_no_activity_and_no_bio_and_dates_match = users_df[
        (users_df["_is_dates_match"])
        & (users_df["_is_no_activity"])
        & (users_df["_is_no_bio"])
    ].shape[0]
    total_recently_created_and_no_activity = users_df[
        (users_df["_is_recently_created"]) & (users_df["_is_no_activity"])
    ].shape[0]
    # created recently and no bio
    total_recently_created_and_no_bio = users_df[
        (users_df["_is_recently_created"]) & (users_df["_is_no_bio"])
    ].shape[0]
    total_recently_created_and_no_activity_and_no_bio = users_df[
        (users_df["_is_recently_created"])
        & (users_df["_is_no_activity"])
        & (users_df["_is_no_bio"])
    ].shape[0]
    total_no_activity_and_dates_match = users_df[
        (users_df["_is_dates_match"]) & (users_df["_is_no_activity"])
    ].shape[0]
    total_no_bio_and_dates_match = users_df[
        (users_df["_is_dates_match"]) & (users_df["_is_no_bio"])
    ].shape[0]
    total_recently_created_and_no_activity_and_no_bio_and_dates_match = users_df[
        (users_df["_is_dates_match"])
        & (users_df["_is_no_activity"])
        & (users_df["_is_no_bio"])
        & (users_df["_is_recently_created"])
    ].shape[0]

    percentage_dates_match = (total_dates_match / total_users) * 100
    percentage_no_activity = (total_no_activity / total_users) * 100
    percentage_no_bio = (total_no_bio / total_users) * 100
    percentage_no_activity_and_no_bio = (
        total_no_activity_and_no_bio / total_users
    ) * 100
    percentage_all_checks = (
        total_no_activity_and_no_bio_and_dates_match / total_users
    ) * 100
    percentage_recently_created_and_no_activity = (
        total_recently_created_and_no_activity / total_users
    ) * 100
    percentage_recently_created_and_no_bio = (
        total_recently_created_and_no_bio / total_users
    ) * 100
    percentage_recently_created_and_no_activity_and_no_bio = (
        total_recently_created_and_no_activity_and_no_bio / total_users
    ) * 100
    percentage_no_activity_and_dates_match = (
        total_no_activity_and_dates_match / total_users
    ) * 100
    percentage_no_bio_and_dates_match = (
        total_no_bio_and_dates_match / total_users
    ) * 100
    percentage_recently_created_and_no_activity_and_no_bio_and_dates_match = (
        total_recently_created_and_no_activity_and_no_bio_and_dates_match / total_users
    ) * 100

    print(f"\nStar check for repo `{repo_name}`:")
    print(f"Total users: {total_users:,}")
    print(f"Matching dates: {total_dates_match} ({percentage_dates_match:.2f}%)")
    print(f"No activity: {total_no_activity} ({percentage_no_activity:.2f}%)")
    print(f"No bio: {total_no_bio} ({percentage_no_bio:.2f}%)")
    print(
        f"Created recently AND no activity: {total_recently_created_and_no_activity} ({percentage_recently_created_and_no_activity:.2f}%)"
    )
    print(
        f"Created recently AND no bio: {total_recently_created_and_no_bio} ({percentage_recently_created_and_no_bio:.2f}%)"
    )
    print(
        f"Created recently AND no activity AND no bio: {total_recently_created_and_no_activity_and_no_bio} ({percentage_recently_created_and_no_activity_and_no_bio:.2f}%)"
    )
    print(
        f"No activity AND no bio: {total_no_activity_and_no_bio} ({percentage_no_activity_and_no_bio:.2f}%)"
    )
    print(
        f"No activity AND dates match: {total_no_activity_and_dates_match} ({percentage_no_activity_and_dates_match:.2f}%)"
    )
    print(
        f"No bio AND dates match: {total_no_bio_and_dates_match} ({percentage_no_bio_and_dates_match:.2f}%)"
    )

    print(
        f"No activity AND no bio AND dates match: {total_no_activity_and_no_bio_and_dates_match} ({percentage_all_checks:.2f}%)"
    )
    print(
        f"Created recently AND no activity AND no bio AND dates match: {total_recently_created_and_no_activity_and_no_bio_and_dates_match} ({percentage_recently_created_and_no_activity_and_no_bio_and_dates_match:.2f}%)"
    )


if __name__ == "__main__":
    if len(sys.argv) != 2:
        # repo_name = "frasermarlow/tap-bls"  # TEST
        # validate_users_for_repo(repo_name)
        for repo in [
            # "anil-yelken/cyber-security",     #272
            "atopile/atopile",  # 1403
            "explodinggradients/ragas",  # 3165
            # "framespot/client-py",    # 201
            "frasermarlow/tap-bls",
            "notifo-io/notifo",  # 700
            "QuivrHQ/quivr",  # 28481
            # "stackgpu/Simple-GPU",    # 435
            "venetisgr/space_titanic_basic",  # 118
        ]:
            validate_users_for_repo(repo)

        print("Usage: python validate.py <repo_name>")
    else:
        repo_name = sys.argv[1]
        validate_users_for_repo(repo_name)
