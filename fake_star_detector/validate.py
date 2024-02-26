import pandas as pd
import datetime
import sys  # Import sys to read command line arguments

# Updated file paths
users_data_file = "build/users_data.csv"
user_repo_mapping_file = "build/user_repo_mapping.csv"


def _is_valid_star(row: pd.Series) -> bool:
    """Does not meet any condition that might represent a fake star"""
    try:
        created_at = pd.to_datetime(row["created_at"]).date()
        updated_at = pd.to_datetime(row["updated_at"]).date()
        starred_at = pd.to_datetime(row.get("starred_at", row["created_at"])).date()
        dates_match = starred_at == updated_at == created_at
    except:
        dates_match = False

    may_not_be_valid = (
        (row["followers"] <= 1)
        or (row["following"] <= 1)
        or (row["public_gists"] == 0)
        or (row["public_repos"] <= 4)
        or (created_at >= datetime.date(2022, 1, 1))
        or pd.isnull(row["email"])
        or pd.isnull(row["bio"])
        or pd.isnull(row["blog"])
        or pd.isnull(row["hireable"])
        or dates_match
    )

    if may_not_be_valid:
        pass

    return not may_not_be_valid


def _maybe_valid_star(row: pd.Series) -> bool:
    """Does not meet any condition that might represent a fake star"""
    try:
        created_at = pd.to_datetime(row["created_at"]).date()
        updated_at = pd.to_datetime(row["updated_at"]).date()
        starred_at = pd.to_datetime(row.get("starred_at", row["created_at"])).date()
        dates_match = starred_at == updated_at == created_at
    except:
        dates_match = False

    is_not_valid = (
        (row["followers"] <= 1)
        and (row["following"] <= 1)
        and (row["public_gists"] == 0)
        and (row["public_repos"] <= 4)
        and (created_at >= datetime.date(2022, 1, 1))
        and pd.isnull(row["email"])
        and pd.isnull(row["bio"])
        and pd.isnull(row["blog"])
        and pd.isnull(row["hireable"])
        and dates_match
    )

    return not is_not_valid


def _is_fake_star(row: pd.Series) -> bool:
    """Does not meet any condition that might represent a fake star"""
    try:
        created_at = pd.to_datetime(row["created_at"]).date()
        updated_at = pd.to_datetime(row["updated_at"]).date()
        dates_match = updated_at == created_at
    except:
        dates_match = False

    no_profile = (
        pd.isnull(row["email"])
        and pd.isnull(row["bio"])
        and pd.isnull(row["blog"])
        and pd.isnull(row["hireable"])
    )

    no_activity = (
        (row["followers"] <= 1)
        and (row["following"] <= 10)  # 13
        and (row["public_gists"] <= 1)
        and (row["public_repos"] <= 10)  # 10 # 0
    )

    if dates_match:
        pass

    if no_profile and no_activity:
        pass

    return no_profile and no_activity or dates_match


def validate_users_for_repo(repo_name):
    try:
        mappings_df = pd.read_csv(user_repo_mapping_file)
        users_of_repo = mappings_df[mappings_df["repo_name"] == repo_name]["username"]
        users_df = pd.read_csv(users_data_file)
        users_df = users_df[users_df["username"].isin(users_of_repo)]

        users_df["is_fake_star"] = users_df.apply(_is_fake_star, axis=1)
        display_results(users_df, repo_name)

    except FileNotFoundError as e:
        print(f"File not found: {e.filename}")


def display_results(users_df: pd.DataFrame, repo_name: str) -> None:
    total_users = len(users_df)
    total_is_fake = users_df["is_fake_star"].sum()
    percentage_fake = (total_is_fake / total_users) * 100 if total_users else 0

    print()
    print(f"Star validity check for repo `{repo_name}`:")
    print(f"Total users: {total_users}")
    print(f"Total fake users identified: {total_is_fake}")
    print(f"Percentage of fake users: {percentage_fake:.0f}%")
    # print(f"Total definitely valid users identified: {total_is_valid}")
    # print(f"Total maybe valid users identified: {total_maybe_valid}")
    # print(f"Percentage of maybe valid users: {percentage_maybe_valid:.0f}%")
    # print(f"Percentage of definitely valid users: {percentage_valid:.0f}%")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        # repo_name = "frasermarlow/tap-bls"  # TEST
        # validate_users_for_repo(repo_name)
        for repo in [
            # "anil-yelken/cyber-security",
            # "explodinggradients/ragas",
            # "framespot/client-py",
            "frasermarlow/tap-bls",
            # "notifo-io/notifo",
            # "QuivrHQ/quivr",
            # "stackgpu/Simple-GPU",
            # "venetisgr/space_titanic_basic",
        ]:
            validate_users_for_repo(repo)

        print("Usage: python validate.py <repo_name>")
    else:
        repo_name = sys.argv[1]
        validate_users_for_repo(repo_name)
