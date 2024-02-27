import pytest

# from pathlib import Path
from fake_star_detector import delete
import csv

HEADERS = [
    "starred_at",
    "created_at",
    "updated_at",
    "username",
    "user_id",
    "bio",
    "blog",
    "email",
    "hireable",
    "profile_name",
    "twitter_username",
    "location",
    "repo_starred",
    "plan",
    "followers",
    "following",
    "public_gists",
    "private_gists",
    "public_repos",
    "private_repos_owned",
]


@pytest.fixture
def test_csv(tmp_path):
    test_file = tmp_path / "test_delete_all_users_for_repo.csv"
    # Use HEADERS for the CSV file
    with test_file.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(HEADERS)
        writer.writerow(
            [
                "2022-01-01T00:00:00",
                "2022-01-01T00:00:00",
                "2022-01-01T00:00:00",
                "username1",
                "1",
                "bio1",
                "blog1",
                "email1",
                True,
                "profile_name1",
                "twitter_username1",
                "location1",
                "repo_to_delete",
                "plan1",
                "1",
                "1",
                "1",
                "1",
                "1",
                "1",
            ]
        )
        writer.writerow(
            [
                "2022-01-01T00:00:00",
                "2022-01-01T00:00:00",
                "2022-01-01T00:00:00",
                "username2",
                "2",
                "bio2",
                "blog2",
                "email2",
                True,
                "profile_name2",
                "twitter_username2",
                "location2",
                "repo_to_keep",
                "plan2",
                "2",
                "2",
                "2",
                "2",
                "2",
                "2",
            ]
        )
    return test_file


def test_delete_all_users_for_repo(test_csv):
    """
    Test that delete_all_users_for_repo only removes the users for the specified repo.
    """
    with test_csv.open("r", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        assert len(rows) == 2

    delete.delete_all_users_for_repo("repo_to_delete", file_path=str(test_csv))

    with test_csv.open("r", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

        assert len(rows) == 1
        assert rows[0]["repo_starred"] != "repo_to_delete"
        assert rows[0]["repo_starred"] == "repo_to_keep"
