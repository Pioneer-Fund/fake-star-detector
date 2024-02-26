# Sample data with corrected date and datetime types
data = {
    "followers": [1, 3],
    "following": [1, 4],
    "public_gists": [0, 1],
    "public_repos": [4, 10],
    "created_at": [
        datetime.date(2023, 1, 2),
        datetime.date(2021, 12, 31),
    ],  # datetime.date object
    "email": [None, "example@example.com"],
    "bio": [None, "A bio"],
    "blog": ["", "http://example.com"],
    "starred_at": [
        datetime.date(2023, 1, 2),
        datetime.date(2022, 1, 2),
    ],  # datetime.date object
    "updated_at": [
        datetime.datetime(2023, 1, 2, 15, 0),
        datetime.datetime(2022, 1, 2, 15, 0),
    ],  # datetime.datetime object
    "hireable": [None, True],
}
