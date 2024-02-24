import pandas as pd
import datetime


def _validate_star(row: pd.Series) -> int:
    if (
        (row["followers"] < 2)
        and (row["following"] < 2)
        and (row["public_gists"] == 0)
        and (row["public_repos"] < 5)
        and (row["created_at"] > datetime.date(2022, 1, 1))
        and (pd.isnull(row["email"]))
        and (pd.isnull(row["bio"]))
        and (not row["blog"])
        and (row["starred_at"] == row["updated_at"].date() == row["created_at"])
        and not isinstance(row["hireable"], bool)
    ):
        return 1
    else:
        return 0


df = pd.read_csv("stargazers.csv")

# numeric_cols = ["followers", "following", "public_gists", "public_repos"]
# df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")

# Applying the function to each row
df["is_fake"] = df.apply(_validate_star, axis=1)

print(
    df[
        [
            "followers",
            "following",
            "public_gists",
            "public_repos",
            "created_at",
            "starred_at",
            "updated_at",
            "is_fake",
        ]
    ]
)
