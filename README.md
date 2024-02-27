# fake-star-detector

An attempt to indentify

> fake GitHub accounts created for the sole purpose of "starring" just one or two GitHub repos. They show activity on one day (the day the account was created, which matches the day the target repo was starred), and nothing else.

## Usage

Fetch

```sh
python fake_star_detector/fetch.py org/repo
```

Validate

```sh
python fake_star_detector/validate.py org/repo
```

Delete

```sh
python fake_star_detector/delete.py org/repo
```

Multiple repos stored in same local data file.

# Reference

Based on [dagster/fake-star-detector](https://dagster.io/blog/fake-stars), a simple Dagster project to analyze the number of fake GitHub stars on any GitHub repository. It is a companion to the blog post found [on the Dagster blog](https://dagster.io/blog/fake-stars).
Specifically, we used the simple model

- [Simpler model](#trying-the-simpler-model-using-data-from-the-github-api): A simple model running “low activity” heuristic. This simple heuristic can detect many (but hardly all) suspected fake accounts that starred the same set of repositories, using nothing but data from the GitHub REST API (via [pygithub](https://github.com/PyGithub/PyGithub)).

- [Complex detector](#running-the-complex-model-using-bigquery-archive-data): An alternative detection model which runs a sophisticated clustering algorithm as well as the heuristic, using the public [GH Archive](https://www.gharchive.org) available in Bigquery. This model is written in SQL and uses [dbt](https://github.com/dbt-labs/dbt-core) alongside Dagster.
  - _Note: You can run this within the limits of a free-tier BQ account, but the analysis will be reduced in scope. By default, this model only scans data in 2023 on a small repository, in order to make it stay within the free-tier quota._
