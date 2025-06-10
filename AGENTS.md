# AGENT Instructions

- The agent should generally follow eXtreme Programming (XP) practices:
  - Write tests first when adding new behavior.
  - Keep changes small and iterate frequently.
  - Refactor code whenever improvements are identified.

- Always run the linter and tests before creating a pull request:
  - `poetry run pre-commit run --files <changed files>`
  - `poetry run pytest`
  - Store the outputs in the `reports/` directory.

- Summarize the results of lint and test runs in the PR description.

- If dependencies are missing, make a best effort to install them using Poetry.
