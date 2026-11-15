# Security Policy

UMDB is a static site with a Python harvester. It handles no user accounts or personal data, but it uses API keys (NCBI and OpenAI) and has workflows that can push to `main`.

## Reporting a vulnerability

Report it privately through GitHub: the repository's **Security** tab → **Report a vulnerability**. Please do not open a public issue for security problems.

Include what is affected, how to reproduce it, and the impact. You should get an acknowledgement within a week.

In scope:
- Leaked credentials in the repository, its history or workflow logs
- Workflow injection (e.g. untrusted `workflow_dispatch` inputs reaching shell commands)
- Script injection or XSS in the website from harvested metadata (record titles and attributes are untrusted text submitted to NCBI)
- Generated download scripts (`download.sh`) that could run unintended commands

## Handling secrets

- Keys live in GitHub Actions secrets (CI) or a local `.env` (git-ignored). Never in code, issues, pull requests or commit messages.
- If a key is exposed, **revoke it first** (NCBI account settings, OpenAI dashboard), then replace the secret. Deleting the commit is not enough: forks and caches keep it.
- The OpenAI key can spend money. Use a project-scoped key with a usage limit.

## Supported versions

Only the current `main` branch and the live site are supported.
