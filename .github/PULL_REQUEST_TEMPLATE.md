## What and why

<!-- One or two sentences. Link the issue if there is one. -->

## Type

- [ ] Pipeline / code
- [ ] Annotation rules (geo, assay, discovery queries, AI curation)
- [ ] Website
- [ ] Documentation
- [ ] Data refresh (generated files only)

## Checklist

- [ ] `python3 -m unittest discover tests` passes
- [ ] Tests added or updated for changed parsing, classification or export logic
- [ ] Code and regenerated data are in separate commits
- [ ] `QUERY_PROFILES_VERSION` bumped if `QUERY_PROFILES` changed
- [ ] `CHANGELOG.md` updated under `Unreleased` for user-visible changes
- [ ] No secrets, `.env` or `data/debug/` files committed

## Effect on the catalog (annotation-rule changes only)

<!-- Before/after counts across the local catalog, e.g. runs per assay class or runs with a city. See CONTRIBUTING.md. -->

| Metric | Before | After |
| --- | --- | --- |
|  |  |  |
