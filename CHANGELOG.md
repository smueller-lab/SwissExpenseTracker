# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-07-06

### Added

- Mobile-responsive dashboard layout: sidebar collapses to a top bar, grid cards
  and filter/budget rows stack to a single column, and chart legends move below
  the plot below the 768px breakpoint.
- Budget and forecasting dashboard page with a basic forecasting model.
- Docker support for the app, including a dev container.
- End-to-end test pipeline coverage.
- GitHub issue templates (bug report, feature request) and a pull request template.
- Codecov integration for coverage reporting.
- Dev container bind mount for a host data folder, so `DATA_DIR` resolves inside the
  container without a separate Docker Compose run.
- Forecast chart wobble for lumpy categories, so the projection reads as plausible
  month-to-month variation instead of a dead-straight ramp.

### Changed

- Refreshed README screenshots and dev setup instructions.
- Lumpy-category detection and the median monthly rate now include the current year's
  completed months, not just prior years.
- Continuous categories flatten a one-off large transaction out of the pace calculation
  instead of letting it blow up the year-end forecast.
- Raised the forecast shrinkage constant so the year-end blend leans on the historical
  annual level for longer relative to the current-year pace.

### Fixed

- Forecast chart could show cumulative spend dipping downward at the actual/forecast
  seam; the forecast segment is now anchored to continue exactly from spend-to-date.

## [0.1.1] - 2026-06-20

### Fixed

- Missing user configuration causing startup failures.
- CI test run failures.

## [0.1.0] - 2026-06-19

### Added

- Initial dashboard with core pages (home, food, vacation).
- Agentic ingestion pipeline with post-pipeline cleaning.
- Migros Cumulus and Swissquote positions pipelines.
- Balance sheet and balance progression views.
- CI deployment workflow.
- Redesigned color contrast for accessibility.

[Unreleased]: https://github.com/smueller-lab/SwissExpenseTracker/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/smueller-lab/SwissExpenseTracker/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/smueller-lab/SwissExpenseTracker/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/smueller-lab/SwissExpenseTracker/releases/tag/v0.1.0
