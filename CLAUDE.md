# CLAUDE.md

Guidance for Claude Code working in this repository.

> **Sandboxing**: when launched via the `claude` alias from
> [wmf-claude](https://gitlab.wikimedia.org/repos/product-safety-and-integrity/wmf-claude),
> Claude Code runs inside a [nono](https://github.com/always-further/nono) sandbox.
> A denied path or domain is nono enforcing the wmf-engineer profile, not a bug to
> work around.

## What this is

Wikilink (repo `externallinks`) tracks link additions to Wikimedia projects made
through Wikimedia partnerships, primarily The Wikipedia Library. It is a Django
app **hosted on GitHub** (https://github.com/WikipediaLibrary/externallinks),
**not Gerrit**, deployed on a Cloud VPS instance and served at wikilink.wmflabs.org.
Issues: the [Wikilink-Tool Phabricator tag](https://phabricator.wikimedia.org/tag/wikilink-tool/).

Layout:

- `./extlinks/` is the Django project. Apps: `links` (LinkEvent, URLPattern,
  LinkSearchTotal), `organisations` (Organisation, Collection, User), `programs`
  (Program), `aggregates` (rollups), `healthcheck`, `common`, plus `templates/`
  and `logs/`. Settings split under `extlinks/settings/`: `base`, `local` (dev
  default), `production`, `helpers`, `logging`.
- `./requirements/`: `django.txt` (production) and `local.txt` (dev overlay:
  debug toolbar, pudb). No separate test file.
- `./Dockerfile`: multi-stage, targets `eventstream` (base: Python 3.11 + deps),
  `externallinks` (adds gunicorn), `cron`. No virtualenv.
- `./docker-compose.yml`: single file, no overlays yet. Services `externallinks`
  (gunicorn), `eventstream` (stream consumer), `crons`, `db` (MariaDB 10),
  `cache` (memcached), `nginx`.
- `./bin/`: operational scripts (`gunicorn.sh`, `cron.sh`, `django_wait_for_db.sh`,
  `restore.sh`, `swarm_update.sh`, `example_data.sh`, ...). No venv wrappers.
- `./backup.py` + `./backup/`: gzipped SQL-dump backups, 14-day retention,
  filelock-guarded. `./crontab` schedules them alongside the aggregate fills,
  `linksearchtotal_collect`, `linkevents_archive`, and hourly `users_update_lists`.
- `./db.cnf`, `./nginx.conf`, `./template.env`, `./django_wait_for_migrations.py`
  (30s migration wait, wraps the CI test run), `./wiki-list.csv`, `./static/`.

## Domain and data flow

**Program** groups **Organisation**s; each organisation owns **Collection**s;
each collection tracks one or more **URLPattern**s. Two independent sources feed
the stats:

- **Link events** from the Wikimedia page-links-change EventStream, consumed by
  `extlinks/links/management/commands/linkevents_collect.py` (SSE; `--historical`
  resumes from the last stored event; 5-minute retry on drops). Matched adds and
  removes become `LinkEvent` rows. `users_update_lists` refreshes each org's
  authorized-user list hourly (from the Library Card Platform's AuthorizedUsers
  view) so events can be flagged `on_user_list`.
- **Total link counts** from the MediaWiki externallinks table, pulled per URL
  pattern per day into `LinkSearchTotal` by `linksearchtotal_collect`.

`aggregates/` precomputes the rollups the org/collection/program pages render;
`healthcheck/` exposes per-cron health endpoints so a stalled collector or
aggregate job is visible. Fuller writeups: the wiki's
[App structure](https://github.com/WikipediaLibrary/externallinks/wiki/App-structure)
and [Data tracking](https://github.com/WikipediaLibrary/externallinks/wiki/Data-tracking)
pages.

## Deployment, and the migration in flight

Production today runs on **Docker Swarm** (`bin/swarm_update.sh` does
`docker stack deploy`; docker-compose is pinned to 1.25.5). The current build is
documented on the wiki's
[Debian Server Setup](https://github.com/WikipediaLibrary/externallinks/wiki/Debian-Server-Setup)
page.

That VM is a deprecated Bullseye instance being replaced under **T402055**
(deadline 2026-07-31), and the replacement moves deployment to the
[CloudVPS Compose Deploy toolkit](https://gitlab.wikimedia.org/repos/modtools/cloudvps-compose-deploy)
(rootless docker compose), the same toolkit TWLight already runs. Its
contract sets the target layout:

- One VM runs one environment, and that environment is a git branch (`staging` or
  `production`) named by the VM's `env` instance metadata.
- `COMPOSE_FILE` pins `docker-compose.yml` (base) plus `docker-compose.deploy.yml`
  (deploy overlay); `docker-compose.override.yml` is left free as the local-dev
  overlay so a bare `docker compose up` stays a dev command.
- `template.env` is copied to `.env` on first setup; scheduled jobs live in
  `conf/crontab`.

**Guidance:** favor the migration direction. This repo is already clean (no
virtualenv-in-container, one compose file, a modest `bin/`); keep it that way, and
don't add swarm-specific coupling, new overlays, or wrapper-script sprawl. Both
the swarm path and the new compose path must work during the rollout, so prefer
additive changes (a new `docker-compose.deploy.yml` alongside the existing file)
over in-place rewrites of anything the live swarm deployment still depends on.

## Conventions

- **Python 3.11, Django 4.2, MariaDB 10** (mysqlclient), memcached (pymemcache),
  gunicorn (7 gthread workers, see `bin/gunicorn.sh`). Event ingestion via
  sseclient; retries via tenacity. Server-rendered templates, no node toolchain.
- **PEP 8 / Django style**, four-space indent. No formatter or linter is enforced
  (no black, flake8, pyproject.toml, setup.cfg, .flake8, or pre-commit config).
  Match surrounding code; the dominant local pattern wins over a strict reading of
  any one style guide. If you run black, run it only on files you already touched.
- Imports: stdlib, third-party, first-party (`extlinks.*`), local.
- **Keep the diff small.** Reuse existing helpers and management commands rather
  than inventing parallel ones. Split needed refactors into their own commit ahead
  of the feature commit.
- This runs on Cloud VPS, not core production, so absolute scale differs from
  MediaWiki proper, but the principles hold: no synchronous HTTP in the request
  path, batch DB reads, defer slow work to cron. The stream consumer and the
  aggregate fills are the hot spots.
- Logs go to container stdout; `docker compose logs externallinks` (or the swarm
  equivalent for now) is the first place to look. Slow queries show up in the `db`
  container logs.

## Testing

Tests are per-app `tests.py` files (no `tests/` dir). Run inside the container:

```bash
docker exec -ti externallinks-externallinks-1 python manage.py test
```

CI runs `docker compose exec -T externallinks /app/bin/django_wait_for_db.sh
python django_wait_for_migrations.py test`. Coverage config is `.coveragerc`
(dynamic `test_function` context, migrations omitted); the README covers the
`htmlcov/` report.

## CI

`.github/workflows/dockerpublish.yml` builds the images and runs the suite on
every PR and push. On `master` / `staging`, after tests pass, it pushes
`externallinks`, `eventstream`, and `externallinks_cron` to
`quay.io/wikipedialibrary/`, tagged by branch and commit sha. Dependencies are
tracked by Dependabot (`.github/dependabot.yml`).

## Commit messages

Wikimedia format, GitHub variant: no `Change-Id:` trailer (that is Gerrit-only).
`/wmf-claude:write-commit-msg` drafts one. Subject `component: Subject`, body wraps
at 72; trailers in order `Assisted-by:` (kernel style, model name only) then
`Bug: TXXXXX` (Phabricator, still applies here). No `Co-Authored-By:`.

## wmf-claude skills and tooling

`/wmf-claude:write-commit-msg`, `write-phab-task`, `vuln-audit`, and `codesearch`
apply here. The MediaWiki-specific skills (`run-tests`, `test-coverage`, `lint`,
`manual-test`, `compare-rebase`, `perf-audit`) and the Gerrit-only
`review-patch` / `gerrit-reviewer` do NOT: this is a GitHub-hosted Django app, so
use normal PR review flows.

- **Code search**: [codesearch.wmcloud.org](https://codesearch.wmcloud.org/search/)
  spans the WMF ecosystem; the Hound backend API is
  `https://codesearch-backend.wmcloud.org/search/api/v1/search?q={query}&repos=*`.
- **Web fetch**: prefer `curl` (piped to `jq`) over `WebFetch` for Wikimedia sites
  (mediawiki.org, wikitech, phabricator, gerrit, gitlab.wikimedia.org); WebFetch
  often gets 403'd. Try WebFetch first for non-Wikimedia sites.

## Documentation

- Project wiki (the real docs): https://github.com/WikipediaLibrary/externallinks/wiki
- Phabricator: https://phabricator.wikimedia.org/tag/wikilink-tool/
- Migration toolkit: https://gitlab.wikimedia.org/repos/modtools/cloudvps-compose-deploy
- Cloud VPS practice: https://www.mediawiki.org/wiki/Moderator_Tools/Development/Cloud_VPS
  (rootless-compose model: the
  [Hashtags playbook](https://www.mediawiki.org/wiki/Moderator_Tools/Development/Cloud_VPS/Hashtags))
