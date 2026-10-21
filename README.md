[![Docker Workflow Status](https://github.com/WikipediaLibrary/externallinks/workflows/Docker/badge.svg)](https://github.com/WikipediaLibrary/externallinks/actions?query=workflow%3ADocker)


[![Dependabot Status](https://api.dependabot.com/badges/status?host=github&repo=WikipediaLibrary/externallinks)](https://dependabot.com)

# Wikilink
Wikilink is a tool for tracking link additions made to Wikimedia projects as a result of Wikimedia partnerships. It monitors link additions and removals to specific URL patterns across all Wikimedia projects, providing high level statistics and graphs for analysis and tracking.

# Usage
The tool is currently live at https://wikilink.wmflabs.org/. There is currently no way for users to input new tracking patterns or other data, and therefore the only partnerships tracked at present are for [The Wikipedia Library](https://meta.wikimedia.org/wiki/The_Wikipedia_Library).

Individual link events are tracked from the [page-links-change](https://stream.wikimedia.org/?doc#!/Streams/get_v2_stream_page_links_change) event stream, and stored in the tools database if the changed URL matches a pattern in the tool.

The tool also monitors the total number of links across all Wikimedia projects for each URL pattern by using the [externallinks table](https://www.mediawiki.org/wiki/Manual:Externallinks_table).

**[Organisation](https://wikilink.wmflabs.org/organisations/)** pages provide a view applicable to a single organisation. Organisations can have multiple collections of tracked URLs - these could be different websites or simply different URL patterns. Results for each collection are presented individually. Additionally, each collection can have multiple URLs. This is useful primarily in the case that a website has moved; both URLs can continue to be tracked in the same place.

**[Programs](https://wikilink.wmflabs.org/programs/)** provide a high level overview of data from multiple organisations. A program isn't required for tracking links, and simply helps retrieve high level data for programs like The Wikipedia Library that are interested in the overall impact from multiple partners.

# Local development

Contributions to the tool are welcomed! A list of open tasks can be found on the tool's [Phabricator workboard](https://phabricator.wikimedia.org/project/view/4082/).

The tool uses the [Django framework](https://www.djangoproject.com/) and is deployed via [Docker](https://www.docker.com/). Docker and docker-compose are required for local setup.

After cloning the repository to your directory of choice:
1. Copy `template.env` to `.env`. No further changes are required for local development.
2. Run `docker-compose up -d --build` to build containers. If you want to enable the [Django Debug Toolbar](https://django-debug-toolbar.readthedocs.io/en/latest/index.html), you should run `docker-compose build --build-arg REQUIREMENTS_FILE=local.txt && docker-compose up`
3. The `eventstream` container will fail to build on the first run, so it will need to be restarted with `docker restart externallinks_eventstream_1`

You should now be able to access the tool via `localhost`.

## Example data

To populate a local instance for testing, run the [`example_data.sh`](bin/example_data.sh)
script. It generates:

- 10 programs, each with 1-20 organisations, 1-3 collections per organisation,
  1-2 URL patterns per collection, and 10-50 new users per organisation
- 60 monthly LinkSearchTotal figures for each URL pattern
- 10,000 link events across all patterns

## Running tests

The tests can be run within the container using docker exec. The following command will run the test suite:

```bash
docker exec -ti externallinks-externallinks-1 python manage.py test
```

If you would like to generate a coverage report as well, you can invoke the test runner using `coverage run` instead:

```bash
docker exec -ti externallinks-externallinks-1 coverage run manage.py test
```

You can view this report in your browser (located at `htmlcov/index.html`) by running the following command:

```bash
docker exec -ti externallinks-externallinks-1 coverage html
```

## Swift object store (local)

IMPORTANT: This setup is only for local testing, never production.

A bare `docker compose up` also loads `docker-compose.override.yml`, which runs
a local Keystone + Swift ([CSCfi/docker-keystone-swift](https://github.com/CSCfi/docker-keystone-swift))
as the `swift` service. The one-shot `swift-init` service then runs
`bin/swift_local_init.sh`, which creates the `wikilink` project, the
`djangouser` user, and the application credential named in `.env`, and points
the Swift endpoints at `http://swift:8080`. The defaults in `template.env`
already match, so the archive commands work without further setup. Swift
containers are created on first upload. An `.env` copied before this setup
existed needs the Swift block from `template.env` copied over it.

The web app and crons wait for `swift-init` to finish before starting.
Objects and Keystone's database live in the `swift_objects` and
`swift_keystone` volumes, so like the database they survive
`docker compose down` and go away with `docker compose down -v`.

The image is pinned because it never migrates Keystone's database on its
own. After bumping both image tags in `docker-compose.override.yml`,
migrate it once:

```bash
docker compose up -d swift
docker compose exec swift su -s /bin/sh -c "keystone-manage db_sync" keystone
docker compose restart swift
```

To look at what's stored:

```bash
docker compose exec -e OS_USERNAME=djangouser -e OS_PASSWORD=djangopass \
  -e OS_PROJECT_NAME=wikilink swift swift list archive-linkevents
```

If `swift-init` fails, `docker compose logs swift swift-init` shows why.
