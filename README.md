[![Docker Workflow Status](https://github.com/WikipediaLibrary/externallinks/workflows/Docker/badge.svg)](https://github.com/WikipediaLibrary/externallinks/actions?query=workflow%3ADocker)

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
2. Run `docker-compose up -d --build` to build containers.
3. The `eventstream` container will fail to build on the first run, so it will need to be restarted with `docker restart externallinks_eventstream_1`

You should now be able to access the tool via `localhost`.
