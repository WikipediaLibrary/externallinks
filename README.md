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

## Swift object store - local setup (separate container)

IMPORTANT: This setup should NOT be used in production! It is only suitable for local testing.

Reference: https://github.com/CSCfi/docker-keystone-swift

- Setup a separate single container for the Swift object store + Keystone auth
  - In terminal, run:
  ```bash
  docker run --name externallinks-swift -d \
    --env S6_LOGGING=0 \
    -p 8080:8080 \
    -p 5000:5000 \
    -p 6000:6000 -p 6001:6001 -p 6002:6002 \
    -v externallinks_swift:/srv/node \
    ghcr.io/cscfi/docker-keystone-swift:latest
  ```
  - This will create a new single container from a built image from ghcr.io, and will create a local volume (`-v`) so you don't lose everything when restarting the container
  - `S6_LOGGING` is to enable openstack logging to the console (better visualization in Docker)
  - Known possible errors:
    - If you have a service already using one of those ports, you can remap as you wish `-p 5001:5000 \`

- Connect the network between this new container and the Django `externallinks` stack
  - When you run `docker network ls`, it is expected you'd get something like
    ```
    NETWORK ID     NAME                            DRIVER    SCOPE
    12e998998050   externallinks_default           bridge    local
    ```
  - `externallinks_default` is the network from the `externallinks` stack (the Django app), connect it to the new container
    ```
    docker network connect externallinks_default externallinks-swift
    ```
  - To make calls from Django to Swift (locally), you can now use the URL `http://externallinks-swift:{port}`
    - The env var `OPENSTACK_AUTH_URL` can now be set to `http://externallinks-swift:5001/v3` (in the Django app)
- Setup the new container
  - Access the running container
    ```
    docker exec -it externallinks-swift bash
    ```
  - Create the wikilink project
    ```
    openstack project create --domain default --description "Wikilink" wikilink
    +-------------+----------------------------------+
    | Field       | Value                            |
    +-------------+----------------------------------+
    | description | Wikilink                         |
    | domain_id   | default                          |
    | enabled     | True                             |
    | id          | 544303a08953422b848a2393616adf63 |
    | is_domain   | False                            |
    | name        | wikilink                         |
    | options     | {}                               |
    | parent_id   | default                          |
    | tags        | []                               |
    +-------------+----------------------------------+
    ```
  - Create a new user (you can use one of the pre-configured users, skip this if you want)
    ```
    openstack user create --domain default --password djangopass djangouser
    +---------------------+----------------------------------+
    | Field               | Value                            |
    +---------------------+----------------------------------+
    | domain_id           | default                          |
    | enabled             | True                             |
    | id                  | ec41d7c9923e4b06821eca9fb40df2ab |
    | name                | djangouser                       |
    | options             | {}                               |
    | password_expires_at | None                             |
    +---------------------+----------------------------------+
    ```
  - Assign user to the project
    ```
    openstack role add --project wikilink --user djangouser admin
    ```
  - Update the variables to start using the created user and project
    ```
    export OS_USERNAME=djangouser
    export OS_PASSWORD=djangopass
    export OS_PROJECT_NAME=wikilink
    ```
    - You can set these values as default in your Docker, otherwise you need to remember to set this whenever you want to bash into this container
  - Create an application credential (copy the obtained `id` and `secret`)
    ```
    openstack application credential create --role admin django_admin_credential
    +--------------+----------------------------------------------------------------------------------------+
    | Field        | Value                                                                                  |
    +--------------+----------------------------------------------------------------------------------------+
    | description  | None                                                                                   |
    | expires_at   | None                                                                                   |
    | id           | f85b36fcf5754ddb9fefe676b4e08286                                                       |
    | name         | django_admin_credential                                                                |
    | project_id   | 544303a08953422b848a2393616adf63                                                       |
    | roles        | admin                                                                                  |
    | secret       | LjEcrcS2A70DFYOxJayPuB336CqMPvUBr-8RWEV7anW3U-99bKPUqTf5jfwEr9dU-rhQt-WXRLfftGC04w4fzA |
    | system       | None                                                                                   |
    | unrestricted | False                                                                                  |
    | user_id      | ec41d7c9923e4b06821eca9fb40df2ab                                                       |
    +--------------+----------------------------------------------------------------------------------------+
    ```
    - Copy the credential id value to the env var `SWIFT_APPLICATION_CREDENTIAL_ID` in the Django app
    - Copy the secret value to the env var `SWIFT_APPLICATION_CREDENTIAL_SECRET` in the Django app
  - One final step now is to set the local Swift endpoint to the network we connected previously
    - When you execute the command `openstack endpoint list`, you'd see something like
      ```
      +----------------------------------+-----------+--------------+--------------+---------+-----------+--------------------------------------------+
      | ID                               | Region    | Service Name | Service Type | Enabled | Interface | URL                                        |
      +----------------------------------+-----------+--------------+--------------+---------+-----------+--------------------------------------------+
      | a4b6a66d5b204afaab8afbe6d3521c8a | RegionOne | keystone     | identity     | True    | public    | http://localhost:5000/v3                   |
      | a98f5748c124489291747fcfb7a1a84a | RegionOne | keystone     | identity     | True    | internal  | http://localhost:5000/v3                   |
      | c9c933e896074f7880ff00b9aafec468 | RegionOne | keystone     | identity     | True    | admin     | http://localhost:5000/v3                   |
      | 3b5bdc053046452b943ed2d021cf1d3a | RegionOne | swift        | object-store | True    | internal  | http://0.0.0.0:8080/v1/AUTH_%(project_id)s |
      | b5f6dc8fde7d4cd59ce616edcbf47a58 | RegionOne | swift        | object-store | True    | admin     | http://0.0.0.0:8080/v1                     |
      | 3af31d12aa1b40c2b24fbc9eeef57f6d | RegionOne | swift        | object-store | True    | public    | http://0.0.0.0:8080/v1/AUTH_%(project_id)s |
      +----------------------------------+-----------+--------------+--------------+---------+-----------+--------------------------------------------+
      ```
    - Notice how the URL for the Swift service are pointing to localhost, they need to target the connection we created
    - Delete the existing Swift endpoints (pay attention to the IDs)
      ```
      openstack endpoint delete 3b5bdc053046452b943ed2d021cf1d3a
      openstack endpoint delete b5f6dc8fde7d4cd59ce616edcbf47a58
      openstack endpoint delete 3af31d12aa1b40c2b24fbc9eeef57f6d
      ```
    - And finally, create the endpoints with the correct URL
      ```
      openstack endpoint create --region RegionOne object-store public http://externallinks-swift:8080/v1/AUTH_%\(project_id\)s
      openstack endpoint create --region RegionOne object-store internal http://externallinks-swift:8080/v1/AUTH_%\(project_id\)s
      openstack endpoint create --region RegionOne object-store admin http://externallinks-swift:8080/v1
      ```
