# Data tracking

Wikilink monitors the [page-links-change](https://stream.wikimedia.org/?doc#/Streams/get_v2_stream_page_links_change)
[EventStream](https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams)
for link additions and removals against the URL patterns it tracks.

## Link event tracking

The `linkevents_collect` management command
(`extlinks/links/management/commands/linkevents_collect.py`) collects this data.
For each event in the stream it:

- checks whether the event is one we track (we have a URLPattern matching the URL
  in the event)
- if so, gets or creates a User for whoever triggered the event
- finds all URL patterns matching the event (we might track
  `clipping.newspapers.com` alongside `newspapers.com`)
- assumes the event relates to a single organisation (see the comment in
  `linkevents_collect.py`), and checks that organisation's authorized-user list
  (below) for a match; on a hit, sets `on_user_list` to `True`
- saves the LinkEvent

## Username lists

For each link event we cross-reference the user against a list of users from the
Library Card Platform. Ideally this would be destination-agnostic so the tool can
serve other use cases, but for now it's implemented in a way that only supports
The Wikipedia Library.

`users_update_lists`
(`extlinks/organisations/management/commands/users_update_lists.py`) runs
regularly to refresh those lists. For each organisation it reads the username-list
URL field and fetches the response the API returns there. The data follows the
format defined by the TWLight user serializer, served through its AuthorizedUsers
view (`TWLight/users/serializers.py` and `TWLight/users/views.py` in the
[TWLight repo](https://github.com/WikipediaLibrary/TWLight)).
