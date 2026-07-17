# App structure

The Wikilink app's structure broadly reflects the way access is provided in The
Wikipedia Library, generalised to support other use cases. A few interlocking
models fit together like this:

- **Programs** group organisations so their data can be collated in one place.
  The Wikipedia Library is a program: it has many partner organisations, and
  staff want to view totals collated across all of them.
- **Organisations** hold one or more collections, each with its own URL patterns
  to track, and want that data viewable in one place. For The Wikipedia Library,
  organisations are the publisher partners viewing link-tracking data for just
  their URL patterns. An organisation can belong to multiple programs.
- **Collections** are usually individual websites or resources published by or of
  interest to an organisation. Springer Nature, for example, hosts both Springer
  Link and Nature. A collection is tied to a single organisation; an organisation
  can have one or many collections.
- **URL patterns** define which URLs we track. The value doesn't need to be a
  tidy URL: we match against a pattern like `website.com`, and subdomains and
  paths are allowed too, such as `gateway.proquest.com/openurl/`. A URLPattern
  links to a single collection, but a collection can have several URL patterns,
  which is handy when a site's URL changes and we want to track both the old and
  new forms.
