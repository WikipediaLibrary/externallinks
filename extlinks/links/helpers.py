def split_url_for_query(url):
    """
    Given a URL pattern, split it into two components:
    url_optimised: URL and domain name in the el_index format
    (https://www.mediawiki.org/wiki/Manual:Externallinks_table#el_index)
    url_pattern_end: Anything following the domain name
    Returned with % in the place of * ready for db querying
    """
    url = url.strip()  # Catch any trailing spaces
    # Start after *. if present
    if url[0] == "*":
        url = url[2:]

    url_start = url.split("/")[0].split(".")[::-1]
    url_optimised = '.'.join(url_start) + ".%"

    if "/" in url:
        url_end = "/".join(url.split("/")[1:])
        url_pattern_end = "%./" + url_end + "%"
    else:
        url_pattern_end = '%'

    return url_optimised, url_pattern_end
