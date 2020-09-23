from urllib.parse import unquote

from .models import URLPattern


def split_url_for_query(url):
    """
    Given a URL pattern, split it into two components:
    url_optimised: URL and domain name in the el_index format
    (https://www.mediawiki.org/wiki/Manual:Externallinks_table#el_index)
    url_pattern_end: Anything following the domain name
    """
    url = url.strip()  # Catch any trailing spaces
    # Start after *. if present
    if url.startswith("*."):
        url = url[2:]

    url_start = url.split("/")[0].split(".")[::-1]
    url_optimised = '.'.join(url_start) + ".%"

    if "/" in url:
        url_end = "/".join(url.split("/")[1:])
        url_pattern_end = "%./" + url_end + "%"
    else:
        url_pattern_end = '%'

    return url_optimised, url_pattern_end


def link_is_tracked(link):
    all_urlpatterns = URLPattern.objects.all()
    tracked_links_list = list(all_urlpatterns.values_list('url', flat=True))
    proxied_url = False

    # If this looks like a TWL proxied URL we're going to need to match
    # it against a longer list of strings
    if "wikipedialibrary.idm.oclc" in link:
        proxied_url = True
        proxied_urls = [urlpattern.get_proxied_url
                        for urlpattern in all_urlpatterns]
        tracked_links_list.extend(proxied_urls)

    # This is a quick check so we can filter the majority of events
    # which won't be matching our filters
    if any(links in link for links in tracked_links_list):
        # Then we do a more detailed check, to make sure this is the
        # root URL.
        for tracked_link in tracked_links_list:
            # If we track apa.org, we don't want to match iaapa.org
            # so we make sure the URL is actually pointing at apa.org
            url_starts = ["//" + tracked_link, "." + tracked_link]
            if proxied_url:
                # Proxy URLs may contain //www- not //www.
                url_starts.append("-" + tracked_link)

            # We want to avoid link additions from e.g. InternetArchive
            # where the URL takes the structure
            # https://web.archive.org/https://test.com/
            protocol_count = link.count("//")

            if any(start in link for start in url_starts) and protocol_count < 2:
                return True
    else:
        return False
