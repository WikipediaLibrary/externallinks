import concurrent.futures
import logging
import os

from typing import Iterable, List, Tuple, cast, Dict, Optional

import swiftclient
import keystoneauth1.identity.v3 as identity
import keystoneauth1.session as session

logger = logging.getLogger("django")

MAX_WORKERS = 10
OBJECT_LIMIT = 1000


def swift_connection() -> swiftclient.Connection:
    """
    Creates a swiftclient Connection configured using environment variables.

    This method works with v3 application credentials authentication only.

    Returns
    -------
    swiftclient.Connection
        A connection to the Swift object storage.
    """

    auth_url = os.environ.get("OPENSTACK_AUTH_URL")
    credential_id = os.environ.get("SWIFT_APPLICATION_CREDENTIAL_ID")
    credential_secret = os.environ.get("SWIFT_APPLICATION_CREDENTIAL_SECRET")

    if not auth_url or not credential_id or not credential_secret:
        raise RuntimeError(
            "The 'OPENSTACK_AUTH_URL', 'SWIFT_APPLICATION_CREDENTIAL_ID' and "
            "'SWIFT_APPLICATION_CREDENTIAL_SECRET' environment variables must "
            "be defined to use the Swift client"
        )

    return swiftclient.Connection(
        session=session.Session(
            auth=identity.ApplicationCredential(
                auth_url=auth_url,
                application_credential_id=credential_id,
                application_credential_secret=credential_secret,
                user_domain_id="default",
            )
        )
    )


def get_containers(conn: swiftclient.Connection) -> List[dict]:
    """
    Retrieves a list of containers from object storage.

    Parameters
    ----------
    conn : swiftclient.Connection
        A connection to the Swift object storage.

    Returns
    -------
    List[dict]
        A list of dictionaries containing information about each container.
    """
    try:
        response = conn.get_account()
    except RuntimeError:
        logger.error("Swift credentials not provided. Skipping upload.")
        return False
    if not response or len(response) < 2:
        raise RuntimeError("Failed to retrieve container list from Swift account.")

    return response[1]


def ensure_container_exists(conn: swiftclient.Connection, container: str) -> bool:
    """
    Creates a new container in object storage if it doesn't already exist.

    Parameters
    ----------
    conn : swiftclient.Connection
        A connection to the Swift object storage.

    container : str
        The name of the container to create.

    Returns
    -------
    bool
        True if the container was created, False if it already existed.
    """

    containers = (c["name"] for c in get_containers(conn))
    if container not in containers:
        conn.put_container(container)
        return True

    return False


def get_object_list(
    conn: swiftclient.Connection,
    container: str,
    prefix: Optional[str] = None,
) -> List[Dict]:
    """
    Gets a list of all objects in a container matching an optional prefix.
    """

    objects = []
    marker = None

    while True:
        _, objects_page = conn.get_container(
            container,
            prefix=prefix,
            marker=marker,
            limit=OBJECT_LIMIT,
        )
        if not objects_page:
            break

        objects.extend(objects_page)
        marker = objects_page[-1]["name"]

        if len(objects_page) < OBJECT_LIMIT:
            break

    return objects


def upload_file(
    conn: swiftclient.Connection,
    container: str,
    path: str,
    content_type="application/octet-stream",
):
    """
    Uploads a file on the local filesystem to the provided Swift container.

    Parameters
    ----------
    conn : swiftclient.Connection
        A connection to the Swift object storage.

    container : str
        The name of the container to upload the file to.

    path : str
        The path to the file on the local filesystem.

    content_type : str
        The content type of the file.

    Returns
    -------
    str
        The name of the object in Swift.
    """

    object_name = os.path.basename(path)

    with open(path, "rb") as f:
        conn.put_object(
            container,
            object_name,
            contents=f,
            content_type=content_type,
        )

    return object_name


def download_file(conn: swiftclient.Connection, container: str, object: str) -> bytes:
    """
    Downloads a file from object storage.

    Parameters
    ----------
    conn : swiftclient.Connection
        A connection to the Swift object storage.

    container : str
        The name of the container to download the file from.

    object : str
        The name of the object to download.

    Returns
    -------
    bytes
        The contents of the object.
    """

    _, response = conn.get_object(container, object)

    return cast(bytes, response)


def file_exists(conn: swiftclient.Connection, container: str, object: str) -> bool:
    """
    Checks if a file exists in Swift.

    Parameters
    ----------
    conn : swiftclient.Connection
        A connection to the Swift object storage.

    container : str
        The name of the container to check.

    object : str
        The name of the object to check.

    Returns
    -------
    bool
        True if the file exists, False if it doesn't.
    """

    try:
        conn.head_object(container, object)
        return True
    except swiftclient.ClientException as e:
        if e.http_status == 404:
            return False
        else:
            raise e


def batch_upload_files(
    conn: swiftclient.Connection,
    container: str,
    files: Iterable[str],
    max_workers=MAX_WORKERS,
) -> Tuple[List[str], List[str]]:
    """
    Uploads a batch of multiple files to the given Swift container.

    Parameters
    ----------
    conn : swiftclient.Connection
        A connection to the Swift object storage.

    container : str
        The name of the container to upload the files to.

    files : Iterable[str]
        An iterable of file paths to upload.

    max_workers : int
        The maximum number of concurrent uploads to perform.

    Returns
    -------
    Tuple[List[str], List[str]]
        A tuple containing two lists. The first list contains the names of the
        files that were successfully uploaded. The second list contains the
        names of the files that failed to upload.
    """

    successful = []
    failed = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(upload_file, conn, container, f): f for f in files}

        for future in concurrent.futures.as_completed(futures):
            path = futures[future]

            try:
                object_name = future.result()
                logger.info(f"Successfully uploaded '%s' as '%s'", path, object_name)
                successful.append(path)
            except Exception as exc:
                logger.error(f"Upload failed for '%s': %s", path, exc)
                failed.append(path)

    return successful, failed


def batch_download_files(
    conn: swiftclient.Connection,
    container: str,
    objects: Iterable[str],
    max_workers=MAX_WORKERS,
) -> Dict[str, bytes]:
    """
    Downloads a batch of multiple files from the given Swift container.
    """

    result: Dict[str, bytes] = {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(download_file, conn, container, o): o for o in objects
        }

        for future in concurrent.futures.as_completed(futures):
            name = futures[future]

            try:
                value = future.result()
                result[name] = value
            except Exception as exc:
                logging.error(
                    "Unable to download '%s' from the '%s' container: %s",
                    name,
                    container,
                    exc,
                )

    return result
