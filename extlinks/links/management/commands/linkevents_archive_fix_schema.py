import gzip, json, logging, os, re
from keystoneauth1.identity.v3 import ApplicationCredential
from keystoneauth1 import session as keystone_session
from swiftclient import client as swiftclient

from typing import List, Optional

from django.contrib.contenttypes.models import ContentType
from django.core import serializers
from django.core.management import BaseCommand

logger = logging.getLogger("django")
# Authentication for Swift Object Store
AUTH_URL = "https://openstack.eqiad1.wikimediacloud.org:25000/v3"
APPLICATION_CREDENTIAL_ID = os.environ["SWIFT_APPLICATION_CREDENTIAL_ID"]
APPLICATION_CREDENTIAL_SECRET = os.environ["SWIFT_APPLICATION_CREDENTIAL_SECRET"]
USER_DOMAIN_ID = "default"
PROJECT_NAME = os.environ["SWIFT_PROJECT_NAME"]


class Command(BaseCommand):
    help = "Updates schema for old LinkEvents archives, and upload them to Swift."

    def add_arguments(self, parser):
        parser.add_argument(
            "action",
            type=str,
            choices=["update", "upload"],
            help="update: updates the archive schema and loads the file to Swift. upload: uploads archive file(s) to Swift.",
        )
        parser.add_argument(
            "filenames",
            nargs="*",
            type=str,
            help="LinkEvent archive filenames to process.",
        )
        parser.add_argument(
            "-o",
            "--output",
            nargs="?",
            type=str,
            help="The directory that the archives containing the fixed LinkEvents should be written to.",
        )
        parser.add_argument(
            "--skip-validation",
            action="store_true",
            help="If enabled, skips model validation after updating the schema.",
        )
        parser.add_argument(
            "--skip-upload",
            action="store_true",
            help="If enabled, skips upload to Swift after updating the schema.",
        )

    def handle(self, *args, **options):
        action = options["action"]
        filenames = options["filenames"]

        if not filenames:
            logger.info("No link event archives specified")
            return

        if action == "update":
            for filename in sorted(filenames):
                if os.path.isfile(filename):
                    self.update_schema(
                        filename=filename,
                        output=options["output"],
                        skip_validation=options["skip_validation"],
                        skip_upload=options["skip_upload"],
                    )
                else:
                    logger.info(
                        f"Archive {filename} doesn't exist! Skipping this entry"
                    )

        elif action == "upload":
            for filename in sorted(filenames):
                if os.path.isfile(filename):
                    self.upload_swift(filename)
                else:
                    logger.info(
                        f"Archive {filename} doesn't exist! Skipping this entry"
                    )

    def update_schema(
        self,
        filename: str,
        output: Optional[str] = None,
        skip_validation: Optional[bool] = False,
        skip_upload: Optional[bool] = False,
    ):
        """
        Updates the JSON archive schema manually and validates it
        using Django serializers. If the new schema is valid, the
        updated archive is uploaded to Swift.

        Parameters
        ----------
        filenames : str
            List of filenames to process

        output : Optional[str]
            Optional directory to output the processed files. If none
            was specified, the files will be overwritten.

        skip_validation: Optional[bool]
            If enabled, it will skip the model validation after the
            schema updates.

        skip_upload: Optional[bool]
            If enabled, it will skip uploading the updated file to
            Swift.

        Returns
        -------
        None

        """
        logger.info(f"Updating schema for {filename}")

        try:
            urlpattern_content_type_id = ContentType.objects.get(model="urlpattern").id
        except ContentType.DoesNotExist:
            # This should not happen, but just in case
            logger.error("ContentType for URLPattern not found! Cannot proceed.")
            return

        with gzip.open(filename, "rt", encoding="utf-8") as archive:
            data = json.load(archive)

        for linkevent in data:
            fields = linkevent["fields"]

            # From migrations 0013 and 0014:
            # - Add content_type and object_id
            if "content_type" not in fields or "object_id" not in fields:
                url_values = fields.get("url", [])

                if len(url_values) == 1:
                    fields["content_type"] = urlpattern_content_type_id
                    fields["object_id"] = url_values[0]
                else:
                    fields["content_type"] = None
                    fields["object_id"] = None

        if skip_validation == False:
            try:
                logger.info("Validating schema")
                # Just parsing for validation purposes, not saving
                list(serializers.deserialize("json", json.dumps(data)))
            except Exception as e:
                logger.error(f"Schema validation failed: {e}")
                logger.error(f"Archive {filename} skipped")
                return

        if output:
            os.makedirs(output, exist_ok=True)
            new_archive_path = os.path.join(output, os.path.basename(filename))
        else:
            new_archive_path = filename

        with gzip.open(new_archive_path, "wt", encoding="utf-8") as new_archive:
            json.dump(data, new_archive)

        logger.info(f"Updated archive saved {new_archive_path}")

        if skip_upload == False:
            logger.info(f"Uploading archive to Swift")
            self.upload_swift(new_archive_path)

    def upload_swift(self, local_filepath: str):
        """
        Upload a file to Swift object storage, ensuring the container exists.
        Reference: https://docs.openstack.org/python-swiftclient/latest/client-api.html

        It is expected the filename follows the pattern
        `links_linkevent_YYYYMM[DD].N.json.gz`
        - `links_linkevent_201907.0.json.gz`
        - `links_linkevent_20250101.0.json.gz`

        Parameters
        ----------
        local_file_path : str
            The backup file path to be uploaded to Swift

        Returns
        -------
        None

        """
        remote_filename = os.path.basename(local_filepath)
        # Extract YYYYMM from the filename
        match = re.search(
            r"links_linkevent_(\d{6})\d{0,2}\.\d+\.json\.gz", remote_filename
        )

        if not match:
            logger.error(
                f"Invalid filename format: {remote_filename}. Skipping upload."
            )
            return

        # group 1 should contain YYYYMM
        container_name = f"linkevents-backup-{match.group(1)}"

        try:
            auth = ApplicationCredential(
                auth_url=AUTH_URL,
                application_credential_id=APPLICATION_CREDENTIAL_ID,
                application_credential_secret=APPLICATION_CREDENTIAL_SECRET,
                user_domain_id=USER_DOMAIN_ID,
                project_name=PROJECT_NAME,
            )
            session = keystone_session.Session(auth=auth)
            conn = swiftclient.Connection(session=session)

            # Ensure the container exists before uploading
            existing_containers = [
                container["name"] for container in conn.get_account()[1]
            ]
            if container_name not in existing_containers:
                logger.info(f"Creating new container: {container_name}")
                conn.put_container(container_name)  # Create the container

            # Upload the file
            with open(local_filepath, "rb") as f:
                conn.put_object(
                    container_name,
                    remote_filename,
                    contents=f,
                    content_type="application/gzip",
                )

            logger.info(
                f"Successfully uploaded {local_filepath} to Swift container {container_name}"
            )
            return

        except Exception as e:
            logger.error(f"Failed to upload {local_filepath} to Swift: {e}")
            return
