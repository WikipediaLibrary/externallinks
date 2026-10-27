#!/usr/bin/env bash
# Prepares the local dev Keystone + Swift container (see
# docker-compose.override.yml) for Django. Safe to rerun: it creates only
# what's missing and recreates the application credential so its secret
# always matches .env. Never point this at a real Keystone.
set -euo pipefail

project=wikilink
username="${SWIFT_APPLICATION_CREDENTIAL_USERNAME:?}"
password=djangopass
credential="${SWIFT_APPLICATION_CREDENTIAL_NAME:?}"
secret="${SWIFT_APPLICATION_CREDENTIAL_SECRET:?}"
swift_url=http://swift:8080/v1

openstack project show "$project" >/dev/null 2>&1 \
    || openstack project create --domain default --description Wikilink "$project"
openstack user show "$username" >/dev/null 2>&1 \
    || openstack user create --domain default --password "$password" "$username"
openstack role add --project "$project" --user "$username" admin

# Application credentials belong to the user, so manage them as that user.
as_user=(openstack --os-username "$username" --os-password "$password"
    --os-project-name "$project")
"${as_user[@]}" application credential delete "$credential" >/dev/null 2>&1 || true
"${as_user[@]}" application credential create --role admin --secret "$secret" "$credential" >/dev/null

# The image registers Swift at 0.0.0.0:8080, which other containers can't reach.
openstack endpoint list --service object-store -f value -c ID -c Interface \
    | while read -r id interface; do
        if [ "$interface" = admin ]; then
            url="$swift_url"
        else
            url="$swift_url/AUTH_%(project_id)s"
        fi
        openstack endpoint set --url "$url" "$id"
    done

echo "Local Swift ready: project $project, credential $credential for $username"
