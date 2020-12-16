#!/usr/bin/env bash

# One level up from this script, which should be the root of this repo.
dir=$(dirname $(readlink -f $0))/..

# Gets a value from a dotenv file at .env.
function default () {
    grep ${1} ${dir}/.env | cut -d '=' -f2
}

# Pull updated image if available.
function pull () {
    image_name=${1}
    tag=${2}
    target=${image_name}:${tag}
    
    # Check for newer image
    pull=$(docker pull ${target})

    # Pull swarm config updates and update the stack if there is a new image.
    if echo ${pull} | grep "Status: Downloaded newer image for ${target}" >/dev/null
    then
       echo "${target} updated"

    # Report if the local image is already up to date.
    elif echo ${pull} | grep "Status: Image is up to date for ${target}" >/dev/null
    then
       echo "${target} already up to date"

    # Fail in any other circumstance.
    else
       echo "Error updating ${target}"
       exit 1
    fi
}

# Take .env values or arguments.
env=${1:-$(default ENV)}
externallinks_tag=${2:-$(default EXTERNALLINKS_TAG)}
eventstream_tag=${3:-$(default EVENTSTREAM_TAG)}

if [ -z "$env" ] || [ -z "$externallinks_tag" ] || [ -z "$eventstream_tag" ]
then
    echo "Usage: swarm_update.sh \$env \$externallinks_tag \$eventstream_tag
    \$env               docker swarm environment (eg. staging | production).
    \$externallinks_tag docker hub image tag (eg. branch_staging | branch_production | latest)
    \$eventstream_tag   docker hub image tag (eg. branch_staging | branch_production | latest)"
    exit 1;
fi

# Pull image updates.
pull quay.io/wikipedialibrary/externallinks ${externallinks_tag}
pull quay.io/wikipedialibrary/eventstream ${eventstream_tag}

# Update repository for updates to code or to the swarm deployment itself.
git -C ${dir} pull

# Deploy the updates.
docker stack deploy -c <(cd ${dir}; docker-compose config) ${env}
