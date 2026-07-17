# Developer and maintainer notes

Operational notes for deploying and testing Wikilink. They live in-tree, next to
the code they describe, so a change to a deploy mechanism and its documentation
travel in the same commit instead of drifting apart on the wiki.

Contributor setup (local development, running the test suite) is in the README.
This file is the maintainer side: how production is deployed, and how to stand
up a throwaway instance to test a change end to end.

## Deployment model

Production runs on a Cloud VPS VM managed by the CloudVPS Compose Deploy toolkit
(https://gitlab.wikimedia.org/repos/modtools/cloudvps-compose-deploy): rootless
docker compose, one environment per VM. Two properties matter when testing:

- **The image is a dependency layer, not the code.** The containers bind-mount
  the git checkout at `/srv/externallinks`, so the running code is whatever the
  checkout is synced to. The published image carries only the Python runtime and
  installed dependencies. CI publishes it as `:latest` (the current master
  build) and `commit_<sha>` (one per master commit); see
  `.github/workflows/dockerpublish.yml`.

- **A git ref names what's deployed.** The VM's instance metadata sets `env`,
  which names both the environment and the git ref the toolkit keeps the
  checkout synced to (`master` for production). The ref is any git ref: a branch
  tracks its tip, a tag or commit pins a fixed state.

So a deployed state has two independent knobs: the code (the ref the checkout is
synced to) and the dependencies (the image tag pinned in `.env`). Testing a
code-only change needs only a ref; testing a dependency change needs a matching
image.

## Testing a change on a throwaway VM

To exercise a change on a real VM without touching production, provision a
separate, disposable instance and point it at the ref and image you want.

1. Provision a throwaway VM through the toolkit's cloud-init bootstrap (see the
   toolkit README). In the instance metadata:
   - `env`: the git ref to deploy. A branch (`master` or a feature branch), a
     tag, or a commit. The redeploy tick syncs the checkout to it, and for a
     branch tracks its tip as you push.
   - `ephemeral`: `true`. A throwaway VM has no dedicated cinder volumes, so this
     runs docker off the root filesystem instead of refusing to provision. Never
     set it on production.

2. Fill in `.env` from `template.env`, then pin the dependency image:
   ```
   EXTERNALLINKS_TAG=commit_<8sha>
   EVENTSTREAM_TAG=commit_<8sha>
   ```
   Use `latest` to track the current master build, or a `commit_<sha>` to pin a
   specific one (quay.io/wikipedialibrary lists the published tags). For a
   code-only change, `latest` is fine: the bind-mounted checkout carries your
   code. A change to the Dockerfile or `requirements/` needs its own image, so
   either wait for CI to publish its `commit_<sha>` after merge to master, or
   build it on the VM with `docker compose build`.

3. The redeploy tick brings the ref and image up within five minutes. Bring up a
   database the usual way (restore a dump, or start empty), then watch
   `docker compose logs -f externallinks eventstream`.

4. Tear down when done: delete the instance in Horizon.

Because `env` names the ref, a VM set to `env=<some-branch>` follows that
branch's tip as you push to it, which is handy for iterating in a
production-like environment. Set `env` to a tag or commit to freeze on a fixed
state.
