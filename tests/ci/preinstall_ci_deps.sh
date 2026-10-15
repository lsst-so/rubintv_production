#!/bin/bash
# Install rapid analysis dependencies that are NOT shipped with the
# stock LSST stack / rubinenv: the integration suite's runtime deps, plus
# the mypy / pytest / mypy-coverage tooling that unit-test validation and
# the pre-commit hook run.
#
# Run this once per user, after activating the LSST stack
# (`source loadLSST.bash && setup lsst_distrib`), and again whenever the
# underlying conda env changes. Re-running is safe (each step is
# idempotent).
#
# Strategy: pip only, no conda. Python packages go into the active env
# when its site-packages is writable (a personal stack install), and into
# the user's ${HOME}/.local site-packages when it isn't (the shared,
# read-only rubinenv on dev nodes). User-site copies outlive the env and
# shadow its own packages, so any that the env now provides at the same
# or a newer version are removed first. redis-server is built from
# source into ${HOME}/local/bin, which tests/ci/setup_ci_env.sh puts on
# PATH.
#
# Conda-only dependencies from the production Dockerfile that this
# script does NOT install:
#   - rubin-libradtran  (conda-forge only; an atmospec/Spectractor dep,
#                        only needed for the LATISS spectral pipeline)
#
# If `pip install --user` is disabled in your env, create a venv on top
# of the conda Python instead:
#     python -m venv --system-site-packages ${HOME}/ra-ci-venv
#     source ${HOME}/ra-ci-venv/bin/activate
# and re-run this script.

if [[ "${BASH_SOURCE[0]}" != "${0}" ]]; then
    echo "ERROR: run this script directly, do not source it" >&2
    echo "       (sourcing would leak 'set -euo pipefail' into your shell)" >&2
    return 1
fi

set -euo pipefail

# Python deps - mirrors the conda + pip lists in the Dockerfile, minus
# rubin-libradtran (see header) and easyocr (removed).
PIP_PACKAGES=(
    google-cloud-storage
    # PyPI's lsst-efd-client tops out at 0.13.1, which hard-pins
    # numpy==1.23.5 and won't build on Python 3.13. v1.0.0 on the
    # lsst-ts repo drops the pin; install from the git tag until it
    # ships to PyPI.
    "lsst-efd-client @ git+https://github.com/lsst-ts/lsst-efd-client@v1.0.0"
    pytorch_lightning
    sentry-sdk
    "redis>=8"
    batoid
    "danish>=1.3.0"
    timm
    peft
    fakeredis
    "mypy>=2.0,<3.0"
    "mypy-coverage==0.2.3"
    pytest
    pytest-cov
    pytest-xdist
    types-PyYAML
    types-requests
)

RETIRED_PACKAGES=(
    types-redis
)

REDIS_VERSION="${REDIS_VERSION:-7.2.5}"
REDIS_PREFIX="${REDIS_PREFIX:-${HOME}/local}"

echo "==> [1/3] Removing stale user-site packages"
STALE=$(python - "${RETIRED_PACKAGES[*]}" <<'EOF'
import importlib.metadata
import site
import sys

from packaging.utils import canonicalize_name
from packaging.version import InvalidVersion, Version


def versions(path):
    return {
        canonicalize_name(dist.metadata["Name"]): dist.version
        for dist in importlib.metadata.distributions(path=[p for p in path if p])
    }


retired = {canonicalize_name(name) for name in sys.argv[1].split()}
userSite = site.getusersitepackages()
if userSite not in sys.path:
    sys.exit(0)
userIndex = sys.path.index(userSite)
earlierNames = versions(sys.path[:userIndex])
envVersions = versions(sys.path[userIndex + 1 :])

stale = []
for dist in importlib.metadata.distributions(path=[userSite]):
    name = canonicalize_name(dist.metadata["Name"])
    envVersion = envVersions.get(name)
    if name in earlierNames:
        continue
    if name in retired:
        print(f"    {name} {dist.version}: no longer used", file=sys.stderr)
        stale.append(name)
    elif envVersion is not None:
        try:
            shadowed = Version(envVersion) >= Version(dist.version)
        except InvalidVersion:
            continue
        if shadowed:
            print(f"    {name} {dist.version}: env provides {envVersion}", file=sys.stderr)
            stale.append(name)
print(" ".join(stale))
EOF
)
if [[ -n "${STALE}" ]]; then
    pip uninstall -y ${STALE}
else
    echo "    none found"
fi

echo
SITE_PACKAGES=$(python -c "import sysconfig; print(sysconfig.get_path('purelib'))")
echo "==> [2/3] Installing Python deps"
echo "    Packages: ${PIP_PACKAGES[*]}"
if [[ -w "${SITE_PACKAGES}" ]]; then
    echo "    Target: the active env (${SITE_PACKAGES})"
    pip install "${PIP_PACKAGES[@]}"
else
    USER_BIN="$(python -m site --user-base)/bin"
    echo "    Target: user site ($(python -m site --user-site)), as ${SITE_PACKAGES} is read-only"
    pip install --user "${PIP_PACKAGES[@]}"
    case ":${PATH}:" in
        *:"${USER_BIN}":*) ;;
        *) echo "    WARNING: ${USER_BIN} is not on PATH, so mypy and mypy-coverage" \
                "won't be found until it is" ;;
    esac
fi

echo
echo "==> [3/3] Ensuring redis-server is on PATH"
if command -v redis-server >/dev/null 2>&1; then
    echo "    redis-server already available at $(command -v redis-server) - skipping build"
else
    echo "    Building redis ${REDIS_VERSION} from source into ${REDIS_PREFIX} ..."
    mkdir -p "${REDIS_PREFIX}"
    BUILD_DIR=$(mktemp -d)
    trap 'rm -rf "${BUILD_DIR}"' EXIT
    cd "${BUILD_DIR}"

    curl -fsSL "https://download.redis.io/releases/redis-${REDIS_VERSION}.tar.gz" -o redis.tar.gz
    tar xf redis.tar.gz
    cd "redis-${REDIS_VERSION}"
    make -j 4
    make PREFIX="${REDIS_PREFIX}" install

    echo "    redis-server installed to ${REDIS_PREFIX}/bin/redis-server"
fi

echo
echo "==> Verifying everything is reachable"
for module in sentry_sdk redis fakeredis danish lsst_efd_client; do
    python -c "import ${module}; print('    ${module}:', ${module}.__file__)"
done
for tool in mypy mypy-coverage; do
    command -v "${tool}" >/dev/null 2>&1 \
        || { echo "ERROR: ${tool} not on PATH" >&2; exit 1; }
    echo "    ${tool}: $(command -v "${tool}")"
done
"${REDIS_PREFIX}/bin/redis-server" --version 2>/dev/null \
    || command -v redis-server >/dev/null 2>&1 \
    || { echo "ERROR: redis-server still not findable" >&2; exit 1; }

echo
echo "Done. Dependencies are installed."
echo "Make sure tests/ci/setup_ci_env.sh is sourced before running CI -"
echo "it adds ${REDIS_PREFIX}/bin to PATH so the redis-server binary is found."
