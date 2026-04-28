#!/bin/bash
# Install rapid analysis CI dependencies that are NOT shipped with the
# stock LSST stack / rubinenv.
#
# Run this once per user, after activating the LSST stack
# (`source loadLSST.bash && setup lsst_distrib`), before running the CI
# suite for the first time. Re-running is safe (each step is idempotent).
#
# Strategy: pip --user only, no conda. The shared rubinenv on dev nodes
# is read-only, so we install Python packages into the user's
# ${HOME}/.local site-packages and build the redis-server binary from
# source into ${HOME}/local/bin. Both locations are picked up
# automatically once tests/ci/setup_ci_env.sh is sourced.
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
# and re-run this script (drop the --user flag from the pip command).

set -euo pipefail

# Python deps - mirrors the conda + pip lists in the Dockerfile, minus
# rubin-libradtran (see header) and easyocr (removed).
PIP_PACKAGES=(
    google-cloud-storage
    lsst-efd-client
    pytorch_lightning
    sentry-sdk
    redis
    batoid
    danish==1.0.0
    timm
    peft
)

REDIS_VERSION="${REDIS_VERSION:-7.2.5}"
REDIS_PREFIX="${REDIS_PREFIX:-${HOME}/local}"

echo "==> [1/2] Installing Python deps via pip --user"
echo "    Packages: ${PIP_PACKAGES[*]}"
pip install --user "${PIP_PACKAGES[@]}"

echo
echo "==> [2/2] Ensuring redis-server is on PATH"
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
python -c "import sentry_sdk; print('    sentry_sdk:', sentry_sdk.__file__)"
python -c "import redis; print('    redis (py):', redis.__file__)"
"${REDIS_PREFIX}/bin/redis-server" --version 2>/dev/null \
    || command -v redis-server >/dev/null 2>&1 \
    || { echo "ERROR: redis-server still not findable" >&2; exit 1; }

echo
echo "Done. CI dependencies are installed."
echo "Make sure tests/ci/setup_ci_env.sh is sourced before running CI -"
echo "it adds ${REDIS_PREFIX}/bin to PATH so the redis-server binary is found."
