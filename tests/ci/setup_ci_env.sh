#!/bin/bash
# Per-user environment variables for the rapid analysis CI suite.
#
# Edit the values below for your account, then `source` this file before
# running tests/ci/test_rapid_analysis.py or tests/createUnitTestCollections.py:
#
#     source tests/ci/setup_ci_env.sh
#
# Defaults shown here are the ones that work for `mfl` at SLAC.

# Base directory under which most CI output paths are rooted (plots,
# sidecar metadata, shards, AOS data, dimension universe file, etc.).
# Maps to ${RA_CI_DATA_ROOT} in config/config_usdf_testing.yaml.
export RA_CI_DATA_ROOT="/sdf/home/m/mfl/u/rubintv"

# Star tracker raw data root (does NOT include the
# GenericCamera/101/ or /102/ subpaths).
export RA_CI_STAR_TRACKER_DATA_PATH="/sdf/home/m/mfl/u/starTracker"

# Base directory containing the astrometry.net reference catalogues
# (does NOT include the /4100 or /4200 subdirectories).
export RA_CI_ASTROMETRY_NET_REF_CAT_PATH="/sdf/home/m/mfl/u/astrometry_net"

# TARTS pipeline data directory (read by the AOS worker).
export TARTS_DATA_DIR="/sdf/home/m/mfl/temp/TARTS"

# AI donut model data directory (read by the AOS worker).
export AI_DONUT_DATA_DIR="/sdf/home/m/mfl/u/rubintv/aos_data/AI_DONUT"

# Port the CI suite spawns its private redis-server on. Override if a
# colleague is already using this port on the same dev node.
export RA_CI_REDIS_PORT="${RA_CI_REDIS_PORT:-6111}"

# preinstall_ci_deps.sh builds redis-server into ${HOME}/local/bin when
# it can't find a system one. Make sure that's on PATH (no-op if you
# already have a system redis-server or have added this elsewhere).
case ":${PATH}:" in
    *:"${HOME}/local/bin":*) ;;
    *) export PATH="${HOME}/local/bin:${PATH}" ;;
esac

echo "[setup_ci_env] CI environment variables exported for user '${USER}':"
for v in RA_CI_DATA_ROOT RA_CI_STAR_TRACKER_DATA_PATH \
         RA_CI_ASTROMETRY_NET_REF_CAT_PATH TARTS_DATA_DIR \
         AI_DONUT_DATA_DIR RA_CI_REDIS_PORT; do
    echo "    ${v}=${!v}"
done
