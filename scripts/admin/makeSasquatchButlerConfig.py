# This file is part of rubintv_production.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

"""Write the ``+sasquatch`` variant of a butler repo config.

Rapid analysis publishes analysis_tools metric bundles to Sasquatch (and so to
Chronograf) by pointing its butlers at a copy of the repo's config whose
datastore is wrapped in a ChainedDatastore with analysis_tools'
SasquatchDatastore as the last child. That child only accepts the
MetricMeasurementBundle storage class and is write-only, so for every other
dataset, and for all reads, the repo behaves exactly as before.

This tool writes that config next to the original (or wherever ``--output``
says), checks that a read-only Butler can be built from it, and prints the
line to add to the site's repository index so that ``<label>+sasquatch``
resolves to it. It never modifies the original config and never contacts
Sasquatch: the first real contact happens when a worker puts a metric bundle.

Run it once per repo, per site, as a user who can write next to the repo's
butler.yaml (or pass ``--output``). The rapid analysis configs in
``config/config_<site>.yaml`` expect the aliases ``LSSTCam+sasquatch`` and
``LATISS+sasquatch`` at the summit, base and Tucson test stands, pointing at
that site's own REST proxy.

Examples
--------
At the summit, for the alias ``LSSTCam`` in ``$DAF_BUTLER_REPOSITORY_INDEX``::

    python makeSasquatchButlerConfig.py LSSTCam \\
        --rest-proxy-url https://summit-lsp.lsst.codes/sasquatch-rest-proxy \\
        --dataset-tag LSSTCam/rapid_analysis

For a repo given by path, writing the config somewhere else::

    python makeSasquatchButlerConfig.py /repo/LATISS \\
        --rest-proxy-url https://base-lsp.lsst.codes/sasquatch-rest-proxy \\
        --dataset-tag LATISS/rapid_analysis \\
        --output /project/rubintv/butlerConfigs/LATISS+sasquatch.yaml
"""

from __future__ import annotations

import argparse
import logging
import sys
from typing import Any

from lsst.daf.butler import Butler, Config
from lsst.resources import ResourcePath

BUTLER_ROOT_PLACEHOLDER = "<butlerRoot>"
CHAINED_DATASTORE_CLS = "lsst.daf.butler.datastores.chainedDatastore.ChainedDatastore"
FILE_DATASTORE_CLS = "lsst.daf.butler.datastores.fileDatastore.FileDatastore"
SASQUATCH_DATASTORE_CLS = "lsst.analysis.tools.interfaces.datastore.SasquatchDatastore"
DEFAULT_NAMESPACE = "lsst.dm"
DEFAULT_OUTPUT_NAME = "butler+sasquatch.yaml"

log = logging.getLogger("lsst.rubintv.production.makeSasquatchButlerConfig")


def resolveConfigUri(repo: str) -> tuple[ResourcePath, str | None]:
    """Resolve a repo label, directory or config file to its butler.yaml.

    Parameters
    ----------
    repo : `str`
        A label in the butler repository index, a repo directory or URI, or
        the path/URI of a butler.yaml.

    Returns
    -------
    configUri : `lsst.resources.ResourcePath`
        The URI of the repo's seed butler config.
    label : `str` or `None`
        ``repo`` if it was a repository-index label, else `None`.
    """
    label: str | None = None
    try:
        uri = Butler.get_repo_uri(repo)
        label = repo
        log.info(f"Resolved repository label {repo!r} to {uri}")
    except (KeyError, FileNotFoundError, RuntimeError):  # not a label, or no index: treat as a path/URI
        uri = ResourcePath(repo)

    if not uri.basename().endswith((".yaml", ".yml")):
        uri = ResourcePath(uri, forceDirectory=True).join("butler.yaml")
    if not uri.exists():
        raise FileNotFoundError(f"No butler config found at {uri}")
    return uri, label


def _replacePlaceholder(obj: Any, root: str) -> Any:
    """Replace ``<butlerRoot>`` in every string of a nested config structure.

    Parameters
    ----------
    obj : `Any`
        A (possibly nested) dict/list/scalar structure from a config.
    root : `str`
        The value to substitute for the placeholder.

    Returns
    -------
    replaced : `Any`
        The same structure with the placeholder substituted throughout.
    """
    if isinstance(obj, str):
        return obj.replace(BUTLER_ROOT_PLACEHOLDER, root)
    if isinstance(obj, dict):
        return {key: _replacePlaceholder(value, root) for key, value in obj.items()}
    if isinstance(obj, list):
        return [_replacePlaceholder(value, root) for value in obj]
    return obj


def makeSasquatchConfig(
    configUri: ResourcePath,
    outputUri: ResourcePath,
    restProxyUrl: str,
    namespace: str,
    datasetTag: str | None,
) -> Config:
    """Build the chained-datastore config from a repo's seed config.

    Parameters
    ----------
    configUri : `lsst.resources.ResourcePath`
        The repo's existing butler.yaml.
    outputUri : `lsst.resources.ResourcePath`
        Where the new config will be written. If this is not alongside the
        original, any ``<butlerRoot>`` placeholders are pinned to the original
        repo's directory, since the placeholder means "the directory this
        config file lives in".
    restProxyUrl : `str`
        The Sasquatch REST proxy to publish to.
    namespace : `str`
        The Sasquatch namespace to publish into.
    datasetTag : `str` or `None`
        If given, added as the ``dataset_tag`` field of every record.

    Returns
    -------
    config : `lsst.daf.butler.Config`
        The new config, identical to the original except for the datastore.
    """
    data: dict[str, Any] = Config(configUri).toDict()  # the seed config only, no defaults merged in

    original: dict[str, Any] = data.get("datastore") or {}
    original.setdefault("cls", FILE_DATASTORE_CLS)
    if original["cls"] == CHAINED_DATASTORE_CLS:
        children: list[dict[str, Any]] = list(original.get("datastores", []))
        if any(child.get("cls") == SASQUATCH_DATASTORE_CLS for child in children):
            raise RuntimeError(f"{configUri} already contains a SasquatchDatastore")
    else:
        if original["cls"] == FILE_DATASTORE_CLS:
            original.setdefault("root", BUTLER_ROOT_PLACEHOLDER)
        children = [original]

    sasquatch: dict[str, Any] = {
        "cls": SASQUATCH_DATASTORE_CLS,
        "restProxyUrl": restProxyUrl,
        "namespace": namespace,
    }
    if datasetTag:
        sasquatch["extra_fields"] = {"dataset_tag": datasetTag}
    data["datastore"] = {"cls": CHAINED_DATASTORE_CLS, "datastores": [*children, sasquatch]}

    if outputUri.dirname() != configUri.dirname():
        rootDir = configUri.dirname()
        root = rootDir.ospath.rstrip("/") if rootDir.isLocal else rootDir.geturl().rstrip("/")
        log.info(
            f"Output is not alongside the original config, so pinning {BUTLER_ROOT_PLACEHOLDER} to {root}"
        )
        data = _replacePlaceholder(data, root)

    return Config(data)


def validateConfig(uri: ResourcePath) -> list[str]:
    """Build a read-only Butler from a config and check it has a Sasquatch
    datastore.

    Parameters
    ----------
    uri : `lsst.resources.ResourcePath`
        The config to build from.

    Returns
    -------
    names : `list` [`str`]
        The names of the butler's datastores, for display.
    """
    butler = Butler.from_config(uri)
    datastore = getattr(butler, "_datastore", None)
    names: list[str] = list(getattr(datastore, "names", [])) if datastore is not None else []
    if not any("SasquatchDatastore" in name for name in names):
        raise RuntimeError(f"The Butler built from {uri} has no SasquatchDatastore; datastores: {names}")
    return names


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "repo",
        help="Repository-index label (e.g. LSSTCam), repo directory or URI, or the path of its butler.yaml",
    )
    parser.add_argument(
        "--rest-proxy-url",
        required=True,
        help="The Sasquatch REST proxy, e.g. https://summit-lsp.lsst.codes/sasquatch-rest-proxy",
    )
    parser.add_argument(
        "--dataset-tag",
        required=True,
        help="Value of the dataset_tag field added to every record, e.g. LSSTCam/rapid_analysis",
    )
    parser.add_argument(
        "--namespace", default=DEFAULT_NAMESPACE, help=f"Sasquatch namespace (default {DEFAULT_NAMESPACE})"
    )
    parser.add_argument(
        "--output",
        help=f"Where to write the new config (default: {DEFAULT_OUTPUT_NAME} next to the original)",
    )
    parser.add_argument(
        "--alias",
        help="Repository-index alias to suggest for the new config (default: <label>+sasquatch for a label)",
    )
    parser.add_argument("--overwrite", action="store_true", help="Overwrite the output if it exists")
    parser.add_argument(
        "--no-validate",
        action="store_true",
        help="Skip building a read-only Butler from the new config to check it (needs registry access)",
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    configUri, label = resolveConfigUri(args.repo)
    outputUri = ResourcePath(args.output) if args.output else configUri.dirname().join(DEFAULT_OUTPUT_NAME)
    if outputUri.exists() and not args.overwrite:
        parser.error(f"{outputUri} already exists; pass --overwrite to replace it")

    config = makeSasquatchConfig(configUri, outputUri, args.rest_proxy_url, args.namespace, args.dataset_tag)
    config.dumpToUri(outputUri, overwrite=args.overwrite)
    log.info(f"Wrote {outputUri}")

    if not args.no_validate:
        names = validateConfig(outputUri)
        log.info(f"Validated: a Butler builds from the new config, with datastores {names}")

    alias = args.alias or (f"{label}+sasquatch" if label else None)
    # a plain path reads better than the %2B-encoded file:// URL for local
    outputPath = outputUri.ospath if outputUri.isLocal else outputUri.geturl()
    print()
    print("Next steps:")
    if alias:
        print("  1. Add this to the repository index ($DAF_BUTLER_REPOSITORY_INDEX):")
        print(f'       "{alias}": "{outputPath}"')
        print(f"  2. Rapid analysis's config for this site should point at the alias {alias!r}")
    else:
        print(f"  1. Rapid analysis's config for this site should point at {outputPath}")
    print("  Publishing needs no credentials for the anonymous REST proxies at the summit sites.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
