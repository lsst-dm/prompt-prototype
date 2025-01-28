#!/usr/bin/env python
# This file is part of prompt_processing.
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


"""Selectively export some contents from a butler repo.

This script selects some data in a source butler repo, and makes an export
file for importing to the test central prompt processing repository.
"""


import argparse
import logging
import sys
import tempfile
import yaml

import lsst.daf.butler as daf_butler
from lsst.utils.timer import time_this

from activator.middleware_interface import _filter_datasets, _generic_query


def _make_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--src-repo",
        required=True,
        help="The location of the repository from which datasets are exported.",
    )
    parser.add_argument(
        "--target-repo",
        required=False,
        help="The location of the repository to which datasets are exported. "
             "Datasets already existing in the target repo will not be "
             "exported from the source repo. If no target repo is given, all "
             "selected datasets in the source repo will be exported.",
    )
    parser.add_argument(
        "--select",
        required=True,
        help="URI to a YAML file containing expressions to identify the "
             "datasets and collections to be exported. An example is at "
             "etc/export_latiss.yaml."
    )
    return parser


def main():
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)

    args = _make_parser().parse_args()
    src_butler = daf_butler.Butler(args.src_repo)
    with open(args.select, "r") as file:
        wants = yaml.safe_load(file)

    with tempfile.TemporaryDirectory() as temp_repo:
        if args.target_repo:
            target_butler = daf_butler.Butler(args.target_repo, writeable=False)
        else:
            # If no target_butler is given, create an empty one.
            config = daf_butler.Butler.makeRepo(temp_repo)
            target_butler = daf_butler.Butler(config)

        with time_this(msg="Datasets and collections exported", level=logging.INFO):
            _export_for_copy(src_butler, target_butler, wants)


def _export_for_copy(butler, target_butler, wants):
    """Export selected data to make copies in another butler repo.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        The source Butler from which datasets are exported.
    target_butler : `lsst.daf.butler.Butler`
        The target Butler to which datasets are exported. It is checked
        to avoid exporting existing datasets. No checks are done to
        verify if datasets are really identical.
    wants : `dict`
        A dictionary to identify selections with optional keys:

        ``"datasets"``, optional
            A list of dataset selection expressions (`list` of `dict`).
            The list is iterated over to find matching datasets in the butler,
            with the matching criteria provided via the selection expressions.
            Each selection expression has the keyworded argument dictionary to
            be passed to butler to query datasets; it has the same meanings
            as the parameters of `lsst.daf.butler.Registry.queryDatasets`.
        ``"collections"``, optional
            A list of collection selection expressions (`list` of `dict`).
            The list is iterated over to find matching collections in the butler,
            with the matching criteria provided via the selection expressions.
            Each selection expression has the keyworded argument dictionary to
            be passed to butler to query collectionss; it has the same meanings
            as the parameters of `lsst.daf.butler.Registry.queryCollections`.
    """
    with butler.export(format="yaml") as contents:
        if "datasets" in wants:
            for selection in wants["datasets"]:
                logging.debug(f"Selecting datasets: {selection}")
                if "collections" not in selection:
                    raise RuntimeError("Must provide collections to select datasets.")
                if "datasetType" in selection:
                    dataset_types = [selection.pop("datasetType")]
                else:
                    # TODO: A new query API after DM-45873 may replace or improve this usage.
                    all_types = {t.name for t in butler.registry.queryDatasetTypes()}
                    collections_info = butler.collections.query_info(
                        selection["collections"], include_summary=True
                    )
                    dataset_types = butler.collections._filter_dataset_types(
                        all_types, collections_info
                    )
                records = _filter_datasets(
                    butler, target_butler, _generic_query(dataset_types, **selection)
                )
                contents.saveDatasets(records)

        # Save selected collections and chains
        if "collections" in wants:
            for selection in wants["collections"]:
                for collection in butler.registry.queryCollections(**selection):
                    logging.debug(f"Selecting collection {collection}")
                    try:
                        target_butler.registry.queryCollections(collection)
                    except daf_butler.registry.MissingCollectionError:
                        # MissingCollectionError is raised if the collection does not exist in target_butler.
                        contents.saveCollection(collection)


if __name__ == "__main__":
    main()
