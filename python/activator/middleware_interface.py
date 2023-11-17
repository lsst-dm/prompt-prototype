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

__all__ = ["get_central_butler", "make_local_repo", "MiddlewareInterface"]

import collections.abc
import datetime
import hashlib
import itertools
import logging
import os
import os.path
import tempfile
import typing

import astropy

from lsst.resources import ResourcePath
import lsst.afw.cameraGeom
from lsst.ctrl.mpexec import SeparablePipelineExecutor
from lsst.daf.butler import Butler, CollectionType
import lsst.geom
from lsst.meas.algorithms.htmIndexer import HtmIndexer
import lsst.obs.base
import lsst.pipe.base

from .config import PipelinesConfig
from .visit import FannedOutVisit

_log = logging.getLogger("lsst." + __name__)
_log.setLevel(logging.DEBUG)
# See https://developer.lsst.io/stack/logging.html#logger-trace-verbosity
_log_trace = logging.getLogger("TRACE1.lsst." + __name__)
_log_trace.setLevel(logging.CRITICAL)  # Turn off by default.


def get_central_butler(central_repo: str, instrument_class: str):
    """Provide a Butler that can access the given repository and read and write
    data for the given instrument.

    Parameters
    ----------
    central_repo : `str`
        The path or URI to the central repository.
    instrument_class : `str`
        The name of the instrument whose data will be retrieved or written. May
        be either the fully qualified class name or the short name.

    Returns
    -------
    butler : `lsst.daf.butler.Butler`
        A Butler for ``central_repo`` pre-configured to load and store
        ``instrument_name`` data.
    """
    # TODO: how much overhead do we take on from asking the central repo about
    # the instrument instead of handling it internally?
    registry = Butler(central_repo).registry
    instrument = lsst.obs.base.Instrument.from_string(instrument_class, registry)
    return Butler(central_repo,
                  collections=[instrument.makeUmbrellaCollectionName()],
                  writeable=True,
                  inferDefaults=False,
                  )


def make_local_repo(local_storage: str, central_butler: Butler, instrument: str):
    """Create and configure a new local repository.

    The repository is represented by a temporary directory object, which can be
    used to manage its lifetime.

    Parameters
    ----------
    local_storage : `str`
        An absolute path to a space where this function can create a local
        Butler repo.
    central_butler : `lsst.daf.butler.Butler`
        Butler repo containing instrument and skymap definitions.
    instrument : `str`
        Name of the instrument taking the data, for populating
        butler collections and dataIds. May be either the fully qualified class
        name or the short name. Examples: "LsstCam", "lsst.obs.lsst.LsstCam".

    Returns
    -------
    repo_dir
        An object of the same type as returned by `tempfile.TemporaryDirectory`,
        pointing to the local repo location.
    """
    repo_dir = tempfile.TemporaryDirectory(dir=local_storage, prefix="butler-")
    butler = Butler(Butler.makeRepo(repo_dir.name), writeable=True)
    _log.info("Created local Butler repo at %s.", repo_dir.name)

    # Run-once repository initialization

    instrument = lsst.obs.base.Instrument.from_string(instrument, central_butler.registry)
    instrument.register(butler.registry)

    butler.registry.registerCollection(instrument.makeUmbrellaCollectionName(),
                                       CollectionType.CHAINED)
    butler.registry.registerCollection(instrument.makeDefaultRawIngestRunName(),
                                       CollectionType.RUN)

    return repo_dir


# Time zone used to define exposures' day_obs value.
_DAY_OBS_TZ = datetime.timezone(datetime.timedelta(hours=-12), name="day_obs")


class MiddlewareInterface:
    """Interface layer between the Butler middleware and the prompt processing
    data handling system, to handle processing individual images.

    An instance of this class will accept an incoming group of single-detector
    snaps to process, using an instance-local butler repo. The instance can
    pre-load the necessary calibrations to process an incoming detector-visit,
    ingest the data when it is available, and run the difference imaging
    pipeline, all in that local butler.

    Each instance must be used for processing only one group-detector
    combination. The object may contain state that is unique to a particular
    processing run.

    ``MiddlewareInterface`` objects are not thread- or process-safe. It is up
    to the client to avoid conflicts from multiple objects trying to access the
    same local repo.

    Parameters
    ----------
    central_butler : `lsst.daf.butler.Butler`
        Butler repo containing the calibration and other data needed for
        processing images as they are received. This butler must be created
        with the default instrument, skymap, and collections assigned.
    image_bucket : `str`
        Storage bucket where images will be written to as they arrive.
        See also ``prefix``.
    visit : `activator.visit.FannedOutVisit`
        The visit-detector combination to be processed by this object.
    pipelines : `activator.config.PipelinesConfig`
        Information about which pipelines to run on ``visit``.
    skymap: `str`
        Name of the skymap in the central repo for querying templates.
    local_repo : `str`
        A URI to the local Butler repo, which is assumed to already exist and
        contain standard collections and the registration of ``instrument``.
    prefix : `str`, optional
        URI scheme followed by ``://``; prepended to ``image_bucket`` when
        constructing URIs to retrieve incoming files. The default is
        appropriate for use in the USDF environment; typically only
        change this when running local tests.

    Raises
    ------
    ValueError
        Raised if ``visit`` does not have equatorial coordinates and sky
        rotation angles.
    """
    _COLLECTION_SKYMAP = "skymaps"
    """The collection used for skymaps.
    """

    # Class invariants:
    # self.image_host is a valid URI with non-empty path and no query or fragment.
    # self._download_store is None if and only if self.image_host is a local URI.
    # self.visit, self.instrument, self.camera, self.skymap, self._deployment
    #   self._day_obs do not change after __init__.
    # self.butler defaults to the "defaults" chained collection, which contains
    #   all pipeline inputs. However, self.butler is not
    #   guaranteed to contain concrete data, or even the dimensions
    #   corresponding to self.camera and self.skymap. Do not assume that
    #   self.butler is the only Butler pointing to the local repo.

    def __init__(self, central_butler: Butler, image_bucket: str, visit: FannedOutVisit,
                 pipelines: PipelinesConfig, skymap: str, local_repo: str,
                 prefix: str = "s3://"):
        self.visit = visit
        if self.visit.coordinateSystem != FannedOutVisit.CoordSys.ICRS:
            raise ValueError("Only ICRS coordinates are supported in Visit, "
                             f"got {self.visit.coordinateSystem!r} instead.")
        if self.visit.rotationSystem != FannedOutVisit.RotSys.SKY:
            raise ValueError("Only sky camera rotations are supported in Visit, "
                             f"got {self.visit.rotationSystem!r} instead.")

        # Deployment/version ID -- potentially expensive to generate.
        self._deployment = self._get_deployment()
        self._apdb_uri = self._make_apdb_uri()
        self._apdb_namespace = os.environ.get("NAMESPACE_APDB", None)
        self.central_butler = central_butler
        self.image_host = prefix + image_bucket
        # TODO: _download_store turns MWI into a tagged class; clean this up later
        if not self.image_host.startswith("file"):
            self._download_store = tempfile.TemporaryDirectory(prefix="holding-")
        else:
            self._download_store = None
        # TODO: how much overhead do we pick up from going through the registry?
        self.instrument = lsst.obs.base.Instrument.from_string(visit.instrument, central_butler.registry)
        self.pipelines = pipelines

        # Guard against a processing run starting on one day and ending the next.
        self._day_obs = datetime.datetime.now(_DAY_OBS_TZ)

        self._init_local_butler(local_repo, [self.instrument.makeUmbrellaCollectionName()], None)
        self._prep_collections()
        self._init_ingester()
        self._init_visit_definer()

        # HACK: explicit collection gets around the fact that we don't have any
        # timestamp/exposure information in a form we can pass to the Butler.
        # This code will break once cameras start being versioned.
        self.camera = self.central_butler.get(
            "camera", instrument=self.instrument.getName(),
            collections=self.instrument.makeUnboundedCalibrationRunName()
        )
        self.skymap_name = skymap
        self.skymap = self.central_butler.get("skyMap", skymap=self.skymap_name,
                                              collections=self._COLLECTION_SKYMAP)

        # How much to pad the refcat region we will copy over.
        self.padding = 30*lsst.geom.arcseconds

    def _get_deployment(self):
        """Get a unique version ID of the active stack and pipeline configuration(s).

        Returns
        -------
        version : `str`
            A unique version identifier for the stack. Contains only
            word characters and "-", but not guaranteed to be human-readable.
        """
        try:
            # Defined by Knative in containers, guaranteed to be unique for
            # each deployment. Currently of the form prompt-proto-service-#####.
            return os.environ["K_REVISION"]
        except KeyError:
            # If not in a container, read the active Science Pipelines install.
            packages = lsst.utils.packages.Packages.fromSystem()  # Takes several seconds!
            h = hashlib.md5(usedforsecurity=False)
            for package, version in packages.items():
                h.update(bytes(package + version, encoding="utf-8"))
            return f"local-{h.hexdigest()}"

    def _make_apdb_uri(self):
        """Generate a URI for accessing the APDB.
        """
        return os.environ["URL_APDB"]

    def _init_local_butler(self, repo_uri: str, output_collections: list[str], output_run: str):
        """Prepare the local butler to ingest into and process from.

        ``self.butler`` is correctly initialized after this method returns.

        Parameters
        ----------
        repo_uri : `str`
            A URI to the location of the local repository.
        output_collections : `list` [`str`]
            The collection(s) in which to search for inputs and outputs.
        output_run : `str`
            The run in which to place new pipeline outputs.
        """
        # Internal Butler keeps a reference to the newly prepared collection.
        # This reference makes visible any inputs for query purposes.
        self.butler = Butler(repo_uri,
                             collections=output_collections,
                             run=output_run,
                             writeable=True,
                             )

    def _init_ingester(self):
        """Prepare the raw file ingester to receive images into this butler.

        ``self._init_local_butler`` must have already been run.
        """
        config = lsst.obs.base.RawIngestConfig()
        config.transfer = "copy"  # Copy files into the local butler.
        # TODO: Could we use the `on_ingest_failure` and `on_success` callbacks
        # to send information back to this interface?
        config.failFast = True  # We want failed ingests to fail immediately.
        self.rawIngestTask = lsst.obs.base.RawIngestTask(config=config,
                                                         butler=self.butler)

    def _init_visit_definer(self):
        """Prepare the visit definer to define visits for this butler.

        ``self._init_local_butler`` must have already been run.
        """
        define_visits_config = lsst.obs.base.DefineVisitsConfig()
        self.define_visits = lsst.obs.base.DefineVisitsTask(config=define_visits_config, butler=self.butler)

    def _predict_wcs(self, detector: lsst.afw.cameraGeom.Detector) -> lsst.afw.geom.SkyWcs:
        """Calculate the expected detector WCS for an incoming observation.

        Parameters
        ----------
        detector : `lsst.afw.cameraGeom.Detector`
            The detector for which to generate a WCS.

        Returns
        -------
        wcs : `lsst.afw.geom.SkyWcs`
            An approximate WCS for this object's visit.
        """
        boresight_center = lsst.geom.SpherePoint(
            self.visit.position[0], self.visit.position[1], lsst.geom.degrees)
        orientation = self.visit.cameraAngle * lsst.geom.degrees

        flip_x = True if self.instrument.getName() == "DECam" else False
        return lsst.obs.base.createInitialSkyWcsFromBoresight(boresight_center,
                                                              orientation,
                                                              detector,
                                                              flipX=flip_x)

    def _detector_bounding_circle(self, detector: lsst.afw.cameraGeom.Detector,
                                  wcs: lsst.afw.geom.SkyWcs
                                  ) -> (lsst.geom.SpherePoint, lsst.geom.Angle):
        # Could return a sphgeom.Circle, but that would require a lot of
        # sphgeom->geom conversions downstream. Even their Angles are different!
        """Compute a small sky circle that contains the detector.

        Parameters
        ----------
        detector : `lsst.afw.cameraGeom.Detector`
            The detector for which to compute an on-sky bounding circle.
        wcs : `lsst.afw.geom.SkyWcs`
            The conversion from detector to sky coordinates.

        Returns
        -------
        center : `lsst.geom.SpherePoint`
            The center of the bounding circle.
        radius : `lsst.geom.Angle`
            The opening angle of the bounding circle.
        """
        radii = []
        center = wcs.pixelToSky(detector.getCenter(lsst.afw.cameraGeom.PIXELS))
        for corner in detector.getCorners(lsst.afw.cameraGeom.PIXELS):
            radii.append(wcs.pixelToSky(corner).separation(center))
        return center, max(radii)

    def prep_butler(self) -> None:
        """Prepare a temporary butler repo for processing the incoming data.

        After this method returns, the internal butler is guaranteed to contain
        all data and all dimensions needed to run the appropriate pipeline on
        this object's visit, except for ``raw`` and the ``exposure`` and
        ``visit`` dimensions, respectively. It may contain other data that would
        not be loaded when processing the visit.
        """
        _log.info(f"Preparing Butler for visit {self.visit!r}")

        detector = self.camera[self.visit.detector]
        wcs = self._predict_wcs(detector)
        center, radius = self._detector_bounding_circle(detector, wcs)

        # repos may have been modified by other MWI instances.
        # TODO: get a proper synchronization API for Butler
        self.central_butler.registry.refresh()
        self.butler.registry.refresh()

        refcat_datasets = list(self._export_refcats(center, radius))
        template_datasets = list(self._export_skymap_and_templates(center, detector, wcs, self.visit.filters))
        calib_datasets = list(self._export_calibs(self.visit.detector, self.visit.filters))
        self.butler.transfer_from(self.central_butler,
                                  refcat_datasets + template_datasets + calib_datasets,
                                  transfer="copy",
                                  skip_missing=True,
                                  register_dataset_types=True,
                                  transfer_dimensions=True,
                                  )

        self._export_collections(self._get_template_collection())
        self._export_collections(self.instrument.makeUmbrellaCollectionName())
        self._export_calib_associations(self.instrument.makeCalibrationCollectionName(), calib_datasets)

        # Temporary workarounds until we have a prompt-processing default top-level collection
        # in shared repos, and raw collection in dev repo, and then we can organize collections
        # without worrying about DRP use cases.
        _prepend_collection(self.butler,
                            self.instrument.makeUmbrellaCollectionName(),
                            [self._get_template_collection(),
                             self.instrument.makeDefaultRawIngestRunName(),
                             ])

    def _get_template_collection(self):
        """Get the collection name for templates
        """
        return self.instrument.makeCollectionName("templates")

    def _export_refcats(self, center, radius):
        """Identify the refcats to export from the central butler.

        Parameters
        ----------
        center : `lsst.geom.SpherePoint`
            Center of the region to find refcat shards in.
        radius : `lst.geom.Angle`
            Radius to search for refcat shards in.

        Returns
        -------
        refcats : iterable [`DatasetRef`]
            The refcats to be exported, after any filtering.
        """
        indexer = HtmIndexer(depth=7)
        shard_ids, _ = indexer.getShardIds(center, radius+self.padding)
        htm_where = f"htm7 in ({','.join(str(x) for x in shard_ids)})"
        # Get shards from all refcats that overlap this detector.
        # TODO: `...` doesn't work for this queryDatasets call
        # currently, and we can't queryDatasetTypes in just the refcats
        # collection, so we have to specify a list here. Replace this
        # with another solution ASAP.
        possible_refcats = ["gaia", "panstarrs", "gaia_dr2_20200414", "ps1_pv3_3pi_20170110",
                            "atlas_refcat2_20220201", "gaia_dr3_20230707"]
        _log.debug("Searching for refcats in %s...", shard_ids)
        refcats = set(_filter_datasets(
                      self.central_butler, self.butler,
                      possible_refcats,
                      collections=self.instrument.makeRefCatCollectionName(),
                      where=htm_where,
                      findFirst=True))
        _log.debug("Found %d new refcat datasets.", len(refcats))
        return refcats

    def _export_skymap_and_templates(self, center, detector, wcs, filter):
        """Identify the skymap and templates to export from the central butler.

        Parameters
        ----------
        center : `lsst.geom.SpherePoint`
            Center of the region to load the skyamp tract/patches for.
        detector : `lsst.afw.cameraGeom.Detector`
            Detector we are loading data for.
        wcs : `lsst.afw.geom.SkyWcs`
            Rough WCS for the upcoming visit, to help finding patches.
        filter : `str`
            Physical filter for which to export templates.

        Returns
        -------
        skymapTemplates : iterable [`DatasetRef`]
            The datasets to be exported, after any filtering.
        """
        # TODO: This exports the whole skymap, but we want to only export the
        # subset of the skymap that covers this data.
        skymaps = set(_filter_datasets(
            self.central_butler, self.butler,
            "skyMap",
            skymap=self.skymap_name,
            collections=self._COLLECTION_SKYMAP,
            findFirst=True))
        _log.debug("Found %d new skymap datasets.", len(skymaps))
        # Getting only one tract should be safe: we're getting the
        # tract closest to this detector, so we should be well within
        # the tract bbox.
        tract = self.skymap.findTract(center)
        points = [center]
        for corner in detector.getCorners(lsst.afw.cameraGeom.PIXELS):
            points.append(wcs.pixelToSky(corner))
        patches = tract.findPatchList(points)
        patches_str = ','.join(str(p.sequential_index) for p in patches)
        template_where = f"patch in ({patches_str}) and tract={tract.tract_id} and physical_filter='{filter}'"
        # TODO: do we need to have the coadd name used in the pipeline
        # specified as a class kwarg, so that we only load one here?
        # TODO: alternately, we need to extract it from the pipeline? (best?)
        try:
            _log.debug("Searching for templates in tract %d, patches %s...", tract.tract_id, patches_str)
            templates = set(_filter_datasets(
                self.central_butler, self.butler,
                "*Coadd",
                collections=self._get_template_collection(),
                instrument=self.instrument.getName(),
                skymap=self.skymap_name,
                where=template_where,
                findFirst=True))
        except _MissingDatasetError as err:
            _log.error(err)
            templates = set()
        else:
            _log.debug("Found %d new template datasets.", len(templates))
        return skymaps | templates

    def _export_calibs(self, detector_id, filter):
        """Identify the calibs to export from the central butler.

        Parameters
        ----------
        detector_id : `int`
            Identifier of the detector to load calibs for.
        filter : `str`
            Physical filter name of the upcoming visit.

        Returns
        -------
        calibs : iterable [`DatasetRef`]
            The calibs to be exported, after any filtering.
        """
        # TODO: we can't filter by validity range because it's not
        # supported in queryDatasets yet.
        calib_where = f"detector={detector_id} and physical_filter='{filter}'"
        # private_sndStamp is in TAI, not UTC, but difference shouldn't matter
        calib_date = datetime.datetime.fromtimestamp(self.visit.private_sndStamp, tz=datetime.timezone.utc)
        # TODO: we can't use findFirst=True yet because findFirst query
        # in CALIBRATION-type collection is not supported currently.
        calibs = set(_filter_datasets(
            self.central_butler, self.butler,
            ...,
            collections=self.instrument.makeCalibrationCollectionName(),
            instrument=self.instrument.getName(),
            where=calib_where,
            calib_date=calib_date,
        ))
        if calibs:
            for dataset_type, n_datasets in self._count_by_type(calibs):
                _log.debug("Found %d new calib datasets of type '%s'.", n_datasets, dataset_type)
        else:
            _log.debug("Found 0 new calib datasets.")
        return calibs

    def _export_collections(self, collection):
        """Export the collection and all its children.

        This preserves the collection structure even if some child collections
        do not have data. Exporting a collection does not export its datasets.

        Parameters
        ----------
        collection : `str`
            The collection to be exported. It is usually a CHAINED collection
            and can have many children.
        """
        src = self.central_butler.registry
        dest = self.butler.registry

        # Store collection chains after all children guaranteed to exist
        chains = {}
        for child in src.queryCollections(collection, flattenChains=True, includeChains=True):
            if src.getCollectionType(child) == CollectionType.CHAINED:
                chains[child] = src.getCollectionChain(child)
            dest.registerCollection(child,
                                    src.getCollectionType(child),
                                    src.getCollectionDocumentation(child))
        for chain, children in chains.items():
            dest.setCollectionChain(chain, children)

    def _export_calib_associations(self, calib_collection, datasets):
        """Export the associations between a set of datasets and a
        calibration collection.

        Parameters
        ----------
        calib_collection : `str`
            The calibration collection, or a chain thereof, containing the
            associations. The collection and any children must exist in both
            the central and local repos.
        datasets : iterable [`lsst.daf.butler.DatasetRef']
            The calib datasets whose associations must be exported. Must be
            certified in ``calib_collection`` in the central repo, and must
            exist in the local repo.
        """
        dataset_types = {ref.datasetType for ref in datasets}
        associations = {}
        for dataset_type in dataset_types:
            associations.update(
                (a.ref, a) for a in self.central_butler.registry.queryDatasetAssociations(
                    dataset_type,
                    calib_collection,
                    collectionTypes={CollectionType.CALIBRATION},
                    flattenChains=True
                )
            )
        for dataset in datasets:
            association = associations[dataset]
            # certify is designed to work on groups of datasets; in practice,
            # the total number of calibs (~1 of each type) is small enough that
            # grouping by timespan isn't worth it.
            self.butler.registry.certify(association.collection, [dataset], association.timespan)

    @staticmethod
    def _count_by_type(refs):
        """Count the number of dataset references of each type.

        Parameters
        ----------
        refs : iterable [`lsst.daf.butler.DatasetRef`]
            The references to classify.

        Yields
        ------
        type : `str`
            The name of a dataset type in ``refs``.
        count : `int`
            The number of elements of type ``type`` in ``refs``.
        """
        def get_key(ref):
            return ref.datasetType.name

        ordered = sorted(refs, key=get_key)
        for k, g in itertools.groupby(ordered, key=get_key):
            yield k, len(list(g))

    def _get_init_output_run(self,
                             pipeline_file: str,
                             date: datetime.date | None = None) -> str:
        """Generate a deterministic init-output collection name that avoids
        configuration conflicts.

        Parameters
        ----------
        pipeline_file : `str`
            The pipeline file that the run will be used for.
        date : `datetime.date`, optional
            Date of the processing run (not observation!), defaults to the
            day_obs this method was called.

        Returns
        -------
        run : `str`
            The run in which to place pipeline init-outputs.
        """
        if date is None:
            date = datetime.datetime.now(_DAY_OBS_TZ)
        # Current executor requires that init-outputs be in the same run as
        # outputs. This can be changed once DM-36162 is done.
        return self._get_output_run(pipeline_file, date)

    def _get_output_run(self,
                        pipeline_file: str,
                        date: datetime.date | None = None) -> str:
        """Generate a deterministic collection name that avoids version or
        provenance conflicts.

        Parameters
        ----------
        pipeline_file : `str`
            The pipeline file that the run will be used for.
        date : `datetime.date`, optional
            Date of the processing run (not observation!), defaults to the
            day_obs this method was called.

        Returns
        -------
        run : `str`
            The run in which to place processing outputs.
        """
        if date is None:
            date = datetime.datetime.now(_DAY_OBS_TZ)
        pipeline_name, _ = os.path.splitext(os.path.basename(pipeline_file))
        # Order optimized for S3 bucket -- filter out as many files as soon as possible.
        return self.instrument.makeCollectionName(
            "prompt", f"output-{date:%Y-%m-%d}", pipeline_name, self._deployment)

    def _prep_collections(self):
        """Pre-register output collections in advance of running the pipeline.
        """
        self.butler.registry.refresh()
        for pipeline_file in self._get_pipeline_files():
            self.butler.registry.registerCollection(
                self._get_init_output_run(pipeline_file, self._day_obs),
                CollectionType.RUN)
            self.butler.registry.registerCollection(
                self._get_output_run(pipeline_file, self._day_obs),
                CollectionType.RUN)

    def _get_pipeline_files(self) -> str:
        """Identify the pipelines to be run, based on the configured instrument
        and visit.

        Returns
        -------
        pipelines : sequence [`str`]
            A sequence of paths to a configured pipeline file, in order from
            most preferred to least preferred.
        """
        return self.pipelines.get_pipeline_files(self.visit)

    def _prep_pipeline(self, pipeline_file) -> lsst.pipe.base.Pipeline:
        """Setup the pipeline to be run, based on the configured instrument and
        details of the incoming visit.

        Parameters
        ----------
        pipeline_file : `str`
            The pipeline file to run.

        Returns
        -------
        pipeline : `lsst.pipe.base.Pipeline`
            The fully configured pipeline.
        """
        pipeline = lsst.pipe.base.Pipeline.fromFile(pipeline_file)

        try:
            pipeline.addConfigOverride("diaPipe", "apdb.db_url", self._apdb_uri)
            pipeline.addConfigOverride("diaPipe", "apdb.namespace", self._apdb_namespace)
        except LookupError:
            _log.debug("diaPipe is not in this pipeline.")
        return pipeline

    def _download(self, remote):
        """Download an image located on a remote store.

        Parameters
        ----------
        remote : `lsst.resources.ResourcePath`
            The location from which to download the file. Must not be a
            file:// URI.

        Returns
        -------
        local : `lsst.resources.ResourcePath`
            The location to which the file has been downloaded.
        """
        local = ResourcePath(os.path.join(self._download_store.name, remote.basename()))
        # TODO: this requires the service account to have the otherwise admin-ish
        # storage.buckets.get permission (DM-34188). Once that's resolved, see if
        # prompt-service can do without the "Storage Legacy Bucket Reader" role.
        local.transfer_from(remote, "copy")
        return local

    def ingest_image(self, oid: str) -> None:
        """Ingest an image into the temporary butler.

        The temporary butler must not already contain a ``raw`` dataset
        corresponding to ``oid``. After this method returns, the temporary
        butler contains one ``raw`` dataset corresponding to ``oid``, and the
        appropriate ``exposure`` dimension.

        Parameters
        ----------
        oid : `str`
            Identifier for incoming image, relative to the image bucket.
        """
        # TODO: consider allowing pre-existing raws, as may happen when a
        # pipeline is rerun (see DM-34141).
        _log.info(f"Ingesting image id '{oid}'")
        file = ResourcePath(f"{self.image_host}/{oid}")
        if not file.isLocal:
            # TODO: RawIngestTask doesn't currently support remote files.
            file = self._download(file)
        result = self.rawIngestTask.run([file])
        # We only ingest one image at a time.
        # TODO: replace this assert with a custom exception, once we've decided
        # how we plan to handle exceptions in this code.
        assert len(result) == 1, "Should have ingested exactly one image."
        _log.info("Ingested one %s with dataId=%s", result[0].datasetType.name, result[0].dataId)

    def run_pipeline(self, exposure_ids: set[int]) -> None:
        """Process the received image(s).

        The internal butler must contain all data and all dimensions needed to
        run the appropriate pipeline on this visit and ``exposure_ids``, except
        for the ``visit`` dimension itself.

        Parameters
        ----------
        exposure_ids : `set` [`int`]
            Identifiers of the exposures that were received.

        Raises
        ------
        RuntimeError
            Raised if the pipeline could not be loaded or configured, or the
            graph generated.
            TODO: could be a good case for a custom exception here.
        """
        # TODO: we want to define visits earlier, but we have to ingest a
        # faked raw file and appropriate SSO data during prep (and then
        # cleanup when ingesting the real data).
        try:
            self.define_visits.run({"instrument": self.instrument.getName(),
                                    "exposure": exp} for exp in exposure_ids)
        except lsst.daf.butler.registry.DataIdError as e:
            # TODO: a good place for a custom exception?
            raise RuntimeError("No data to process.") from e

        where = (
            f"instrument='{self.visit.instrument}' and detector={self.visit.detector}"
            f" and exposure in ({','.join(str(x) for x in exposure_ids)})"
            " and visit_system = 0"
        )
        # Try pipelines in order until one works.
        for pipeline_file in self._get_pipeline_files():
            try:
                pipeline = self._prep_pipeline(pipeline_file)
            except FileNotFoundError as e:
                raise RuntimeError from e
            init_output_run = self._get_init_output_run(pipeline_file, self._day_obs)
            output_run = self._get_output_run(pipeline_file, self._day_obs)
            executor = SeparablePipelineExecutor(
                Butler(butler=self.butler,
                       collections=[output_run, init_output_run] + list(self.butler.collections),
                       run=output_run),
                clobber_output=False,
                skip_existing_in=None
            )
            qgraph = executor.make_quantum_graph(pipeline, where=where)
            if len(qgraph) == 0:
                # Diagnostic logs are the responsibility of GraphBuilder.
                _log.error(f"Could not build quantum graph for {pipeline_file}; "
                           "see previous logs for details.")
                continue
            # Past this point, partial execution creates datasets.
            # Don't retry -- either fail (raise) or break.

            # If this is a fresh (local) repo, then types like calexp,
            # *Diff_diaSrcTable, etc. have not been registered.
            # TODO: after DM-38041, move pre-execution to one-time repo setup.
            executor.pre_execute_qgraph(qgraph, register_dataset_types=True, save_init_outputs=True)
            _log.info(f"Running '{pipeline._pipelineIR.description}' on {where}")
            executor.run_pipeline(qgraph)
            _log.info("Pipeline successfully run.")
            break
        else:
            # TODO: a good place for a custom exception?
            raise RuntimeError("No pipeline graph could be built.")

    def export_outputs(self, exposure_ids: set[int]) -> None:
        """Copy pipeline outputs from processing a set of images back
        to the central Butler.

        Parameters
        ----------
        exposure_ids : `set` [`int`]
            Identifiers of the exposures that were processed.
        """
        exported = False
        # Rather than determining which pipeline was run, just try to export all of them.
        for pipeline_file in self._get_pipeline_files():
            output_run = self._get_output_run(pipeline_file, self._day_obs)
            exports = self._export_subset(exposure_ids,
                                          # TODO: find a way to merge datasets like *_config
                                          # or *_schema that are duplicated across multiple
                                          # workers.
                                          self._get_safe_dataset_types(self.butler),
                                          in_collections=output_run,
                                          )
            if exports:
                exported = True
                _log.info(f"Pipeline products saved to collection '{output_run}'.")
        if not exported:
            raise ValueError(f"No datasets match visit={self.visit} and exposures={exposure_ids}.")

    @staticmethod
    def _get_safe_dataset_types(butler):
        """Return the set of dataset types that can be safely merged from a worker.

        Parameters
        ----------
        butler : `lsst.daf.butler.Butler`
            The butler in which to search for dataset types.

        Returns
        -------
        types : iterable [`str`]
            The dataset types to return.
        """
        return [dstype for dstype in butler.registry.queryDatasetTypes(...)
                if "detector" in dstype.dimensions]

    def _export_subset(self, exposure_ids: set[int],
                       dataset_types: typing.Any, in_collections: typing.Any) -> None:
        """Copy datasets associated with a processing run back to the
        central Butler.

        Parameters
        ----------
        exposure_ids : `set` [`int`]
            Identifiers of the exposures that were processed.
        dataset_types
            The dataset type(s) to transfer; can be any expression described in
            :ref:`daf_butler_dataset_type_expressions`.
        in_collections
            The collections to transfer from; can be any expression described
            in :ref:`daf_butler_collection_expressions`.

        Returns
        -------
        datasets : collection [`~lsst.daf.butler.DatasetRef`]
            The datasets exported (may be empty).
        """
        # local repo may have been modified by other MWI instances.
        self.butler.registry.refresh()

        try:
            # Need to iterate over datasets at least twice, so list.
            datasets = list(self.butler.registry.queryDatasets(
                dataset_types,
                collections=in_collections,
                # in_collections may include other runs, so need to filter.
                # Since AP processing is strictly visit-detector, these three
                # dimensions should suffice.
                # DO NOT assume that visit == exposure!
                where="exposure in (exposure_ids)",
                bind={"exposure_ids": exposure_ids},
                instrument=self.instrument.getName(),
                detector=self.visit.detector,
            ).expanded())
        except lsst.daf.butler.registry.DataIdError as e:
            raise ValueError("Invalid visit or exposures.") from e

        # central repo may have been modified by other MWI instances.
        # TODO: get a proper synchronization API for Butler
        self.central_butler.registry.refresh()

        # Transferring governor dimensions in parallel can cause deadlocks in
        # central registry. We need to transfer our exposure/visit dimensions,
        # so handle those manually.
        for dimension in ["exposure",
                          "visit",
                          "visit_definition",
                          "visit_detector_region",
                          "visit_system",
                          "visit_system_membership",
                          ]:
            for record in self.butler.registry.queryDimensionRecords(
                dimension,
                where="exposure in (exposure_ids)",
                bind={"exposure_ids": exposure_ids},
                instrument=self.instrument.getName(),
                detector=self.visit.detector,
            ):
                self.central_butler.registry.syncDimensionData(dimension, record, update=False)
        self.central_butler.transfer_from(self.butler, datasets, transfer="copy", transfer_dimensions=False)

        return datasets

    def clean_local_repo(self, exposure_ids: set[int]) -> None:
        """Remove local repo content that is only needed for a single visit.

        This includes raws and pipeline outputs.

        Parameter
        ---------
        exposure_ids : `set` [`int`]
            Identifiers of the exposures to be removed.
        """
        self.butler.registry.refresh()
        raws = self.butler.registry.queryDatasets(
            'raw',
            collections=self.instrument.makeDefaultRawIngestRunName(),
            where=f"exposure in ({', '.join(str(x) for x in exposure_ids)})",
            instrument=self.visit.instrument,
            detector=self.visit.detector,
        )
        self.butler.pruneDatasets(raws, disassociate=True, unstore=True, purge=True)
        # Outputs are all in their own runs, so just drop them.
        for pipeline_file in self._get_pipeline_files():
            output_run = self._get_output_run(pipeline_file, self._day_obs)
            for chain in self.butler.registry.getCollectionParentChains(output_run):
                _remove_from_chain(self.butler, chain, [output_run])
            self.butler.removeRuns([output_run], unstore=True)


class _MissingDatasetError(RuntimeError):
    """An exception flagging that required datasets were not found
    where expected.
    """
    pass


def _filter_datasets(src_repo: Butler,
                     dest_repo: Butler,
                     *args,
                     calib_date: datetime.datetime | None = None,
                     **kwargs) -> collections.abc.Iterable[lsst.daf.butler.DatasetRef]:
    """Identify datasets in a source repository, filtering out those already
    present in a destination.

    Unlike Butler or database queries, this method raises if nothing in the
    source repository matches the query criteria.

    Parameters
    ----------
    src_repo : `lsst.daf.butler.Butler`
        The repository in which a dataset must be present.
    dest_repo : `lsst.daf.butler.Butler`
        The repository in which a dataset must not be present.
    calib_date : `datetime.datetime`, optional
        If provided, also filter anything other than calibs valid at
        ``calib_date`` and check that at least one valid calib was found.
        Any ``datetime`` object must be aware.
    *args, **kwargs
        Parameters for describing the dataset query. They have the same
        meanings as the parameters of `lsst.daf.butler.Registry.queryDatasets`.
        The query must be valid for both ``src_repo`` and ``dest_repo``.

    Returns
    -------
    datasets : iterable [`lsst.daf.butler.DatasetRef`]
        The datasets that exist in ``src_repo`` but not ``dest_repo``. All
        datasetRefs are fully expanded.

    Raises
    ------
    _MissingDatasetError
        Raised if the query on ``src_repo`` failed to find any datasets, or
        (if ``calib_date`` is set) if none of them are currently valid.
    """
    try:
        known_datasets = set(dest_repo.registry.queryDatasets(*args, **kwargs))
    except lsst.daf.butler.registry.DataIdValueError as e:
        _log.debug("Pre-export query with args '%s, %s' failed with %s",
                   ", ".join(repr(a) for a in args),
                   ", ".join(f"{k}={v!r}" for k, v in kwargs.items()),
                   e)
        # If dimensions are invalid, then *any* such datasets are missing.
        known_datasets = set()

    # Let exceptions from src_repo query raise: if it fails, that invalidates
    # this operation.
    # "expanded" dimension records are ignored for DataCoordinate equality
    # comparison, so we only need them on src_datasets.
    src_datasets = set(src_repo.registry.queryDatasets(*args, **kwargs).expanded())
    if calib_date:
        src_datasets = _filter_calibs_by_date(
            src_repo,
            kwargs["collections"] if "collections" in kwargs else ...,
            src_datasets,
            calib_date,
        )
    if not src_datasets:
        raise _MissingDatasetError(
            "Source repo query with args '{}, {}' found no matches.".format(
                ", ".join(repr(a) for a in args),
                ", ".join(f"{k}={v!r}" for k, v in kwargs.items())
            )
        )
    return itertools.filterfalse(lambda ref: ref in known_datasets, src_datasets)


def _prepend_collection(butler: Butler, chain: str, new_collections: collections.abc.Iterable[str]) -> None:
    """Add a specific collection to the front of an existing chain.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        The butler in which the collections exist.
    chain : `str`
        The chained collection to prepend to.
    new_collections : sequence [`str`]
        The collections to prepend to ``chain``, in order.

    Notes
    -----
    This function is not safe against concurrent modifications to ``chain``.
    """
    old_chain = butler.registry.getCollectionChain(chain)  # May be empty
    butler.registry.setCollectionChain(chain, list(new_collections) + list(old_chain), flatten=False)


def _remove_from_chain(butler: Butler, chain: str, old_collections: collections.abc.Iterable[str]) -> None:
    """Remove a specific collection from a chain.

    This function has no effect if the collection is not in the chain.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        The butler in which the collections exist.
    chain : `str`
        The chained collection to remove from.
    old_collections : iterable [`str`]
        The collections to remove from ``chain``.

    Notes
    -----
    This function is not safe against concurrent modifications to ``chain``.
    """
    contents = list(butler.registry.getCollectionChain(chain))
    for old in set(old_collections).intersection(contents):
        contents.remove(old)
    butler.registry.setCollectionChain(chain, contents, flatten=False)


def _filter_calibs_by_date(butler: Butler,
                           collections: typing.Any,
                           unfiltered_calibs: collections.abc.Collection[lsst.daf.butler.DatasetRef],
                           date: datetime.datetime
                           ) -> collections.abc.Iterable[lsst.daf.butler.DatasetRef]:
    """Trim a set of calib datasets to those that are valid at a particular time.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        The Butler to query for validity data.
    collections : collection expression
        The calibration collection(s), or chain(s) containing calibration
        collections, to query for validity data.
    unfiltered_calibs : collection [`lsst.daf.butler.DatasetRef`]
        The calibs to be filtered by validity. May be empty.
    date : `datetime.datetime`
        The time at which the calibs must be valid. Must be an
        aware ``datetime``.

    Returns
    -------
    filtered_calibs : iterable [`lsst.daf.butler.DatasetRef`]
        The subset of ``unfiltered_calibs`` that is valid on ``date``.
    """
    dataset_types = {ref.datasetType for ref in unfiltered_calibs}
    associations = {}
    for dataset_type in dataset_types:
        associations.update(
            (a.ref, a) for a in butler.registry.queryDatasetAssociations(
                dataset_type, collections, collectionTypes={CollectionType.CALIBRATION}, flattenChains=True
            )
        )

    t = astropy.time.Time(date, scale='utc')
    _log_trace.debug("Looking up calibs for %s in %s.", t, collections)
    # DatasetAssociation.timespan guaranteed not None
    filtered_calibs = []
    for ref in unfiltered_calibs:
        if ref in associations:
            if associations[ref].timespan.contains(t):
                filtered_calibs.append(ref)
                _log_trace.debug("%s (valid over %s) matches %s.", ref, associations[ref].timespan, t)
            else:
                _log_trace.debug("%s (valid over %s) does not match %s.", ref, associations[ref].timespan, t)
        else:
            _log_trace.debug("No calib associations for %s.", ref)
    return filtered_calibs
