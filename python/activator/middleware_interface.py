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
import hashlib
import itertools
import logging
import os
import os.path
import tempfile
import typing

import astropy
import sqlalchemy

import lsst.utils.timer
from lsst.resources import ResourcePath
import lsst.sphgeom
import lsst.afw.cameraGeom
import lsst.ctrl.mpexec
from lsst.ctrl.mpexec import SeparablePipelineExecutor, SingleQuantumExecutor, MPGraphExecutor
from lsst.daf.butler import Butler, CollectionType, DatasetType, Timespan
from lsst.daf.butler.registry import MissingDatasetTypeError
import lsst.dax.apdb
import lsst.geom
from lsst.meas.algorithms.htmIndexer import HtmIndexer
import lsst.obs.base
import lsst.pipe.base
import lsst.analysis.tools
from lsst.analysis.tools.interfaces.datastore import SasquatchDispatcher  # Can't use fully-qualified name

from .config import PipelinesConfig
from .exception import NonRetriableError
from .visit import FannedOutVisit
from .timer import enforce_schema, time_this_to_bundle

_log = logging.getLogger("lsst." + __name__)
_log.setLevel(logging.DEBUG)
# See https://developer.lsst.io/stack/logging.html#logger-trace-verbosity
_log_trace = logging.getLogger("TRACE1.lsst." + __name__)
_log_trace.setLevel(logging.CRITICAL)  # Turn off by default.
_log_trace3 = logging.getLogger("TRACE3.lsst." + __name__)
_log_trace3.setLevel(logging.CRITICAL)  # Turn off by default.

# VALIDITY-HACK: local-only calib collections break if calibs are not requested
# in chronological order. Turn off for large development runs.
cache_calibs = bool(int(os.environ.get("DEBUG_CACHE_CALIBS", '1')))
# TODO: workaround for DM-40193
cache_anything = bool(int(os.environ.get("DEBUG_CACHE_INPUTS", '1')))


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
    dimension_config = central_butler.dimensions.dimensionConfig
    repo_dir = tempfile.TemporaryDirectory(dir=local_storage, prefix="butler-")
    butler = Butler(Butler.makeRepo(repo_dir.name, dimensionConfig=dimension_config), writeable=True)
    _log.info("Created local Butler repo at %s with dimensions-config %s %d.",
              repo_dir.name, dimension_config["namespace"], dimension_config["version"])

    # Run-once repository initialization

    instrument = lsst.obs.base.Instrument.from_string(instrument, central_butler.registry)
    instrument.register(butler.registry)

    butler.registry.registerCollection(instrument.makeUmbrellaCollectionName(),
                                       CollectionType.CHAINED)
    butler.registry.registerCollection(instrument.makeDefaultRawIngestRunName(),
                                       CollectionType.RUN)

    return repo_dir


def _get_sasquatch_dispatcher():
    """Get a SasquatchDispatcher object ready for use by Prompt Processing.

    Returns
    -------
    dispatcher : `lsst.analysis.tools.interfaces.datastore.SasquatchDispatcher` \
            or `None`
        The object to handle all Sasquatch uploads from this module. If `None`,
        the service is not configured to use Sasquatch.
    """
    url = os.environ.get("SASQUATCH_URL", "")
    if not url:
        return None
    token = os.environ.get("SASQUATCH_TOKEN", "")
    namespace = os.environ.get("DAF_BUTLER_SASQUATCH_NAMESPACE", "lsst.prompt")
    return SasquatchDispatcher(url=url, token=token, namespace=namespace)


# Offset used to define exposures' day_obs value.
_DAY_OBS_DELTA = astropy.time.TimeDelta(-12.0 * astropy.units.hour, scale="tai")


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
    DATASET_IDENTIFIER = "Live"
    """The dataset ID used for Sasquatch uploads.
    """

    _collection_skymap = "skymaps"
    """The collection used for skymaps.
    """
    _collection_ml_model = "pretrained_models"
    """The collection used for machine learning models.
    """

    @property
    def _collection_template(self):
        """The collection used for templates.

        This collection depends on initialization parameters, and must
        not be called from this object's constructor.
        """
        return self.instrument.makeCollectionName("templates")

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
        self._apdb_config = os.environ["CONFIG_APDB"]
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

        self._day_obs = (astropy.time.Time.now() + _DAY_OBS_DELTA).tai.to_value("iso", "date")

        self._init_local_butler(local_repo, [self.instrument.makeUmbrellaCollectionName()], None)
        self._prep_collections()
        self._define_dimensions()
        self._init_ingester()
        self._init_visit_definer()

        # TODO: can replace this with find_dataset on DM-42825
        try:
            self.camera = self.central_butler.get(
                "camera",
                instrument=self.instrument.getName(),
                collections=self.instrument.makeCalibrationCollectionName(),
            )
        except lsst.daf.butler.DatasetNotFoundError:
            # Repos that don't allow retrieval of camera from <instrument>/calibs
            # have <instrument>/calibs/unbounded.
            self.camera = self.central_butler.get(
                "camera",
                instrument=self.instrument.getName(),
                collections=self.instrument.makeUnboundedCalibrationRunName(),
            )
        self.skymap_name = skymap
        self.skymap = self.central_butler.get("skyMap", skymap=self.skymap_name,
                                              collections=self._collection_skymap)

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

    def _define_dimensions(self):
        """Define any dimensions that must be computed from this object's visit.

        ``self._init_local_butler`` must have already been run.
        """
        self.butler.registry.syncDimensionData("group",
                                               {"name": self.visit.groupId,
                                                "instrument": self.instrument.getName(),
                                                })

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
        action_id = "prepButlerTimeMetric"  # For consistency with analysis_tools outputs
        bundle = lsst.analysis.tools.interfaces.MetricMeasurementBundle(
            dataset_identifier=self.DATASET_IDENTIFIER,
        )
        with time_this_to_bundle(bundle, action_id, "prep_butlerTotalTime"):
            with lsst.utils.timer.time_this(_log, msg="prep_butler", level=logging.DEBUG):
                _log.info(f"Preparing Butler for visit {self.visit!r}")

                self._write_region_time()  # Must be done before preprocessing pipeline

                # repos may have been modified by other MWI instances.
                # TODO: get a proper synchronization API for Butler
                self.central_butler.registry.refresh()
                self.butler.registry.refresh()

                with time_this_to_bundle(bundle, action_id, "prep_butlerSearchTime"):
                    all_datasets, calib_datasets = self._find_data_to_preload()

                with time_this_to_bundle(bundle, action_id, "prep_butlerTransferTime"):
                    self._transfer_data(all_datasets, calib_datasets)

        # IMPORTANT: do not remove or rename entries in this list. New entries can be added as needed.
        enforce_schema(bundle, {action_id: ["prep_butlerTotalTime",
                                            "prep_butlerSearchTime",
                                            "prep_butlerTransferTime",
                                            ]})
        self.butler.registry.registerDatasetType(DatasetType(
            "promptPreload_metrics",
            dimensions={"instrument", "group", "detector"},
            storageClass="MetricMeasurementBundle",
            universe=self.butler.dimensions,
        ))
        self.butler.put(bundle,
                        "promptPreload_metrics",
                        run=self._get_preload_run(self._day_obs),
                        instrument=self.instrument.getName(),
                        detector=self.visit.detector,
                        group=self.visit.groupId)

    def _find_data_to_preload(self):
        """Identify the datasets to export from the central repo.

        The returned datasets are a superset of those needed by any pipeline,
        but exclude any datasets that are already present in the local repo.

        Returns
        -------
        datasets : set [`~lsst.daf.butler.DatasetRef`]
            The datasets to be exported, after any filtering.
        calibs : set [`~lsst.daf.butler.DatasetRef`]
            The subset of ``datasets`` representing calibs.
        """
        detector = self.camera[self.visit.detector]
        wcs = self._predict_wcs(detector)
        center, radius = self._detector_bounding_circle(detector, wcs)

        with lsst.utils.timer.time_this(_log, msg="prep_butler (find refcats)", level=logging.DEBUG):
            refcat_datasets = set(self._export_refcats(center, radius))
        with lsst.utils.timer.time_this(_log, msg="prep_butler (find templates)", level=logging.DEBUG):
            template_datasets = set(self._export_skymap_and_templates(
                center, detector, wcs, self.visit.filters))
        with lsst.utils.timer.time_this(_log, msg="prep_butler (find calibs)", level=logging.DEBUG):
            calib_datasets = set(self._export_calibs(self.visit.detector, self.visit.filters))
        with lsst.utils.timer.time_this(_log, msg="prep_butler (find ML models)", level=logging.DEBUG):
            model_datasets = set(self._export_ml_models())
        return (refcat_datasets | template_datasets | calib_datasets | model_datasets,
                calib_datasets,
                )

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
        possible_refcats = _get_refcat_types(self.central_butler)
        _log.debug("Searching for refcats of types %s in %s...",
                   {t.name for t in possible_refcats}, shard_ids)
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
        skymap_templates : iterable [`DatasetRef`]
            The datasets to be exported, after any filtering.
        """
        # TODO: This exports the whole skymap, but we want to only export the
        # subset of the skymap that covers this data.
        skymaps = set(_filter_datasets(
            self.central_butler, self.butler,
            "skyMap",
            skymap=self.skymap_name,
            collections=self._collection_skymap,
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
        template_where = f"patch in ({patches_str})"
        # TODO: do we need to have the coadd name used in the pipeline
        # specified as a class kwarg, so that we only load one here?
        # TODO: alternately, we need to extract it from the pipeline? (best?)
        try:
            _log.debug("Searching for templates in tract %d, patches %s...", tract.tract_id, patches_str)
            templates = set(_filter_datasets(
                self.central_butler, self.butler,
                "*Coadd",
                collections=self._collection_template,
                instrument=self.instrument.getName(),
                skymap=self.skymap_name,
                tract=tract.tract_id,
                physical_filter=filter,
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
        # TAI observation start time should be used for calib validity range.
        calib_date = astropy.time.Time(self.visit.private_sndStamp, format="unix_tai")
        # Querying by specific types is much faster than querying by ...
        # Some calibs have an exposure ID (of the source dataset?), but these can't be used in AP.
        types = {t for t in self.central_butler.registry.queryDatasetTypes()
                 if t.isCalibration() and "exposure" not in t.dimensions}
        # TODO: we can't use findFirst=True yet because findFirst query
        # in CALIBRATION-type collection is not supported currently.
        calibs = set(_filter_datasets(
            self.central_butler, self.butler,
            types,
            collections=self.instrument.makeCalibrationCollectionName(),
            instrument=self.instrument.getName(),
            detector=detector_id,
            physical_filter=filter,
            calib_date=calib_date,
        ))
        if calibs:
            for dataset_type, n_datasets in self._count_by_type(calibs):
                _log.debug("Found %d new calib datasets of type '%s'.", n_datasets, dataset_type)
        else:
            _log.debug("Found 0 new calib datasets.")
        return calibs

    def _export_ml_models(self):
        """Identify the pretrained machine learning models to export from the
        central butler.

        Returns
        -------
        models : iterable [`DatasetRef`]
            The datasets to be exported, after any filtering.
        """
        # TODO: the dataset type name is subject to change (especially if more
        # kinds of models are added in the future). Hardcoded names should
        # become unnecessary with DM-40245.
        try:
            models = set(_filter_datasets(
                self.central_butler, self.butler,
                "pretrainedModelPackage",
                collections=self._collection_ml_model,
                findFirst=True))
        except _MissingDatasetError as err:
            _log.error(err)
            models = set()
        else:
            _log.debug("Found %d new ML model datasets.", len(models))
        return models

    def _transfer_data(self, datasets, calibs):
        """Transfer datasets and all associated collections from the central
        repo to the local repo.

        Parameters
        ----------
        datasets : set [`~lsst.daf.butler.DatasetRef`]
            The datasets to transfer into the local repo.
        calibs : set [`~lsst.daf.butler.DatasetRef`]
            The calibs to re-certify into the local repo.
        """
        with lsst.utils.timer.time_this(_log, msg="prep_butler (transfer datasets)", level=logging.DEBUG):
            transferred = self.butler.transfer_from(self.central_butler,
                                                    datasets,
                                                    transfer="copy",
                                                    skip_missing=True,
                                                    register_dataset_types=True,
                                                    transfer_dimensions=True,
                                                    )
            if len(transferred) != len(datasets):
                _log.warning("Downloaded only %d datasets out of %d; missing %s.",
                             len(transferred), len(datasets),
                             datasets - set(transferred))

        with lsst.utils.timer.time_this(_log, msg="prep_butler (transfer collections)", level=logging.DEBUG):
            # VALIDITY-HACK: ensure local <instrument>/calibs is a
            # chain even if central collection isn't.
            self.butler.registry.registerCollection(
                self.instrument.makeCalibrationCollectionName(),
                CollectionType.CHAINED,
            )
            self._export_collections(self._collection_template)
            self._export_collections(self.instrument.makeUmbrellaCollectionName())

        with lsst.utils.timer.time_this(_log, msg="prep_butler (transfer associations)", level=logging.DEBUG):
            self._export_calib_associations(self.instrument.makeCalibrationCollectionName(), calibs)

        # Temporary workarounds until we have a prompt-processing default top-level collection
        # in shared repos, and raw collection in dev repo, and then we can organize collections
        # without worrying about DRP use cases.
        self.butler.collection_chains.prepend_chain(
            self.instrument.makeUmbrellaCollectionName(),
            [self._collection_template,
             self.instrument.makeDefaultRawIngestRunName(),
             # VALIDITY-HACK: account for case where source
             # collection was CALIBRATION or omitted from
             # umbrella.
             self.instrument.makeCalibrationCollectionName(),
             ])

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
        removed = set()
        for child in src.queryCollections(collection, flattenChains=True, includeChains=True):
            # VALIDITY-HACK: do not transfer calibration collections; we'll make our own
            if src.getCollectionType(child) == CollectionType.CALIBRATION:
                removed.add(child)
                continue
            if src.getCollectionType(child) == CollectionType.CHAINED:
                chains[child] = src.getCollectionChain(child)
            dest.registerCollection(child,
                                    src.getCollectionType(child),
                                    src.getCollectionDocumentation(child))
        for chain, children in chains.items():
            # VALIDITY-HACK: fix up chains after removing calibration
            # collections and including local-only collections.
            present = list(dest.getCollectionChain(chain)) + list(children)
            for missing in removed:
                while missing in present:
                    present.remove(missing)
            dest.setCollectionChain(chain, present)

    # VALIDITY-HACK: used only for local "latest calibs" collection
    def _get_local_calib_collection(self, parent):
        """Generate a name for a local-only calib collection to store mock
        associations.

        Parameters
        ----------
        parent : `str`
            The chain that serves as the calib interface for the rest of
            this class.

        Returns
        -------
        calib_collection : `str`
            The calibration collection name to use. This method does *not*
            create the collection itself.
        """
        return f"{parent}/{self._day_obs}"

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
        # VALIDITY-HACK: (re)define a calibration collection containing the
        # latest calibs as of today. Already-cached calibs are still available
        # from previous collections.
        if datasets:
            calib_latest = self._get_local_calib_collection(calib_collection)
            new = self.butler.registry.registerCollection(calib_latest, CollectionType.CALIBRATION)
            if new:
                self.butler.collection_chains.prepend_chain(calib_collection, [calib_latest])

            # VALIDITY-HACK: real associations are expensive to query. Just apply
            # arbitrary ones and assume that the first collection in the chain is
            # always the best.
            self.butler.registry.certify(calib_latest, datasets, Timespan(None, None))

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

    def _write_region_time(self):
        """Store the approximate sky region and timespan for this
        object's visit.
        """
        detector = self.camera[self.visit.detector]
        wcs = self._predict_wcs(detector)

        # TODO: unify with other region estimates on DM-43712
        center = wcs.pixelToSky(detector.getCenter(lsst.afw.cameraGeom.PIXELS))
        corners = wcs.pixelToSky(detector.getCorners(lsst.afw.cameraGeom.PIXELS))
        padded = [c.offset(center.bearingTo(c), self.padding) for c in corners]
        region = lsst.sphgeom.ConvexPolygon.convexHull([c.getVector() for c in padded])

        # Assume a padded interval that's centered on the most probable time
        # TODO: replace with self.visit.startTime after DM-38635
        start = astropy.time.Time(self.visit.private_sndStamp, format="unix_tai")
        end = start + 3.0 * self.visit.duration * astropy.units.second
        timespan = Timespan(start, end)

        self.butler.registry.registerDatasetType(DatasetType(
            "regionTimeInfo",
            dimensions={"instrument", "group", "detector"},
            storageClass="RegionTimeInfo",
            universe=self.butler.dimensions,
        ))
        self.butler.put(lsst.pipe.base.utils.RegionTimeInfo(region=region, timespan=timespan),
                        "regionTimeInfo",
                        run=self._get_preload_run(self._day_obs),
                        instrument=self.instrument.getName(),
                        detector=self.visit.detector,
                        group=self.visit.groupId)

    def _get_output_chain(self,
                          date: str) -> str:
        """Generate a deterministic output chain name that avoids
        configuration conflicts.

        Parameters
        ----------
        date : `str`
            Date of the processing run (not observation!).

        Returns
        -------
        chain : `str`
            The chain in which to place all output collections.
        """
        # Order optimized for S3 bucket -- filter out as many files as soon as possible.
        return self.instrument.makeCollectionName("prompt", f"output-{date}")

    def _get_preload_run(self,
                         date: str) -> str:
        """Generate a deterministic preload collection name that avoids
        configuration conflicts.

        Parameters
        ----------
        date : `str`
            Date of the processing run (not observation!).

        Returns
        -------
        run : `str`
            The run in which to place preload/pre-execution products.
        """
        return self._get_output_run("Preload", date)

    def _get_init_output_run(self,
                             pipeline_file: str,
                             date: str) -> str:
        """Generate a deterministic init-output collection name that avoids
        configuration conflicts.

        Parameters
        ----------
        pipeline_file : `str`
            The pipeline file that the run will be used for.
        date : `str`
            Date of the processing run (not observation!).

        Returns
        -------
        run : `str`
            The run in which to place pipeline init-outputs.
        """
        # Current executor requires that init-outputs be in the same run as
        # outputs. This can be changed once DM-36162 is done.
        return self._get_output_run(pipeline_file, date)

    def _get_output_run(self,
                        pipeline_file: str,
                        date: str) -> str:
        """Generate a deterministic collection name that avoids version or
        provenance conflicts.

        Parameters
        ----------
        pipeline_file : `str`
            The pipeline file that the run will be used for.
        date : `str`
            Date of the processing run (not observation!).

        Returns
        -------
        run : `str`
            The run in which to place processing outputs.
        """
        pipeline_name, _ = os.path.splitext(os.path.basename(pipeline_file))
        # Order optimized for S3 bucket -- filter out as many files as soon as possible.
        return "/".join([self._get_output_chain(date), pipeline_name, self._deployment])

    def _prep_collections(self):
        """Pre-register output collections in advance of running the pipeline.
        """
        self.butler.registry.refresh()
        self.butler.registry.registerCollection(
            self._get_preload_run(self._day_obs),
            CollectionType.RUN)
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
            pipeline.addConfigOverride("parameters", "apdb_config", self._apdb_config)
        except LookupError:
            _log.warning(f"{pipeline_file} does not have an `apdb_config` parameter; "
                         "assuming no APDB support is needed.")
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

        Returns
        -------
        exposure_id : `int`
            The exposure ID of the image that was just ingested.
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
        return result[0].dataId["exposure"]

    def _get_graph_executor(self, butler, factory):
        """Create a QuantumGraphExecutor suitable for Prompt Processing.

        Parameters
        ----------
        butler : `lsst.daf.butler.Butler`
            The Butler for which the quantum graph will be generated
            and executed. Should match the Butler passed to
            SeparablePipelineExecutor.
        factory : `lsst.pipe.base.TaskFactory`
            The task factory used for pipeline execution. Should match
            the factory passed to SeparablePipelineExecutor.

        Returns
        -------
        executor : `lsst.ctrl.mpexec.QuantumGraphExecutor`
            The executor to use.
        """
        quantum_executor = SingleQuantumExecutor(
            butler,
            factory,
        )
        graph_executor = MPGraphExecutor(
            # TODO: re-enable parallel execution once we can log as desired with CliLog or a successor
            # (see issues linked from DM-42063)
            numProc=1,  # Avoid spawning processes, because they bypass our logger
            timeout=2_592_000.0,  # In practice, timeout is never helpful; set to 30 days.
            quantumExecutor=quantum_executor,
        )
        return graph_executor

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
            preload_run = self._get_preload_run(self._day_obs)
            init_output_run = self._get_init_output_run(pipeline_file, self._day_obs)
            output_run = self._get_output_run(pipeline_file, self._day_obs)
            exec_butler = Butler(butler=self.butler,
                                 collections=[output_run, init_output_run, preload_run]
                                 + list(self.butler.collections),
                                 run=output_run)
            factory = lsst.ctrl.mpexec.TaskFactory()
            executor = SeparablePipelineExecutor(
                exec_butler,
                clobber_output=False,
                skip_existing_in=None,
                task_factory=factory,
            )
            try:
                with lsst.utils.timer.time_this(_log, msg="executor.make_quantum_graph", level=logging.DEBUG):
                    qgraph = executor.make_quantum_graph(pipeline, where=where)
            except MissingDatasetTypeError as e:
                _log.error(f"Building quantum graph for {pipeline_file} failed ", exc_info=e)
                continue
            if len(qgraph) == 0:
                # Diagnostic logs are the responsibility of GraphBuilder.
                _log.error(f"Empty quantum graph for {pipeline_file}; "
                           "see previous logs for details.")
                continue
            # Past this point, partial execution creates datasets.
            # Don't retry -- either fail (raise) or break.

            # If this is a fresh (local) repo, then types like calexp,
            # *Diff_diaSrcTable, etc. have not been registered.
            # TODO: after DM-38041, move pre-execution to one-time repo setup.
            executor.pre_execute_qgraph(qgraph, register_dataset_types=True, save_init_outputs=True)
            _log.info(f"Running '{pipeline._pipelineIR.description}' on {where}")
            try:
                with lsst.utils.timer.time_this(_log, msg="executor.run_pipeline", level=logging.DEBUG):
                    executor.run_pipeline(
                        qgraph,
                        graph_executor=self._get_graph_executor(exec_butler, factory)
                    )
                    _log.info("Pipeline successfully run.")
            except Exception as e:
                state_changed = True  # better safe than sorry
                try:
                    data_ids = set(self.butler.registry.queryDataIds(
                        ["instrument", "visit", "detector"], where=where))
                    if len(data_ids) == 1:
                        data_id = list(data_ids)[0]
                        apdb = lsst.dax.apdb.Apdb.from_uri(self._apdb_config)
                        if not apdb.containsVisitDetector(data_id["visit"], self.visit.detector):
                            state_changed = False
                    else:
                        # Don't know how this could happen, so won't try to handle it gracefully.
                        _log.warning("Unexpected visit ids: %s. Assuming APDB modified.", data_ids)
                except Exception:
                    # Failure in registry or APDB queries
                    _log.exception("Could not determine APDB state, assuming modified.")
                    raise NonRetriableError("APDB potentially modified") from e
                else:
                    if state_changed:
                        raise NonRetriableError("APDB modified") from e
                    else:
                        raise
            finally:
                # Refresh so that registry queries know the processed products.
                self.butler.registry.refresh()
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
        # Rather than determining which pipeline was run, just try to export all of them.
        output_runs = [self._get_preload_run(self._day_obs)]
        for f in self._get_pipeline_files():
            output_runs.extend([self._get_init_output_run(f, self._day_obs),
                                self._get_output_run(f, self._day_obs),
                                ])
        try:
            with lsst.utils.timer.time_this(_log, msg="export_outputs", level=logging.DEBUG):
                exports = self._export_subset(exposure_ids,
                                              # TODO: find a way to merge datasets like *_config
                                              # or *_schema that are duplicated across multiple
                                              # workers.
                                              self._get_safe_dataset_types(self.butler),
                                              in_collections=output_runs,
                                              )
                if exports:
                    populated_runs = {ref.run for ref in exports}
                    _log.info(f"Pipeline products saved to collections {populated_runs}.")
                    output_chain = self._get_output_chain(self._day_obs)
                    self._chain_exports(output_chain, populated_runs)
                else:
                    _log.warning("No datasets match visit=%s and exposures=%s.", self.visit, exposure_ids)

        finally:
            # TODO: can we use SasquatchDatastore to streamline this?
            dispatcher = _get_sasquatch_dispatcher()
            if dispatcher:
                with lsst.utils.timer.time_this(_log, msg="upload metrics", level=logging.DEBUG):
                    # Making bundles a collection makes debug log simpler, and it should be short.
                    bundles = list(self._query_datasets_by_storage_class(
                        self.butler, exposure_ids, output_runs, "MetricMeasurementBundle"))
                    for bundle in bundles:
                        _log_trace.debug("Uploading %s...", bundle)
                        dispatcher.dispatchRef(self.butler.get(bundle), bundle)
                _log.debug("Uploaded %d metrics to %s.", len(bundles), dispatcher.url)

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

        with lsst.utils.timer.time_this(_log, msg="export_outputs (find outputs)", level=logging.DEBUG):
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

        with lsst.utils.timer.time_this(_log, msg="export_outputs (refresh)", level=logging.DEBUG):
            # central repo may have been modified by other MWI instances.
            # TODO: get a proper synchronization API for Butler
            self.central_butler.registry.refresh()

        with lsst.utils.timer.time_this(_log, msg="export_outputs (transfer)", level=logging.DEBUG):
            # Transfer dimensions created by ingest in case it was never done in
            # central repo (which is normal for dev).
            # Transferring governor dimensions in parallel can cause deadlocks in
            # central registry. We need to transfer our exposure/visit dimensions,
            # so handle those manually.
            for dimension in ["group",
                              "day_obs",
                              "exposure",
                              "visit",
                              ]:
                if dimension in self.butler.registry.dimensions:
                    records = self.butler.registry.queryDimensionRecords(
                        dimension,
                        where="exposure in (exposure_ids)",
                        bind={"exposure_ids": exposure_ids},
                        instrument=self.instrument.getName(),
                        detector=self.visit.detector,
                    )
                    # If records don't match, this is not an error, and central takes precedence.
                    self.central_butler.registry.insertDimensionData(dimension, *records, skip_existing=True)
            transferred = self.central_butler.transfer_from(self.butler, datasets,
                                                            transfer="copy", transfer_dimensions=False)
            if len(transferred) != len(datasets):
                _log.error("Uploaded only %d datasets out of %d; missing %s.",
                           len(transferred), len(datasets),
                           set(datasets) - set(transferred))

        return transferred

    def _chain_exports(self, output_chain: str, output_runs: collections.abc.Iterable[str]) -> None:
        """Associate exported datasets with a chained collection in the
        central Butler.

        Parameters
        ----------
        output_chain : `str`
            The chained collection with which to associate the outputs. Need not exist.
        output_runs : iterable [`str`]
            The run collection(s) containing the outputs. Assumed to exist.
        """
        with lsst.utils.timer.time_this(_log, msg="export_outputs (chain runs)", level=logging.DEBUG):
            self.central_butler.registry.refresh()
            self.central_butler.registry.registerCollection(output_chain, CollectionType.CHAINED)

            try:
                self.central_butler.collection_chains.prepend_chain(output_chain, output_runs)
            except sqlalchemy.exc.IntegrityError as e:
                # HACK: I don't know of a better way to distinguish exceptions
                # blended by SQLAlchemy. To be removed on DM-43316.
                if 'duplicate key value violates unique constraint "collection_chain_pkey"' in str(e):
                    _log.error("Failed to update output chain concurrently; continuing export without retry.")
                else:
                    raise

    def _query_datasets_by_storage_class(self, butler, exposure_ids, collections, storage_class):
        """Identify all datasets with a particular storage class, regardless of
        dataset type.

        Parameters
        ----------
        butler : `lsst.daf.butler.Butler`
            The Butler in which to query for datasets.
        exposure_ids : `set` [`int`]
            Exposure IDs for which to return datasets.
        collections : iterable [`str`]
            The collections in which to query for datasets.
        storage_class : `str`
            The name of the storage class by which to query.

        Yields
        ------
        dataset : `lsst.daf.butler.DatasetRef`
            A dataset in ``collections`` of type ``storage_class``. Guaranteed
            to include values for implied dimensions, but need not include
            dimension records. The order in which datasets are returned
            is undefined.
        """
        matching_types = {dtype for dtype in butler.registry.queryDatasetTypes(...)
                          if dtype.storageClass_name == storage_class}
        _log.debug("Found dataset types matching %s: %s", storage_class, {t.name for t in matching_types})
        yield from butler.registry.queryDatasets(
            matching_types,
            collections=collections,
            findFirst=True,
            # collections may include other runs, so need to filter.
            # Since AP processing is strictly visit-detector, these three
            # dimensions should suffice.
            # DO NOT assume that visit == exposure!
            where="exposure in (exposure_ids)",
            bind={"exposure_ids": exposure_ids},
            instrument=self.instrument.getName(),
            detector=self.visit.detector,
        )

    def clean_local_repo(self, exposure_ids: set[int]) -> None:
        """Remove local repo content that is only needed for a single visit.

        This includes raws and pipeline outputs.

        Parameter
        ---------
        exposure_ids : `set` [`int`]
            Identifiers of the exposures to be removed.
        """
        with lsst.utils.timer.time_this(_log, msg="clean_local_repo", level=logging.DEBUG):
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
                    self.butler.collection_chains.remove_from_chain(chain, [output_run])
                self.butler.removeRuns([output_run], unstore=True)
            # VALIDITY-HACK: remove cached calibs to avoid future conflicts
            if not cache_calibs:
                calib_chain = self.instrument.makeCalibrationCollectionName()
                calib_taggeds = self.butler.registry.queryCollections(
                    calib_chain,
                    flattenChains=True,
                    collectionTypes={CollectionType.CALIBRATION, CollectionType.TAGGED})
                calib_runs = self.butler.registry.queryCollections(
                    calib_chain,
                    flattenChains=True,
                    collectionTypes=CollectionType.RUN)
                self.butler.collection_chains.redefine_chain(calib_chain, [])
                for member in calib_taggeds:
                    self.butler.registry.removeCollection(member)
                self.butler.removeRuns(calib_runs, unstore=True)

            # TODO: workaround for DM-40193
            if not cache_anything:
                self.butler.pruneDatasets(self.butler.registry.queryDatasets(...),
                                          disassociate=True, unstore=True, purge=True)


class _MissingDatasetError(RuntimeError):
    """An exception flagging that required datasets were not found
    where expected.
    """
    pass


def _filter_datasets(src_repo: Butler,
                     dest_repo: Butler,
                     *args,
                     calib_date: astropy.time.Time | None = None,
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
    calib_date : `astropy.time.Time`, optional
        If provided, also filter anything other than calibs valid at
        ``calib_date`` and check that at least one valid calib was found.
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
    formatted_args = "{}, {}".format(
        ", ".join(repr(a) for a in args),
        ", ".join(f"{k}={v!r}" for k, v in kwargs.items()),
    )
    try:
        with lsst.utils.timer.time_this(_log, msg=f"_filter_datasets({formatted_args}) (known datasets)",
                                        level=logging.DEBUG):
            known_datasets = set(dest_repo.registry.queryDatasets(*args, **kwargs))
            _log_trace.debug("Known datasets: %s", known_datasets)
    except lsst.daf.butler.registry.DataIdValueError as e:
        _log.debug("Pre-export query with args '%s' failed with %s", formatted_args, e)
        # If dimensions are invalid, then *any* such datasets are missing.
        known_datasets = set()

    # Let exceptions from src_repo query raise: if it fails, that invalidates
    # this operation.
    # "expanded" dimension records are ignored for DataCoordinate equality
    # comparison, so we only need them on src_datasets.
    with lsst.utils.timer.time_this(_log, msg=f"_filter_datasets({formatted_args}) (source datasets)",
                                    level=logging.DEBUG):
        src_datasets = set(src_repo.registry.queryDatasets(*args, **kwargs).expanded())
        # In many contexts, src_datasets is too large to print.
        _log_trace3.debug("Source datasets: %s", src_datasets)
    if calib_date:
        src_datasets = _filter_calibs_by_date(
            src_repo,
            kwargs["collections"] if "collections" in kwargs else ...,
            src_datasets,
            calib_date,
        )
        _log_trace.debug("Sources filtered to %s: %s", calib_date.iso, src_datasets)
    if not src_datasets:
        raise _MissingDatasetError(
            "Source repo query with args '{}' found no matches.".format(formatted_args))
    return itertools.filterfalse(lambda ref: ref in known_datasets, src_datasets)


def _filter_calibs_by_date(butler: Butler,
                           collections: typing.Any,
                           unfiltered_calibs: collections.abc.Collection[lsst.daf.butler.DatasetRef],
                           date: astropy.time.Time
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
    date : `astropy.time.Time`
        The time at which the calibs must be valid.

    Returns
    -------
    filtered_calibs : iterable [`lsst.daf.butler.DatasetRef`]
        The datasets in ``unfiltered_calibs`` that are valid on ``date``. Not
        guaranteed to be the same `~lsst.daf.butler.DatasetRef` objects passesd
        to ``unfiltered_calibs``, but guaranteed to be fully expanded.
    """
    with lsst.utils.timer.time_this(_log, msg="filter_calibs", level=logging.DEBUG):
        # Unfiltered_calibs can have up to one copy of each calib per certify cycle.
        # Minimize redundant queries to find_dataset.
        unique_ids = {(ref.datasetType, ref.dataId) for ref in unfiltered_calibs}
        t = Timespan.fromInstant(date)
        _log_trace.debug("Looking up calibs for %s in %s.", t, collections)
        filtered_calibs = []
        for dataset_type, data_id in unique_ids:
            # Use find_dataset to simultaneously filter by validity and chain order
            found_ref = butler.find_dataset(dataset_type,
                                            data_id,
                                            collections=collections,
                                            timespan=t,
                                            dimension_records=True,
                                            )
            if found_ref:
                filtered_calibs.append(found_ref)
        return filtered_calibs


def _get_refcat_types(butler):
    """Return the refcat dataset types known to a Butler.

    Parameters
    ---------
    butler: `lsst.daf.butler.Butler`
        The butler in which to search for refcat dataset types.

    Returns
    -------
    refcat_types : iterable of `str` or `lsst.daf.butler.DatasetType`
        The matching dataset type objects, or their names.
    """
    # Assume that any type that has ONLY a single spatial dimension, and a
    # SimpleCatalog storage class, is a refcat.
    return {t for t in butler.registry.queryDatasetTypes(...)
            if t.storageClass_name == "SimpleCatalog" and len(t.dimensions) == 1 and t.dimensions.skypix}
