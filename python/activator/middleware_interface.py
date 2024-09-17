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

__all__ = ["get_central_butler", "make_local_repo", "flush_local_repo", "make_local_cache",
           "MiddlewareInterface"]

import collections.abc
import itertools
import logging
import os
import os.path
import shutil
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
from lsst.daf.butler import Butler, CollectionType, DatasetType, Timespan, \
    DataIdValueError, MissingDatasetTypeError
import lsst.dax.apdb
import lsst.geom
import lsst.obs.base
import lsst.pipe.base
import lsst.analysis.tools
from lsst.analysis.tools.interfaces.datastore import SasquatchDispatcher  # Can't use fully-qualified name

from shared.config import PipelinesConfig
import shared.run_utils as runs
from shared.visit import FannedOutVisit
from .caching import DatasetCache
from .exception import GracefulShutdownInterrupt, NonRetriableError, RetriableError, \
    InvalidPipelineError, NoGoodPipelinesError, PipelinePreExecutionError, PipelineExecutionError
from .timer import enforce_schema, time_this_to_bundle

_log = logging.getLogger("lsst." + __name__)
_log.setLevel(logging.DEBUG)
# See https://developer.lsst.io/stack/logging.html#logger-trace-verbosity
_log_trace = logging.getLogger("TRACE1.lsst." + __name__)
_log_trace.setLevel(logging.CRITICAL)  # Turn off by default.
_log_trace3 = logging.getLogger("TRACE3.lsst." + __name__)
_log_trace3.setLevel(logging.CRITICAL)  # Turn off by default.

# The number of calib datasets to keep from previous runs, or one less than the
# number of calibs that may be present *during* a run.
base_keep_limit = int(os.environ.get("LOCAL_REPO_CACHE_SIZE", 3))-1
# Multipliers to base_keep_limit for refcats and templates.
refcat_factor = int(os.environ.get("REFCATS_PER_IMAGE", 4))
template_factor = int(os.environ.get("PATCHES_PER_IMAGE", 4))
# Whether or not to export to the central repo.
do_export = bool(int(os.environ.get("DEBUG_EXPORT_OUTPUTS", '1')))
# The number of arcseconds to pad the region in preloading spatial datasets.
padding = float(os.environ.get("PRELOAD_PADDING", 30))


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


def flush_local_repo(repo_dir: str, central_butler: Butler):
    """Clean up a local repository abandoned by a different PP instance.

    The function attempts to pass datasets to the local repo; as with
    `MiddlewareInterface.export_outputs`, only datasets that would not collide
    with other pods are handled at this time.

    Parameters
    ----------
    repo_dir : `str`
        An absolute path to a local repo that is *not* being managed by
        TemporaryDirectory or similar.
    central_butler : `lsst.daf.butler.Butler`
        Butler repo to which to attempt to flush the local repo's contents.
    """
    try:
        butler = Butler(repo_dir, writeable=True)
        with lsst.utils.timer.time_this(_log, msg="flush_local_repo (find datasets)", level=logging.DEBUG):
            safe_types = MiddlewareInterface._get_safe_dataset_types(butler)
            # Exclude calibs, templates, and other input datasets
            datasets = set()
            for dataset_type in safe_types:
                datasets |= set(
                    butler.query_datasets(dataset_type, collections="*/prompt/*", find_first=False)
                )
        with lsst.utils.timer.time_this(_log, msg="flush_local_repo (transfer)", level=logging.DEBUG):
            MiddlewareInterface._export_exposure_dimensions(butler, central_butler)
            transferred = central_butler.transfer_from(butler, datasets,
                                                       transfer="copy", transfer_dimensions=False)
            # Not necessarily a problem -- we might be transferring datasets
            # that have already been (partially?) transferred.
            _check_transfer_completion(datasets, transferred, "Uploaded")
            # Don't try to chain the runs -- only way to get the correct chain
            # name is to parse the collection name(s), too brittle for the rare
            # case that the runs aren't already in central repo.
    except Exception:
        _log.exception("Could not sync outputs from %s; they will be lost.", repo_dir)
    finally:
        # If deletion fails, raise and let the activator handle it.
        shutil.rmtree(repo_dir)


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


def make_local_cache():
    """Set up a cache for preloaded datasets.

    Returns
    -------
    cache : `activator.caching.DatasetCache`
        An empty cache with configured caching strategy and limits.
    """
    return DatasetCache(
        base_keep_limit,
        cache_sizes={
            # TODO: find an API that doesn't require explicit enumeration
            "goodSeeingCoadd": template_factor * base_keep_limit,
            "deepCoadd": template_factor * base_keep_limit,
            "uw_stars_20240524": refcat_factor * base_keep_limit,
            "uw_stars_20240228": refcat_factor * base_keep_limit,
            "uw_stars_20240130": refcat_factor * base_keep_limit,
            "ps1_pv3_3pi_20170110": refcat_factor * base_keep_limit,
            "gaia_dr3_20230707": refcat_factor * base_keep_limit,
            "gaia_dr2_20200414": refcat_factor * base_keep_limit,
            "atlas_refcat2_20220201": refcat_factor * base_keep_limit,
            "the_monster_20240904": refcat_factor * base_keep_limit,
        },
    )


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
    pre_pipelines : `activator.config.PipelinesConfig`
        Information about which pipelines to run before a visit arrives.
    main_pipelines : `activator.config.PipelinesConfig`
        Information about which pipelines to run on ``visit``'s raws.
    skymap : `str`
        Name of the skymap in the central repo for querying templates.
    local_repo : `str`
        A URI to the local Butler repo, which is assumed to already exist and
        contain standard collections and the registration of ``instrument``.
    local_cache : `activator.caching.DatasetCache`
        A cache holding datasets and usage history for preloaded dataset types
        in ``local_repo``.
    prefix : `str`, optional
        URI scheme followed by ``://``; prepended to ``image_bucket`` when
        constructing URIs to retrieve incoming files. The default is
        appropriate for use in the USDF environment; typically only
        change this when running local tests.
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
                 pre_pipelines: PipelinesConfig, main_pipelines: PipelinesConfig,
                 # TODO: encapsulate relationship between local_repo and local_cache
                 skymap: str, local_repo: str, local_cache: DatasetCache,
                 prefix: str = "s3://"):
        self.visit = visit

        self._apdb_config = os.environ["CONFIG_APDB"]
        # Deployment/version ID -- potentially expensive to generate.
        self._deployment = runs.get_deployment(self._apdb_config)
        self.central_butler = central_butler
        self.image_host = prefix + image_bucket
        # TODO: _download_store turns MWI into a tagged class; clean this up later
        if not self.image_host.startswith("file"):
            self._download_store = tempfile.TemporaryDirectory(prefix="holding-")
        else:
            self._download_store = None
        # TODO: how much overhead do we pick up from going through the registry?
        self.instrument = lsst.obs.base.Instrument.from_string(visit.instrument, central_butler.registry)
        self.pre_pipelines = pre_pipelines
        self.main_pipelines = main_pipelines

        self._day_obs = runs.get_day_obs(astropy.time.Time.now())

        self._init_local_butler(local_repo, [self.instrument.makeUmbrellaCollectionName()], None)
        self.cache = local_cache  # DO NOT copy -- we want to persist this state!
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

        # How much to pad the spatial region we will copy over.
        self.padding = padding*lsst.geom.arcseconds

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
        self.instrument.applyConfigOverrides(lsst.obs.base.RawIngestTask._DefaultName, config)
        config.transfer = "copy"  # Copy files into the local butler.
        config.failFast = True  # We want failed ingests to fail immediately.
        self.rawIngestTask = lsst.obs.base.RawIngestTask(config=config,
                                                         butler=self.butler)

    def _init_visit_definer(self):
        """Prepare the visit definer to define visits for this butler.

        ``self._init_local_butler`` must have already been run.
        """
        define_visits_config = lsst.obs.base.DefineVisitsConfig()
        self.instrument.applyConfigOverrides(lsst.obs.base.DefineVisitsTask._DefaultName,
                                             define_visits_config)
        define_visits_config.groupExposures = "one-to-one"
        self.define_visits = lsst.obs.base.DefineVisitsTask(config=define_visits_config, butler=self.butler)

    def _define_dimensions(self):
        """Define any dimensions that must be computed from this object's visit.

        ``self._init_local_butler`` must have already been run.
        """
        self.butler.registry.syncDimensionData("group",
                                               {"name": self.visit.groupId,
                                                "instrument": self.instrument.getName(),
                                                })

    def _mark_dataset_usage(self, refs: collections.abc.Iterable[lsst.daf.butler.DatasetRef]):
        """Mark requested datasets in the cache.

        Parameters
        ----------
        refs : iterable [`lsst.daf.butler.DatasetRef`]
            The datasets to mark. Assumed to all fit inside the cache.
        """
        self.cache.update(refs)
        try:
            self.cache.access(refs)
        except LookupError as e:
            raise RuntimeError("Cache is too small for one run's worth of datasets.") from e

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

        Raises
        ------
        _NoPositionError
            Raised if the nextVisit message does not have coordinates
            in a supported format.
        """
        try:
            sky_position = self.visit.get_boresight_icrs()
            boresight_center = lsst.geom.SpherePoint(sky_position.ra.degree, sky_position.dec.degree,
                                                     lsst.geom.degrees)
            orientation = self.visit.get_rotation_sky().degree * lsst.geom.degrees
        except TypeError as e:
            raise _NoPositionError("nextVisit does not have a position.") from e
        except RuntimeError as e:
            raise _NoPositionError(str(e)) from e

        formatter = self.instrument.getRawFormatter({"detector": detector.getId()})
        return formatter.makeRawSkyWcsFromBoresight(boresight_center, orientation, detector)

    def _compute_region(self) -> lsst.sphgeom.Region:
        """Compute the sky region of this visit for preload

        Returns
        -------
        region : `lsst.sphgeom.Region`
            Region for preload.

        Raises
        ------
        _NoPositionError
            Raised if the nextVisit message does not have coordinates
            in a supported format.
        """
        detector = self.camera[self.visit.detector]
        wcs = self._predict_wcs(detector)

        # Compare the preload region padding versus the visit region padding
        # in the middleware visit definition.
        visit_definition_padding = (
            self.define_visits.config.computeVisitRegions["single-raw-wcs"].padding
            * wcs.getPixelScale().asArcseconds()
        )
        preload_region_padding = self.padding.asArcseconds()
        if preload_region_padding < visit_definition_padding:
            _log.warning("Preload padding (%.1f arcsec) is smaller than "
                         "visit definition's region padding (%.1f arcsec).",
                         preload_region_padding, visit_definition_padding)

        center = wcs.pixelToSky(detector.getCenter(lsst.afw.cameraGeom.PIXELS))
        corners = wcs.pixelToSky(detector.getCorners(lsst.afw.cameraGeom.PIXELS))
        padded = [c.offset(center.bearingTo(c), self.padding) for c in corners]
        return lsst.sphgeom.ConvexPolygon.convexHull([c.getVector() for c in padded])

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

                try:
                    region = self._compute_region()
                    _log.debug(
                        f"Preload region {region} including padding {self.padding.asArcseconds()} arcsec.")
                    self._write_region_time(region)  # Must be done before preprocessing pipeline
                except _NoPositionError as e:
                    _log.warning("Could not get sky position from visit %s: %s. "
                                 "Spatial datasets won't be loaded.", self.visit, e)
                    region = None

                # repos may have been modified by other MWI instances.
                # TODO: get a proper synchronization API for Butler
                self.central_butler.registry.refresh()
                self.butler.registry.refresh()

                with time_this_to_bundle(bundle, action_id, "prep_butlerSearchTime"):
                    all_datasets, calib_datasets = self._find_data_to_preload(region)

                with time_this_to_bundle(bundle, action_id, "prep_butlerTransferTime"):
                    self._transfer_data(all_datasets, calib_datasets)

                with time_this_to_bundle(bundle, action_id, "prep_butlerPreprocessTime"):
                    try:
                        self._run_preprocessing()
                    except NoGoodPipelinesError:
                        _log.exception("Preprocessing pipelines not runnable, trying main pipelines anyway.")
                    except (PipelinePreExecutionError, PipelineExecutionError):
                        _log.exception("Preprocessing pipeline failed, trying main pipelines anyway.")

        # IMPORTANT: do not remove or rename entries in this list. New entries can be added as needed.
        enforce_schema(bundle, {action_id: ["prep_butlerTotalTime",
                                            "prep_butlerSearchTime",
                                            "prep_butlerTransferTime",
                                            "prep_butlerPreprocessTime",
                                            ]})
        self.butler.registry.registerDatasetType(DatasetType(
            "promptPreload_metrics",
            dimensions={"instrument", "group", "detector"},
            storageClass="MetricMeasurementBundle",
            universe=self.butler.dimensions,
        ))
        self.butler.put(bundle,
                        "promptPreload_metrics",
                        run=runs.get_preload_run(self.instrument, self._deployment, self._day_obs),
                        instrument=self.instrument.getName(),
                        detector=self.visit.detector,
                        group=self.visit.groupId)

    def _find_data_to_preload(self, region):
        """Identify the datasets to export from the central repo.

        The returned datasets are a superset of those needed by any pipeline,
        but exclude any datasets that are already present in the local repo.

        Parameters
        ----------
        region : `lsst.sphgeom.Region` or None
            The region to find data to preload.

        Returns
        -------
        datasets : set [`~lsst.daf.butler.DatasetRef`]
            The datasets to be exported, after any filtering.
        calibs : set [`~lsst.daf.butler.DatasetRef`]
            The subset of ``datasets`` representing calibs.

        Raises
        ------
        RuntimeError
            Raised if attempting the spatial preload with the input region being None.
        """
        with lsst.utils.timer.time_this(_log, msg="prep_butler (find calibs)", level=logging.DEBUG):
            calib_datasets = set(self._export_calibs(self.visit.detector, self.visit.filters))
        if region is None:
            return (calib_datasets, calib_datasets)

        with lsst.utils.timer.time_this(_log, msg="prep_butler (find refcats)", level=logging.DEBUG):
            refcat_datasets = set(self._export_refcats(region))
        with lsst.utils.timer.time_this(_log, msg="prep_butler (find templates)", level=logging.DEBUG):
            template_datasets = set(self._export_skymap_and_templates(
                region, self.visit.filters))
        with lsst.utils.timer.time_this(_log, msg="prep_butler (find ML models)", level=logging.DEBUG):
            model_datasets = set(self._export_ml_models())
        return (refcat_datasets | template_datasets | calib_datasets | model_datasets,
                calib_datasets,
                )

    def _export_refcats(self, region):
        """Identify the refcats to export from the central butler.

        Parameters
        ----------
        region : `lsst.sphgeom.Region`
            The region to find refcat shards in.

        Returns
        -------
        refcats : iterable [`DatasetRef`]
            The refcats to be exported, after any filtering.
        """
        # Get shards from all refcats that overlap this region.
        possible_refcats = _get_refcat_types(self.central_butler)
        _log.debug("Searching for refcats of types %s.", {t.name for t in possible_refcats})
        refcats = set(_filter_datasets(
                      self.central_butler, self.butler,
                      _generic_query(possible_refcats,
                                     collections=self.instrument.makeRefCatCollectionName(),
                                     where="htm7.region OVERLAPS search_region",
                                     bind={"search_region": region},
                                     find_first=True,
                                     ),
                      all_callback=self._mark_dataset_usage,
                      ))
        if refcats:
            for dataset_type, n_datasets in self._count_by_type(refcats):
                _log.debug("Found %d new refcat datasets from catalog '%s'.", n_datasets, dataset_type)
        else:
            _log.debug("Found 0 new refcat datasets.")
        return refcats

    def _export_skymap_and_templates(self, region, filter):
        """Identify the skymap and templates to export from the central butler.

        Parameters
        ----------
        region : `lsst.sphgeom.Region`
            The region to load the skyamp and templates tract/patches for.
        filter : `str`
            Physical filter for which to export templates. May be empty to
            indicate no specific filter.

        Returns
        -------
        skymap_templates : iterable [`DatasetRef`]
            The datasets to be exported, after any filtering.
        """
        skymaps = set(_filter_datasets(
            self.central_butler, self.butler,
            _generic_query(["skyMap"],
                           skymap=self.skymap_name,
                           collections=self._collection_skymap,
                           find_first=True,
                           ),
            all_callback=self._mark_dataset_usage,
        ))
        _log.debug("Found %d new skymap datasets.", len(skymaps))

        if not filter:
            _log.warning("Preloading templates is not supported for visits without a specific filter.")
            return skymaps

        data_id = {"instrument": self.instrument.getName(),
                   "skymap": self.skymap_name,
                   "physical_filter": filter,
                   }
        types = self._get_template_types()
        if types:
            try:
                _log.debug("Searching for templates.")
                templates = set(_filter_datasets(
                    self.central_butler, self.butler,
                    _generic_query(types,
                                   collections=self._collection_template,
                                   data_id=data_id,
                                   where="patch.region OVERLAPS search_region",
                                   bind={"search_region": region},
                                   find_first=True,
                                   ),
                    all_callback=self._mark_dataset_usage,
                ))
            except _MissingDatasetError as err:
                _log.error("Could not load templates: %s", err)
                templates = set()
            else:
                _log.debug("Found %d new template datasets.", len(templates))
        else:
            templates = set()
        return skymaps | templates

    def _get_calib_types(self):
        """Identify the specific calib types to query.

        Returns
        -------
        type_names : collection [`str`]
            The calib types of interest to Prompt Processing.
        """
        # Querying by specific types is much faster than querying by ...
        # Some calibs have an exposure ID (of the source dataset?), but these can't be used in AP.
        types = {t for t in self.central_butler.registry.queryDatasetTypes()
                 if t.isCalibration() and "exposure" not in t.dimensions}
        if not filter:
            _log.warning("Preloading filter-dependent calibs is not supported for visits "
                         "without a specific filter.")
            types = {t for t in types
                     if "physical_filter" not in t.dimensions and "band" not in t.dimensions}
        type_names = {t.name for t in types}
        # For now, filter down to the dataset types that exist in the specific calib collection.
        # TODO: A new query API after DM-45873 may replace or improve this usage.
        # TODO: DM-40245 to identify the datasets.
        collections_info = self.central_butler.collections.query_info(
            self.instrument.makeCalibrationCollectionName(), include_summary=True)
        return self.central_butler.collections._filter_dataset_types(type_names, collections_info)

    def _export_calibs(self, detector_id, filter):
        """Identify the calibs to export from the central butler.

        Parameters
        ----------
        detector_id : `int`
            Identifier of the detector to load calibs for.
        filter : `str`
            Physical filter name of the upcoming visit. May be empty to indicate
            no specific filter.

        Returns
        -------
        calibs : iterable [`DatasetRef`]
            The calibs to be exported, after any filtering.
        """
        # TAI observation start time should be used for calib validity range.
        calib_date = astropy.time.Time(self.visit.private_sndStamp, format="unix_tai")
        data_id = {"instrument": self.instrument.getName(), "detector": detector_id}
        if filter:
            data_id["physical_filter"] = filter
        type_names = self._get_calib_types()

        def query_calibs_by_date(butler, label):
            with lsst.utils.timer.time_this(_log, msg=f"Calib query ({label})", level=logging.DEBUG), \
                    butler.query() as query:
                expr = query.expression_factory
                query = query.where(data_id)
                datasets = set()
                for dataset_type in type_names:
                    try:
                        datasets |= set(
                            query.datasets(dataset_type,
                                           self.instrument.makeCalibrationCollectionName(),
                                           find_first=True)
                            # where needs to come after datasets to pick up the type
                            .where(expr[dataset_type].timespan.overlaps(calib_date))
                            .with_dimension_records()
                        )
                    except (DataIdValueError, MissingDatasetTypeError) as e:
                        # Dimensions/dataset type often invalid for fresh local repo,
                        # where there are no, and never have been, any matching datasets.
                        # It *is* a problem for the central repo, but can be caught later.
                        _log.debug("%s query failed with %s.", label, e)
                # Trace3 because, in many contexts, datasets is too large to print.
                _log_trace3.debug("%s: %s", label, datasets)
                return datasets

        calibs = set(_filter_datasets(
            self.central_butler, self.butler,
            query_calibs_by_date,
            all_callback=self._mark_dataset_usage,
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
                _generic_query(["pretrainedModelPackage"],
                               collections=self._collection_ml_model,
                               find_first=True,
                               ),
                all_callback=self._mark_dataset_usage,
            ))
        except _MissingDatasetError as err:
            _log.error("Could not load ML models: %s", err)
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
            _check_transfer_completion(datasets, transferred, "Downloaded")

        with lsst.utils.timer.time_this(_log, msg="prep_butler (transfer collections)", level=logging.DEBUG):
            self._export_collections(self._collection_template)
            self._export_collections(self.instrument.makeUmbrellaCollectionName())

        with lsst.utils.timer.time_this(_log, msg="prep_butler (transfer associations)", level=logging.DEBUG):
            self._export_calib_associations(self.instrument.makeCalibrationCollectionName(), calibs)

        # Temporary workarounds until we have a prompt-processing default top-level collection
        # in shared repos, and raw collection in dev repo, and then we can organize collections
        # without worrying about DRP use cases.
        self.butler.collections.prepend_chain(
            self.instrument.makeUmbrellaCollectionName(),
            [self._collection_template,
             self.instrument.makeDefaultRawIngestRunName(),
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
        collections = self.central_butler.collections
        with self.central_butler.query() as query, \
                self.central_butler.registry.caching_context():  # Nested loops produce lots of queries
            for dataset in datasets:
                dtype = dataset.datasetType
                result = query.where(dataset.dataId) \
                    .join_dataset_search(dtype, calib_collection) \
                    .general(dtype.dimensions,
                             dataset_fields={dtype.name: {"dataset_id", "run", "collection", "timespan"}},
                             find_first=False,  # Required for timespan queries.
                             )
                # Associations include run membership, and possibly multiple calibration collections.
                for association in lsst.daf.butler.DatasetAssociation.from_query_result(result, dtype):
                    if collections.get_info(association.collection).type == CollectionType.CALIBRATION \
                            and association.ref == dataset:
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

    def _write_region_time(self, region):
        """Store the preload sky region and timespan for this
        object's visit.

        Parameters
        ----------
        region : `lsst.sphgeom.Region`
            Region for preload.
        """
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
                        run=runs.get_preload_run(self.instrument, self._deployment, self._day_obs),
                        instrument=self.instrument.getName(),
                        detector=self.visit.detector,
                        group=self.visit.groupId)

    def _prep_collections(self):
        """Pre-register output collections in advance of running the pipeline.
        """
        self.butler.registry.refresh()
        self.butler.registry.registerCollection(
            runs.get_preload_run(self.instrument, self._deployment, self._day_obs),
            CollectionType.RUN)
        for pipeline_file in self._get_all_pipeline_files():
            self.butler.registry.registerCollection(
                runs.get_output_run(self.instrument, self._deployment, pipeline_file, self._day_obs),
                CollectionType.RUN)

    def _get_all_pipeline_files(self) -> collections.abc.Collection[str]:
        """Identify the pipelines to be run at any point, based on the
        configured instrument and visit.

        Returns
        -------
        pipelines : collection [`str`]
            The paths to a configured pipeline file. The order is undefined.
        """
        # A pipeline appearing in both configs is unlikely, but not impossible.
        all = set(self._get_pre_pipeline_files())
        all.update(self._get_main_pipeline_files())
        return all

    def _get_pre_pipeline_files(self) -> collections.abc.Sequence[str]:
        """Identify the pipelines to be run during preprocessing, based on the
        configured instrument and visit.

        Returns
        -------
        pipelines : sequence [`str`]
            A sequence of paths to a configured pipeline file, in order from
            most preferred to least preferred.
        """
        return self.pre_pipelines.get_pipeline_files(self.visit)

    def _get_main_pipeline_files(self) -> collections.abc.Sequence[str]:
        """Identify the pipelines to be run, based on the configured instrument
        and visit.

        Returns
        -------
        pipelines : sequence [`str`]
            A sequence of paths to a configured pipeline file, in order from
            most preferred to least preferred.
        """
        return self.main_pipelines.get_pipeline_files(self.visit)

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

        # Config overrides are not validated until graph generation.
        pipeline.addConfigOverride("parameters", "apdb_config", self._apdb_config)

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
            assumeNoExistingOutputs=True,  # Outputs cleared out on success *or* failure
            raise_on_partial_outputs=True,  # Only way to detect that partial outputs happened
        )
        graph_executor = MPGraphExecutor(
            # TODO: re-enable parallel execution once we can log as desired with CliLog or a successor
            # (see issues linked from DM-42063)
            numProc=1,  # Avoid spawning processes, because they bypass our logger
            timeout=2_592_000.0,  # In practice, timeout is never helpful; set to 30 days.
            quantumExecutor=quantum_executor,
        )
        return graph_executor

    def _try_pipelines(self, pipelines, in_collections, data_ids, *, label):
        """Attempt to run pipelines from a prioritized list.

        On success, exactly one of the pipelines is run, with outputs going to
        a run named after the pipeline file.

        Parameters
        ----------
        pipelines : sequence [`str`]
            The pipeline file(s) to run, in decreasing order of preference.
        in_collections : sequence [`str`]
            Collections, usually containing previous outputs, to search (in
            order) when reading pipeline inputs. This list is prepended to the
            collections in ``self.butler``.
        data_ids : `str`
            A query string, in the format of the ``where`` parameter to
            `lsst.daf.butler.query_data_ids`, specifying the data IDs
            over which to run the pipelines.
        label : `str`
            A unique name to disambiguate this pipeline run for logging
            purposes.

        Returns
        -------
        output_run : `str`
            The run to which the successful pipeline wrote its outputs.

        Raises
        ------
        activator.exception.InvalidPipelineError
            Raised if any pipeline could not be loaded/configured.
        activator.exception.NoGoodPipelinesError
            Raised if graph generation failed for all pipelines.
        activator.exception.PipelinePreExecutionError
            Raised if pipeline execution was attempted but pre-execution failed.
        activator.exception.PipelineExecutionError
            Raised if pipeline execution was attempted but failed.
        """
        # Try pipelines in order until one works.
        for pipeline_file in pipelines:
            try:
                pipeline = self._prep_pipeline(pipeline_file)
            except FileNotFoundError as e:
                raise InvalidPipelineError(f"Could not load {pipeline_file}.") from e
            output_run = runs.get_output_run(self.instrument, self._deployment, pipeline_file, self._day_obs)
            exec_butler = Butler(butler=self.butler,
                                 collections=[output_run]
                                 + in_collections
                                 + list(self.butler.collections.defaults),
                                 run=output_run)
            factory = lsst.ctrl.mpexec.TaskFactory()
            executor = SeparablePipelineExecutor(
                exec_butler,
                clobber_output=False,
                skip_existing_in=None,
                task_factory=factory,
            )
            try:
                with lsst.utils.timer.time_this(
                        _log, msg=f"executor.make_quantum_graph ({label})", level=logging.DEBUG):
                    qgraph = executor.make_quantum_graph(pipeline, where=data_ids)
            except (FileNotFoundError, MissingDatasetTypeError):
                _log.exception(f"Building quantum graph for {pipeline_file} failed.")
                continue
            if len(qgraph) == 0:
                # Diagnostic logs are the responsibility of GraphBuilder.
                _log.error(f"Empty quantum graph for {pipeline_file}; see previous logs for details.")
                continue
            # Past this point, partial execution creates datasets.
            # Don't retry -- either fail (raise) or break.

            try:
                # If this is a fresh (local) repo, then types like calexp,
                # *Diff_diaSrcTable, etc. have not been registered.
                # TODO: after DM-38041, move pre-execution to one-time repo setup.
                executor.pre_execute_qgraph(qgraph, register_dataset_types=True, save_init_outputs=True)
            except Exception as e:
                raise PipelinePreExecutionError(f"PreExecInit failed for {pipeline_file}.") from e
            _log.info(f"Running '{pipeline_file}' on {data_ids}")
            for quantum_node in qgraph.inputQuanta:
                _log.debug(f"Running with input datasets {quantum_node.quantum.inputs.values()}")
            try:
                with lsst.utils.timer.time_this(
                        _log, msg=f"executor.run_pipeline ({label})", level=logging.DEBUG):
                    executor.run_pipeline(
                        qgraph,
                        graph_executor=self._get_graph_executor(exec_butler, factory)
                    )
                    _log.info(f"{label.capitalize()} pipeline successfully run.")
                    return output_run
            except Exception as e:
                raise PipelineExecutionError(f"Execution failed for {pipeline_file}.") from e
            finally:
                # Refresh so that registry queries know the processed products.
                self.butler.registry.refresh()
            break
        else:
            raise NoGoodPipelinesError(f"No {label} pipeline graph could be built.")

    def _run_preprocessing(self) -> None:
        """Preprocess a visit ahead of incoming image(s).

        The internal butler must contain all data and all dimensions needed to
        run the appropriate pipeline on this visit.

        Raises
        ------
        activator.exception.InvalidPipelineError
            Raised if any pipeline could not be loaded/configured.
        activator.exception.NoGoodPipelinesError
            Raised if graph generation failed for all pipelines.
        activator.exception.PipelinePreExecutionError
            Raised if pipeline execution was attempted but pre-execution failed.
        activator.exception.PipelineExecutionError
            Raised if pipeline execution was attempted but failed.
        """
        pipeline_files = self._get_pre_pipeline_files()
        if not pipeline_files:
            _log.info(f"No preprocessing pipeline configured for {self.visit}, skipping.")
            return

        # Inefficient, but most graph builders can't take equality constraints
        where = (
            f"instrument='{self.visit.instrument}' and detector={self.visit.detector} "
            f"and group='{self.visit.groupId}'"
        )
        preload_run = runs.get_preload_run(self.instrument, self._deployment, self._day_obs)

        self._try_pipelines(self._get_pre_pipeline_files(),
                            in_collections=[preload_run],
                            data_ids=where,
                            label="preprocessing",
                            )

    def _check_permanent_changes(self, where: str) -> bool:
        """Test whether the APDB, alert stream, or other external state has
        changed in a way that makes retries unsafe.

        Parameters
        ----------
        where : `str`
            A :ref:`Butler query string <daf_butler_queries>` identifying the
            current visit. The query should return exactly one visit.

        Returns
        ----------
        changes : `bool`
            `True` if changes have been made, `False` if retries are safe.
        """
        data_ids = self.butler.query_data_ids(["instrument", "visit", "detector"], where=where, explain=False)
        if len(data_ids) == 1:
            data_id = data_ids[0]
            apdb = lsst.dax.apdb.Apdb.from_uri(self._apdb_config)
            return apdb.containsVisitDetector(data_id["visit"], self.visit.detector)
        elif not data_ids:
            # Engineering exposures don't produce visits, but they also can't write to the APDB.
            return False
        else:
            # Don't know how this could happen, so won't try to handle it gracefully.
            _log.warning("Unexpected visit ids: %s. Assuming APDB modified.", data_ids)
            return True

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
        activator.exception.InvalidPipelineError
            Raised if any pipeline could not be loaded/configured.
        activator.exception.NoGoodPipelinesError
            Raised if graph generation failed for all pipelines.
        activator.exception.PipelinePreExecutionError
            Raised if pipeline execution was attempted but pre-execution failed.
        activator.exception.PipelineExecutionError
            Raised if pipeline execution was attempted but failed, and neither
            `~activator.exception.NonRetriableError` nor
            `~activator.exception.RetriableError` apply.
        activator.exception.NonRetriableError
            Raised if external resources (such as the APDB or alert stream)
            may have been left in a state that makes it unsafe to retry
            failures. This exception is always chained to another exception
            representing the original error.
        activator.exception.RetriableError
            Raised if the conditions for NonRetriableError are not met, *and*
            the pipeline fails in a way that is expected to be transient. This
            exception is always chained to another exception representing the
            original error.
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

        # Inefficient, but most graph builders can't take equality constraints
        where = (
            f"instrument='{self.visit.instrument}' and detector={self.visit.detector}"
            f" and exposure in ({','.join(str(x) for x in exposure_ids)})"
        )
        preload_run = runs.get_preload_run(self.instrument, self._deployment, self._day_obs)
        pre_runs = [runs.get_output_run(self.instrument, self._deployment, f, self._day_obs)
                    for f in self._get_pre_pipeline_files()]

        try:
            self._try_pipelines(self._get_main_pipeline_files(),
                                in_collections=pre_runs + [preload_run],
                                data_ids=where,
                                label="main",
                                )
        except GracefulShutdownInterrupt as e:
            try:
                state_changed = self._check_permanent_changes(where)
            except Exception:
                # Failure in registry or APDB queries
                _log.exception("Could not determine APDB state, assuming modified.")
                raise NonRetriableError("APDB potentially modified") from e
            else:
                if state_changed:
                    raise NonRetriableError("APDB modified") from e
                else:
                    raise RetriableError("External interrupt") from e
        # Catch Exception just in case there's a surprise -- raising
        # NonRetriableError on *all* irrevocable changes is important.
        except Exception as e:
            try:
                state_changed = self._check_permanent_changes(where)
            except (Exception, GracefulShutdownInterrupt):
                # Failure in registry or APDB queries
                _log.exception("Could not determine APDB state, assuming modified.")
                raise NonRetriableError("APDB potentially modified") from e
            else:
                if state_changed:
                    raise NonRetriableError("APDB modified") from e
                else:
                    raise

    def export_outputs(self, exposure_ids: set[int]) -> None:
        """Copy pipeline outputs from processing a set of images back
        to the central Butler.

        Parameters
        ----------
        exposure_ids : `set` [`int`]
            Identifiers of the exposures that were processed.
        """
        if not do_export:
            _log.info("Skipping central repo export for exposures %s.", exposure_ids)
            return

        # Rather than determining which pipeline was run, just try to export all of them.
        output_runs = [runs.get_preload_run(self.instrument, self._deployment, self._day_obs)]
        for f in self._get_all_pipeline_files():
            output_runs.append(runs.get_output_run(self.instrument, self._deployment, f, self._day_obs))
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
                    output_chain = runs.get_output_chain(self.instrument, self._day_obs)
                    self._chain_exports(output_chain, populated_runs)
                else:
                    _log.warning("No output datasets match visit=%s and exposures=%s.",
                                 self.visit, exposure_ids)

        finally:
            # TODO: can we use SasquatchDatastore to streamline this?
            dispatcher = _get_sasquatch_dispatcher()
            if dispatcher:
                with lsst.utils.timer.time_this(_log, msg="upload metrics", level=logging.DEBUG):
                    # Making bundles a collection makes debug log simpler, and it should be short.
                    bundles = list(self._query_datasets_by_storage_class(
                        self.butler, exposure_ids, output_runs, "MetricMeasurementBundle"))
                    for bundle in bundles:
                        try:
                            _log_trace.debug("Uploading %s...", bundle)
                            dispatcher.dispatchRef(self.butler.get(bundle), bundle)
                        except lsst.analysis.tools.interfaces.datastore._dispatcher.SasquatchDispatchFailure:
                            # Retries can get messy with multiple bundles, so just abort.
                            _log.exception("Failed to upload %s to Sasquatch: ", bundle)
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
                datasets = set()
                for dataset_type in dataset_types:
                    datasets |= set(self.butler.query_datasets(
                        dataset_type,
                        collections=in_collections,
                        # in_collections may include other runs, so need to filter.
                        # Since AP processing is strictly visit-detector, these three
                        # dimensions should suffice.
                        # DO NOT assume that visit == exposure!
                        where="exposure in (exposure_ids)",
                        bind={"exposure_ids": exposure_ids},
                        with_dimension_records=True,
                        explain=False,  # Failed runs might not have datasets of every type.
                        instrument=self.instrument.getName(),
                        detector=self.visit.detector,
                    ))
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
            self._export_exposure_dimensions(
                self.butler,
                self.central_butler,
                where="exposure in (exposure_ids)",
                bind={"exposure_ids": exposure_ids},
                instrument=self.instrument.getName(),
                detector=self.visit.detector,
            )
            transferred = self.central_butler.transfer_from(self.butler, datasets,
                                                            transfer="copy", transfer_dimensions=False)
            _check_transfer_completion(datasets, transferred, "Uploaded")

        return transferred

    @staticmethod
    def _export_exposure_dimensions(src_butler, dest_butler, **kwargs):
        """Transfer dimensions generated from an exposure to the central repo.

        In many cases the exposure records will already exist in the central
        repo, but this is not guaranteed (especially in dev environments).
        Visit records never exist in the central repo and are the sole
        responsibility of Prompt Processing.

        Parameters
        ----------
        src_butler : `lsst.daf.butler.Butler`
            The butler from which to transfer dimension records.
        dest_butler : `lsst.daf.butler.Butler`
            The butler to which to transfer records.
        **kwargs
            Any data ID parameters to select specific records. They have the
            same meanings as the parameters of
            `lsst.daf.butler.Butler.query_dimension_records`.
        """
        core_dimensions = ["group",
                           "day_obs",
                           "exposure",
                           "visit",
                           "visit_system",
                           ]
        universe = src_butler.dimensions

        full_dimensions = [universe[d] for d in core_dimensions if d in universe]
        extra_dimensions = []
        for d in full_dimensions:
            extra_dimensions.extend(universe.get_elements_populated_by(universe[d]))
        sorted_dimensions = universe.sorted(full_dimensions + extra_dimensions)

        for dimension in sorted_dimensions:
            records = src_butler.query_dimension_records(dimension, explain=False, **kwargs)
            # If records don't match, this is not an error, and central takes precedence.
            dest_butler.registry.insertDimensionData(dimension, *records, skip_existing=True)

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
                self.central_butler.collections.prepend_chain(output_chain, output_runs)
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
        yield from itertools.chain.from_iterable(
            butler.query_datasets(t,
                                  collections=collections,
                                  find_first=True,
                                  # collections may include other runs, so need to filter.
                                  # Since AP processing is strictly visit-detector, these three
                                  # dimensions should suffice.
                                  # DO NOT assume that visit == exposure!
                                  where="exposure in (exposure_ids)",
                                  bind={"exposure_ids": exposure_ids},
                                  explain=False,
                                  instrument=self.instrument.getName(),
                                  detector=self.visit.detector,
                                  ) for t in matching_types
        )

    def _get_template_types(self) -> collections.abc.Set[str]:
        """Identify the dataset types of possible templates in the main pipelines.

        Returns
        -------
        template_types : set [`str`]
            A set of template dataset types in the main pipelines.
        """
        template_types = set()
        for pipeline_file in self._get_main_pipeline_files():
            try:
                pipeline = self._prep_pipeline(pipeline_file)
            except FileNotFoundError as e:
                raise RuntimeError from e
            graph = pipeline.to_graph()
            for dataset_type in graph.iter_overall_inputs():
                if dataset_type[0].endswith("Coadd"):
                    template_types.add(dataset_type[0])
        return template_types

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
            if exposure_ids:
                raws = self.butler.query_datasets(
                    'raw',
                    collections=self.instrument.makeDefaultRawIngestRunName(),
                    where=f"exposure in ({', '.join(str(x) for x in exposure_ids)})",
                    explain=False,  # Raws might not have been ingested.
                    instrument=self.visit.instrument,
                    detector=self.visit.detector,
                )
                self.butler.pruneDatasets(raws, disassociate=True, unstore=True, purge=True)
            # Outputs are all in their own runs, so just drop them.
            preload_run = runs.get_preload_run(self.instrument, self._deployment, self._day_obs)
            _remove_run_completely(self.butler, preload_run)
            for pipeline_file in self._get_all_pipeline_files():
                output_run = runs.get_output_run(self.instrument, self._deployment, pipeline_file,
                                                 self._day_obs)
                _remove_run_completely(self.butler, output_run)

            # Clean out calibs, templates, and other preloaded datasets
            excess_datasets = set()
            for dataset_type in self.butler.registry.queryDatasetTypes(...):
                excess_datasets |= set(self.butler.query_datasets(
                    dataset_type, collections="*", find_first=False, explain=False))
            excess_datasets -= frozenset(self.cache)
            if excess_datasets:
                _log_trace.debug("Clearing out %s.", excess_datasets)
                self.butler.pruneDatasets(excess_datasets, disassociate=True, unstore=True, purge=True)


class _MissingDatasetError(RuntimeError):
    """An exception flagging that required datasets were not found
    where expected.
    """
    pass


class _NoPositionError(RuntimeError):
    """An exception flagging that the nextVisit does not have readable
    position information.
    """


_DatasetResults: typing.TypeAlias = collections.abc.Iterable[lsst.daf.butler.DatasetRef]
"""Type alias for dataset query results, to simplify annotations."""


def _filter_datasets(src_repo: Butler,
                     dest_repo: Butler,
                     query: collections.abc.Callable[[Butler, str], _DatasetResults],
                     all_callback: typing.Callable[[collections.abc.Iterable[lsst.daf.butler.DatasetRef]],
                                                   typing.Any] | None = None,
                     ) -> _DatasetResults:
    """Identify datasets in a source repository, filtering out those already
    present in a destination.

    This function raises if nothing in the source repository matches the query criteria.

    Parameters
    ----------
    src_repo : `lsst.daf.butler.Butler`
        The repository in which a dataset must be present.
    dest_repo : `lsst.daf.butler.Butler`
        The repository in which a dataset must not be present.
    query : callable
        A callable that takes a `~lsst.daf.butler.Butler` and a logging label
        and returns matching datasets. The query must be valid for both
        ``src_repo`` and ``dest_repo``.
    all_callback: callable, optional
        If provided, a callable that is called with *all* datasets picked up by
        the query, before filtering out those already present in ``dest_repo``.
        This callable is not called if the query returns no results.

    Returns
    -------
    datasets : iterable [`lsst.daf.butler.DatasetRef`]
        The datasets that exist in ``src_repo`` but not ``dest_repo``.
        datasetRefs are guaranteed to be fully expanded if any only if
        ``query`` guarantees it.

    Raises
    ------
    _MissingDatasetError
        Raised if the query on ``src_repo`` failed to find any datasets.
    """
    known_datasets = query(dest_repo, "known datasets")

    # Let exceptions from src_repo query raise: if it fails, that invalidates
    # this operation.
    src_datasets = set(query(src_repo, "source datasets"))
    if not src_datasets:
        # The downstream method decides what to do with empty results.
        # DM-40245 and DM-46178 may change this.
        raise _MissingDatasetError("Source repo query found no matches.")
    if all_callback:
        all_callback(src_datasets)
    return itertools.filterfalse(lambda ref: ref in known_datasets, src_datasets)


def _generic_query(dataset_types: collections.abc.Iterable[str | lsst.daf.butler.DatasetType],
                   *args,
                   **kwargs) -> collections.abc.Callable[[Butler, str], _DatasetResults]:
    """Generate a parameterized Butler dataset query.

    Parameters
    ----------
    dataset_types : iterable [`str` | `lsst.daf.butler.DatasetType`]
        Iterable of dataset type object or name to search for.
    *args, **kwargs
        Parameters for describing the dataset query. They have the same
        meanings as the parameters of `lsst.daf.butler.query_datasets`.

    Returns
    -------
    query : callable
        A callable that takes a `~lsst.daf.butler.Butler` and an optional
        logging label and executes the dataset query, returning an iterable of
        fully expanded `~lsst.daf.butler.DatasetRef`.
    """
    formatted_args = "dataset_types={}, {}, {}".format(
        dataset_types,
        ", ".join(repr(a) for a in args),
        ", ".join(f"{k}={v!r}" for k, v in kwargs.items()),
    )

    def query(butler: Butler, butler_name: str = ""):
        # time_this automatically escalates its log to ERROR level if the code raises.
        # Some errors are expected to happen sometimes, and we don't want those to log
        # at the ERROR level and cause confusion.
        label = butler_name or str(butler)
        with lsst.utils.timer.time_this(_log, msg=f"_generic_query({formatted_args}) ({label})",
                                        level=logging.DEBUG):
            datasets = set()
            for dataset_type in dataset_types:
                try:
                    datasets |= set(butler.query_datasets(
                        # explain=False because empty query result is ok here.
                        dataset_type, explain=False, with_dimension_records=True, *args, **kwargs
                    ))
                except (DataIdValueError, MissingDatasetTypeError) as e:
                    # Dimensions/dataset type often invalid for fresh local repo,
                    # where there are no, and never have been, any matching datasets.
                    # It *is* a problem for the central repo, but can be caught later.
                    _log.debug("%s query failed with %s.", label, e)
            # Trace3 because, in many contexts, datasets is too large to print.
            _log_trace3.debug("%s: %s", label, datasets)
            return datasets

    return query


def _get_refcat_types(butler):
    """Return the refcat dataset types known to a Butler.

    Parameters
    ---------
    butler : `lsst.daf.butler.Butler`
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


def _remove_run_completely(butler, run):
    """Remove a run and all references to it from a Butler.

    Parameters
    ---------
    butler : `lsst.daf.butler.Butler`
        The butler in which to search for refcat dataset types.
    run : `str`
        The run to remove.
    """
    for chain in butler.registry.getCollectionParentChains(run):
        butler.collections.remove_from_chain(chain, [run])
    butler.removeRuns([run], unstore=True)


def _check_transfer_completion(expected, transferred, transfer_type):
    """Test whether a Butler transfer ran to completion.

    Current behavior on an incomplete transfer is to log a warning.

    Parameters
    ----------
    expected : collection [`lsst.daf.butler.DatasetRef`]
        The datasets that were marked for transfer.
    transferred : collection [`lsst.daf.butler.DatasetRef`]
        The datasets that were successfully transferred.
    transfer_type : `str`
        A brief description of the transfer. Should be a past-tense verb.
    """
    # Count only unique datasets
    expected_s = set(expected)
    transferred_s = set(transferred)
    if len(transferred_s) != len(expected_s):
        _log.warning("%s only %d datasets out of %d; missing %s.",
                     transfer_type.capitalize(),
                     len(transferred_s), len(expected_s),
                     expected_s - transferred_s)
