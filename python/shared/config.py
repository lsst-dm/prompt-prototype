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


__all__ = ["PipelinesConfig"]


import collections
import collections.abc
import numbers
import os
import typing

from .maps import PredicateMapHealpix
from .visit import FannedOutVisit


class PipelinesConfig:
    """A pipeline configuration for the Prompt Processing service.

    This class provides the execution framework with a simple interface for
    identifying the pipeline to execute. It attempts to abstract the details of
    which factors affect the choice of pipeline to make it easier to add new
    features in the future.

    Parameters
    ----------
    config : sequence [mapping]
        A sequence of mappings ("nodes"), each with the following keys:

        ``"survey"``
            The survey that triggers the pipelines (`str`, optional).
        ``"ra"``, ``"dec"``
            Mappings with ``min`` and ``max`` keys giving the boresight RA or
            dec range in degrees (mapping [`str`, `float`], optional). RA-only
            or dec-only constraints are allowed. RA wraparound is supported.
            Visits with no position *never* match a node with ra/dec
            constraints.
        ``"binary-map"``
            The path to an integer or boolean Healpix map (`str`, optional).
            Only all-sky maps with implicit indexing are supported.
            16 is assumed to represent "unknown".
            The node matches if the boresight position evaluates to `True`.
            Visits with no position *never* match a node with map
            constraints.
        ``"pipelines"``
            A list of zero or more pipelines (sequence [`str`] or `None`). Each
            pipeline path may contain environment variables, and the list of
            pipelines may be replaced by `None` to mean no pipeline should
            be run.

        Nodes are arranged by precedence, i.e., the first node that matches a
        visit is used. A node containing only ``pipelines`` is unconstrained
        and matches *any* visit.

    Examples
    --------
    A single-survey, single-pipeline config:

    >>> PipelinesConfig([{"survey": "TestSurvey",
    ...                   "pipelines": ["/etc/pipelines/SingleFrame.yaml"]},
    ...                  ])  # doctest: +ELLIPSIS
    <config.PipelinesConfig object at 0x...>

    A config with multiple surveys and pipelines, and environment variables:

    >>> PipelinesConfig([{"survey": "TestSurvey",
    ...                   "pipelines": ["/etc/pipelines/ApPipe.yaml", "/etc/pipelines/ISR.yaml"]},
    ...                  {"survey": "Camera Test",
    ...                   "pipelines": ["${AP_PIPE_DIR}/pipelines/LSSTComCam/Isr.yaml"]},
    ...                  {"survey": "",
    ...                   "pipelines": ["${AP_PIPE_DIR}/pipelines/LSSTComCam/Isr.yaml"]},
    ...                  ])  # doctest: +ELLIPSIS
    <config.PipelinesConfig object at 0x...>

    A config that omits a pipeline for non-sky data:

    >>> PipelinesConfig([{"survey": "TestSurvey",
    ...                   "pipelines": ["/etc/pipelines/ApPipe.yaml"]},
    ...                  {"survey": "Dome Flats",
    ...                   "pipelines": None},
    ...                  ])  # doctest: +ELLIPSIS
    <config.PipelinesConfig object at 0x...>

    A config restricted to a box in ra, dec:

    >>> PipelinesConfig([{"ra": {"min": 340.0, "max": 20.0},  # Can also give -20
    ...                   "dec": {"min": -20.0, "max": 20.0},
    ...                   "pipelines": ["/etc/pipelines/NearEquinox.yaml"]},
    ...                  ])  # doctest: +ELLIPSIS
    <config.PipelinesConfig object at 0x...>

    A config that matches against two maps, and runs a third pipeline for
    the rest:

    >>> PipelinesConfig([{"binary-map": "best_mask.fits",
    ...                   "pipelines": ["/etc/pipelines/BestPipe.yaml"]},
    ...                  {"binary-map": "ok_mask.fits",
    ...                   "pipelines": ["/etc/pipelines/OkPipe.yaml"]},
    ...                  {"pipelines": ["/etc/pipelines/WorstPipe.yaml"]},
    ...                  ])  # doctest: +ELLIPSIS
    <config.PipelinesConfig object at 0x...>
    """

    class _Spec:
        """A single case of which pipelines should be run in particular
        circumstances.

        Parameters
        ----------
        config : mapping [`str`]
            A config node with the same keys as documented for the
            `PipelinesConfig` constructor.
        """

        def __init__(self, config: collections.abc.Mapping[str, typing.Any]):
            specs = dict(config)
            try:
                self._filenames = self._parse_pipelines(specs.pop('pipelines'))
                self._check_pipelines(self._filenames)

                self._ra = self._parse_minmax(specs.pop('ra', None), _WrapRange, wrap=360.0)
                self._dec = self._parse_minmax(specs.pop('dec', None), _LinearRange)
                self._map = self._parse_map(specs.pop('binary-map', None))

                self._positions = any(x is not None for x in [self._ra, self._dec, self._map])

                self._survey = self._parse_survey(specs.pop('survey', None))

                if specs:
                    raise ValueError(f"Got unexpected keywords: {specs.keys()}")
            except KeyError as e:
                raise ValueError from e

        @staticmethod
        def _parse_pipelines(config: typing.Any) -> collections.abc.Sequence[str]:
            """Convert a pipelines config snippet into internal metadata.

            Parameters
            ----------
            config : sequence [`str`], `str`, or `None`
                The pipelines designation in the config. Expected to be a
                prioritized list of filenames, or ``"None"`` or `None` as
                synonyms for an empty sequence. This method is responsible for
                any type checking.

            Returns
            -------
            filenames : sequence [`str`]
                The normalized filename list.
            """
            if config is None or config == "None":
                return []
            elif isinstance(config, str):  # Strings are sequences!
                raise ValueError(f"Pipelines spec must be list or None, got {config}")
            elif isinstance(config, collections.abc.Sequence):
                try:
                    return [os.path.abspath(os.path.expandvars(path)) for path in config]
                except (TypeError, OSError) as e:
                    raise ValueError(f"Pipeline list {config} has invalid paths.") from e
            else:
                raise ValueError(f"Pipelines spec must be list or None, got {config}")

        @staticmethod
        def _parse_survey(config: typing.Any) -> str | None:
            """Convert a survey config snippet into internal metadata.

            Parameters
            ----------
            config : `str` or `None`
                The survey constraint in the config. Expected to be a matching
                string, or `None` if the constraint was omitted. This method is
                responsible for any type checking.

            Returns
            -------
            survey : `str` or `None`
                The validated survey constraint.
            """
            if isinstance(config, str) or config is None:
                return config
            else:
                raise ValueError(f"{config} is not a valid survey name.")

        _RangeType = typing.TypeVar('T', bound=collections.abc.Container)

        @staticmethod
        def _parse_minmax(config: typing.Any,
                          factory: collections.abc.Callable[..., _RangeType],
                          **kwargs) -> _RangeType | None:
            """Convert a numerical range into a range object.

            Parameters
            ----------
            config : mapping [`str`, numeric] or `None`
                A range constraint in the config. Expected to be a mapping with
                ``"min"`` and ``"max"`` fields, or None if the constraint was
                omitted. This method is responsible for any type checking.
            factory : callable
                A callable that takes ``min`` and ``max`` arguments and returns
                a range object.
            **kwargs
                Additional arguments for ``factory``.

            Returns
            -------
            range : range or `None`
                The validated range.
            """
            if config is not None:
                min = config["min"]
                max = config["max"]
                return factory(min=min, max=max, **kwargs)
            else:
                return None

        # TODO: generalize return type?
        @staticmethod
        def _parse_map(config: typing.Any) -> PredicateMapHealpix | None:
            """Convert a map config snippet into a fully-initialized map.

            Parameters
            ----------
            config : `str` or `None`
                The map to load. Expected to be a valid file path, or `None` if
                the constraint was omitted. This method is responsible for any
                type checking.

            Returns
            -------
            map : `activator.maps.PredicateMapHealpix` or `None`
                The map.
            """
            if config is not None:
                path = os.path.abspath(os.path.expandvars(config))
                try:
                    # TODO: find a clean way to optionally specify null in config
                    return PredicateMapHealpix.from_fits(path, null=16)
                except OSError as e:
                    raise ValueError(f"Path {config} is not a valid map.") from e
            else:
                return None

        @staticmethod
        def _check_pipelines(pipelines: collections.abc.Sequence[str]):
            """Test the correctness of a list of pipelines.

            At present, the only test is that no two pipelines have the same
            filename, which is used as a pipeline ID elsewhere in the Prompt
            Processing service.

            Parameters
            ----------
            pipelines : sequence [`str`]

            Raises
            ------
            ValueError
                Raised if the pipeline list is invalid.
            """
            filenames = collections.Counter(os.path.splitext(os.path.basename(path))[0] for path in pipelines)
            duplicates = [filename for filename, num in filenames.items() if num > 1]
            if duplicates:
                raise ValueError(f"Pipeline names must be unique, found multiple copies of {duplicates}.")

        def matches(self, visit: FannedOutVisit) -> bool:
            """Test whether a visit matches the conditions for this spec.

            Parameters
            ----------
            visit : `activator.visit.FannedOutVisit`
                The visit to test against this spec.

            Returns
            -------
            matches : `bool`
                `True` if the visit meets all conditions, `False` otherwise.

            Raises
            ------
            ValueError
                Raised if the visit has invalid or unsupported fields.
            """
            if self._survey is not None and self._survey != visit.survey:
                return False
            if self._positions:
                try:
                    visit_radec = visit.get_boresight_icrs()
                except RuntimeError as e:
                    raise ValueError(str(e)) from e
                if visit_radec is None:
                    return False

                if self._ra is not None and visit_radec.ra.degree not in self._ra:
                    return False
                if self._dec is not None and visit_radec.dec.degree not in self._dec:
                    return False
                if self._map is not None and not self._map.at(visit_radec):
                    # at() may return False or None
                    return False
            return True

        @property
        def pipeline_files(self) -> collections.abc.Sequence[str]:
            """An ordered list of pipelines to run in this spec (sequence [`str`]).

            All filenames are expanded and normalized.
            """
            return self._filenames

    def __init__(self, config: collections.abc.Sequence):
        if not config:
            raise ValueError("Must configure at least one pipeline.")

        self._specs = self._expand_config(config)

    @staticmethod
    def _expand_config(config: collections.abc.Sequence) -> collections.abc.Sequence[_Spec]:
        """Turn a config spec into structured config information.

        Parameters
        ----------
        config : sequence [mapping]
            A sequence of mappings, see class docs for details.

        Returns
        -------
        config : sequence [`PipelinesConfig._Spec`]
            A sequence of node objects specifying the pipeline(s) to run and the
            conditions in which to run them.

        Raises
        ------
        ValueError
            Raised if the input config is invalid.
        """
        items = []
        for node in config:
            items.append(PipelinesConfig._Spec(node))
        return items

    def get_pipeline_files(self, visit: FannedOutVisit) -> list[str]:
        """Identify the pipeline to be run, based on the provided visit.

        The first node that matches the visit is returned, and no other nodes
        are considered even if they would provide a "tighter" match.

        Parameters
        ----------
        visit : `activator.visit.FannedOutVisit`
            The visit for which a pipeline must be selected.

        Returns
        -------
        pipelines : `list` [`str`]
            Path(s) to the configured pipeline file(s), in decreasing priority
            order. An empty list means that *no* pipeline should be run on
            this visit.

        Raises
        ------
        ValueError
            Raised if the visit has invalid or unsupported fields.
        """
        for node in self._specs:
            if node.matches(visit):
                return node.pipeline_files
        raise RuntimeError(f"No pipelines config matches {visit}")

    def get_all_pipeline_files(self) -> collections.abc.Collection[str]:
        """Return all pipelines in this configuration.

        Returns
        -------
        pipelines : collection [`str`]
            Path(s) to the configured pipeline file(s). Unlike
            `get_pipeline_files`, the pipelines are not returned in any
            particular order.
        """
        pipelines = set()
        for node in self._specs:
            pipelines.update(os.path.expandvars(path) for path in node.pipeline_files)
        return pipelines


class _LinearRange:
    """A [min, max] range on the number line.

    Parameters
    ----------
    min, max : numeric
        The endpoints of the range. The range requires that `min < max`.
    """

    def __init__(self, min: numbers.Real, max: numbers.Real):
        self._min = min
        self._max = max
        if self._min >= self._max:
            raise ValueError(f"Invalid range ({min}, {max}).")

    def __contains__(self, value: numbers.Real) -> bool:
        """Test if a number falls inside the range.

        The range includes both endpoints.
        """
        return self._min <= value and value <= self._max


class _WrapRange:
    """A range on a circular domain.

    Parameters
    ----------
    min, max : numeric
        The endpoints of the range. Values outside [0, ``wrap``) are allowed.
        Because the domain is circular, [a, b) is the complement of [b, a).
    wrap : numeric
        The point at which the domain wraps back to 0.
    """

    def __init__(self, min: numbers.Real, max: numbers.Real, wrap: numbers.Real):
        # invariant: _min, _max are in [0, wrap)
        self._min = min % wrap
        self._max = max % wrap
        self._wrap = wrap

    def __contains__(self, value: numbers.Real) -> bool:
        """Test if a number falls inside the range.

        The range includes both endpoints.
        """
        wrapped = value % self._wrap
        if self._min < self._max:  # Range excludes 0
            return self._min <= wrapped and wrapped <= self._max
        else:  # Range wraps through 0
            return self._min <= wrapped or wrapped <= self._max
