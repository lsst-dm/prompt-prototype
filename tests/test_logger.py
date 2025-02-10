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


import io
import json
import logging
import os
import threading
import unittest

import pytest

from shared.logger import UsdfJsonFormatter, _parse_log_levels, RecordFactoryContextAdapter


class ParseLogLevelsTest(unittest.TestCase):
    """Test _parse_log_levels.

    Currently _parse_log_levels does not do input validation, so behavior for
    invalid specs is undefined.
    """
    def test_single_level(self):
        self.assertEqual(_parse_log_levels("lsst.daf.butler=DEBUG"), [("lsst.daf.butler", "DEBUG")])

    def test_multi_level(self):
        self.assertEqual(_parse_log_levels("lsst.daf.butler=DEBUG lsst.ip.isr=WARNING"),
                         [("lsst.daf.butler", "DEBUG"), ("lsst.ip.isr", "WARNING")]
                         )

    def test_empty(self):
        self.assertEqual(_parse_log_levels(""), [])

    def test_single_root(self):
        self.assertEqual(_parse_log_levels(".=WARNING"), [(None, "WARNING")])

    def test_root_(self):
        self.assertEqual(_parse_log_levels("lsst.daf.butler=DEBUG .=WARNING lsst=INFO"),
                         [("lsst.daf.butler", "DEBUG"), (None, "WARNING"), ("lsst", "INFO")]
                         )


class UsdfJsonFormatterTest(unittest.TestCase):
    """Test UsdfJsonFormatter with fake log messages.
    """
    def setUp(self):
        super().setUp()

        # Buffer for log output.
        # Can't use assertLogs, because it inserts its own handler/formatter.
        self.output = io.StringIO()
        self.addCleanup(io.StringIO.close, self.output)
        # self.output = self.enterContext(io.StringIO())

        # UsdfJsonFormatter assumes a logging_context field is present.
        old_factory = logging.getLogRecordFactory()
        self.addCleanup(logging.setLogRecordFactory, old_factory)
        logging.setLogRecordFactory(RecordFactoryContextAdapter(old_factory))

        log_handler = logging.StreamHandler(self.output)
        log_handler.setFormatter(UsdfJsonFormatter(
            labels={"instrument": "NotACam"},
        ))
        # Unique logger per test
        self.log = logging.getLogger(self.id())
        self.log.propagate = False
        self.log.addHandler(log_handler)
        self.log.setLevel(logging.DEBUG)

    def _check_log(self, outputs, caller, level, labels, texts):
        """Check that the log output is formatted correctly.

        Parameters
        ----------
        outputs : `list` [`str`]
            A list of the formatted log messages.
        caller : `str`
            The function that called the log.
        level : `str`
            The emitted log level.
        labels : `dict` [`str`, `str`]
            The labels attached to the log message.
        texts : `list` [`str`]
            The expected log messages.
        """
        self.assertEqual(len(outputs), len(texts))
        for output, text in zip(outputs, texts):
            parsed = json.loads(output)
            self.assertEqual(parsed["level"], level)
            self.assertEqual(parsed["message"], text)
            self.assertEqual(parsed["funcName"], caller)
            self.assertEqual(parsed["pathname"], __file__)
            self.assertEqual(parsed["process"], os.getpid())
            self.assertEqual(parsed["name"], self.id())
            for key, value in labels.items():
                self.assertEqual(parsed[key], value)

    def test_direct(self):
        """Test the translation of verbatim log messages.
        """
        msg = "Consider a spherical cow..."
        self.log.info(msg)
        self._check_log(self.output.getvalue().splitlines(),
                        "test_direct", "INFO", {"instrument": "NotACam"},
                        [msg])

    def test_args(self):
        """Test the translation of arg-based log messages.
        """
        msg = "Consider a %s..."
        args = "rotund bovine"
        self.log.warning(msg, args)
        self._check_log(self.output.getvalue().splitlines(),
                        "test_args", "WARNING", {"instrument": "NotACam"},
                        [msg % args])

    def test_quotes(self):
        """Test handling of messages containing single or double quotes.
        """
        msgs = ["Consider a so-called 'spherical cow'.",
                'Consider a so-called "spherical cow".',
                ]
        for msg in msgs:
            self.log.info(msg)
        self._check_log(self.output.getvalue().splitlines(),
                        "test_quotes", "INFO", {"instrument": "NotACam"},
                        msgs)

    def test_multiline(self):
        """Test handling of messages that split across multiple lines.
        """
        msg = """This is a multiline
              message with internal line
              breaks."""
        self.log.error(msg)
        self._check_log(self.output.getvalue().splitlines(),
                        "test_multiline", "ERROR", {"instrument": "NotACam"},
                        [msg])

    def test_exception(self):
        """Test that exception messages include the stack trace.
        """
        try:
            raise RuntimeError("I take exception to that!")
        except RuntimeError as e:
            self.log.exception(e)
        shredded = self.output.getvalue().splitlines()
        self.assertEqual(len(shredded), 1)
        self.assertIn("Traceback (most recent call last)", shredded[0])

    def test_context(self):
        """Test handling of messages that have free-form context.
        """
        msg = "Consider a spherical exposure."
        exposures = {1, 2, 3}
        visit = 42
        ratio = 3.5
        group = "group A"
        settings = {"option": True}
        with logging.getLogRecordFactory().add_context(
            exposures=exposures,
            visit=visit,
            ratio=ratio,
            group=group,
            settings=settings,
        ):
            self.log.info(msg)
        self._check_log(self.output.getvalue().splitlines(),
                        "test_context", "INFO",
                        {"instrument": "NotACam",
                         "exposures": list(exposures),
                         "visit": visit,
                         "ratio": ratio,
                         "group": group,
                         "settings": settings,
                         },
                        [msg])

    def test_side_effects(self):
        """Test that format still modifies exposure records in the same way
        as Formatter.format.
        """
        msg = "Consider a %s..."
        args = "rotund bovine"
        factory = logging.getLogRecordFactory()
        record = factory(self.id(), logging.INFO, "file.py", 42, msg, args, None)
        formatter = UsdfJsonFormatter()
        formatter.format(record)
        # If format has no side effects, record.message does not exist.
        self.assertEqual(record.message, msg % args)


class AddLogContextTest(unittest.TestCase):
    def setUp(self):
        super().setUp()

        old_factory = logging.getLogRecordFactory()
        self.addCleanup(logging.setLogRecordFactory, old_factory)
        logging.setLogRecordFactory(RecordFactoryContextAdapter(old_factory))

        # Unique logger per test
        self.log = logging.getLogger(self.id())
        self.log.propagate = False
        self.log.setLevel(logging.DEBUG)

    def test_basic_context(self):
        factory = logging.getLogRecordFactory()
        with self.assertLogs(self.log, "DEBUG") as recorder:
            self.log.info("Before add_context")
            with factory.add_context(color="blue"):
                self.log.info("This is a log!")
            with factory.add_context(answer="yes", pid=101):
                self.log.info("This is a log!")
            self.log.info("Context-free logging")

            self.assertEqual(len(recorder.records), 4)
            self.assertEqual([dict(rec.logging_context) for rec in recorder.records],
                             [{},
                              {"color": "blue"},
                              {"answer": "yes", "pid": 101},
                              {},
                              ])

    def test_empty_context(self):
        factory = logging.getLogRecordFactory()
        with self.assertLogs(self.log, "DEBUG") as recorder:
            with factory.add_context():
                self.log.info("This is a log!")
            self.log.info("Context-free logging")

            self.assertEqual(len(recorder.records), 2)
            self.assertEqual([dict(rec.logging_context) for rec in recorder.records],
                             [{},
                              {},
                              ])

    def test_nested_context(self):
        factory = logging.getLogRecordFactory()
        with self.assertLogs(self.log, "DEBUG") as recorder:
            with factory.add_context(color="blue"):
                self.log.info("This is a log!")
                with factory.add_context(pid=42):
                    self.log.error("Error found")
                self.log.info("Less context")

            self.assertEqual(len(recorder.records), 3)
            self.assertEqual([dict(rec.logging_context) for rec in recorder.records],
                             [{"color": "blue"},
                              {"pid": 42, "color": "blue"},
                              {"color": "blue"},
                              ])

    def test_overwriting_context(self):
        factory = logging.getLogRecordFactory()
        with self.assertLogs(self.log, "DEBUG") as recorder:
            with factory.add_context(color="blue", pid=42):
                self.log.info("This is a log!")
                with factory.add_context(color="red"):
                    self.log.error("Error found")
                self.log.info("Less context")
                with factory.add_context(pid=88, language="jargon"):
                    self.log.debug("Complex technobabble")
            self.log.info("Logging complete!")

            self.assertEqual(len(recorder.records), 5)
            self.assertEqual([dict(rec.logging_context) for rec in recorder.records],
                             [{"color": "blue", "pid": 42},
                              {"color": "red", "pid": 42},
                              {"color": "blue", "pid": 42},
                              {"color": "blue", "pid": 88, "language": "jargon"},
                              {},
                              ])

    def test_no_clash_context(self):
        factory = logging.getLogRecordFactory()
        with self.assertLogs(self.log, "DEBUG") as recorder:
            with factory.add_context(levelname="TRACE", message="Not a message",
                                     extra={"message": "Read all about it!"}):
                self.log.info("This is a log!")

            self.assertEqual(len(recorder.records), 1)
            self.assertEqual(
                dict(recorder.records[0].logging_context),
                {"levelname": "TRACE", "message": "Not a message",
                 "extra": {"message": "Read all about it!"}}
            )
            # Should not overwrite LogRecord's built-in attributes
            self.assertEqual(recorder.records[0].levelname, "INFO")
            self.assertEqual(recorder.records[0].message, "This is a log!")

    def test_exception_handling(self):
        factory = logging.getLogRecordFactory()
        with self.assertLogs(self.log, "DEBUG") as recorder:
            with factory.add_context(color="blue"):
                self.log.info("This is a log!")
                try:
                    with factory.add_context(pid=42):
                        raise RuntimeError("Something failed")
                except RuntimeError as e:
                    self.assertEqual(e.logging_context, {"color": "blue", "pid": 42})
                    # pid=42 should have been removed here
                    self.log.error("Exception caught")
                    # Exception should hang on to old state
                    self.log.exception("Exception detected")
                    self.log.warning("Implied exception", exc_info=True)
                    self.log.critical("Explicit exception", exc_info=e)

            self.assertEqual(len(recorder.records), 5)
            self.assertEqual([dict(rec.logging_context) for rec in recorder.records],
                             [{"color": "blue"},
                              {"color": "blue"},
                              {"color": "blue", "pid": 42},
                              {"color": "blue", "pid": 42},
                              {"color": "blue", "pid": 42},
                              ])

    def test_overwriting_exception_handling(self):
        factory = logging.getLogRecordFactory()
        with self.assertLogs(self.log, "DEBUG") as recorder:
            try:
                with factory.add_context(color="blue"):
                    with factory.add_context(color="red"):
                        raise RuntimeError("Something failed")
            except RuntimeError as e:
                # Was original context preserved while escaping multiple managers?
                self.assertEqual(e.logging_context, {"color": "red"})
                # Is original context used *only* when logging the exception?
                self.log.error("Exception follows")
                self.log.exception("Exceptional exception")
                self.log.warning("Nothing to see here")

            self.assertEqual(len(recorder.records), 3)
            self.assertEqual([dict(rec.logging_context) for rec in recorder.records],
                             [{},
                              {"color": "red"},
                              {},
                              ])

    def test_bare_exception_handling(self):
        with self.assertLogs(self.log, "DEBUG") as recorder:
            self.log.info("This is a log!")
            try:
                raise RuntimeError("Something failed")
            except RuntimeError as e:
                self.assertFalse(hasattr(e, "logging_context"))
                # Logger shouldn't choke on lack of logging_context
                self.log.exception("Exception detected")
                self.log.warning("Implied exception", exc_info=True)
                self.log.critical("Explicit exception", exc_info=e)

            self.assertEqual(len(recorder.records), 4)
            self.assertEqual([dict(rec.logging_context) for rec in recorder.records],
                             [{},
                              {},
                              {},
                              {},
                              ])

    # This decorator works with scons/unittest as well
    @pytest.mark.filterwarnings("error::pytest.PytestUnhandledThreadExceptionWarning")
    def test_multithreading(self):
        """Test that instances of RecordFactoryContextAdapter work correctly in
        threads other than the factory's creator.

        This test does *not* check whether parallel logging calls are safe, as
        verifying this requires running at scale (with or without GIL).
        """
        # RecordFactoryContextAdapter is designed to be used safely without external locks
        thread = threading.Thread(target=self.test_basic_context)
        thread.start()
        thread.join(timeout=10.0)
        if thread.is_alive():
            self.fail("Test thread timed out!")
