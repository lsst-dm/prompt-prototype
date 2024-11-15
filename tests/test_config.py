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


import dataclasses
import os
import unittest

from lsst.utils import getPackageDir

from activator.config import PipelinesConfig
from activator.visit import FannedOutVisit


TESTDIR = os.path.abspath(os.path.dirname(__file__))


class PipelinesConfigTest(unittest.TestCase):
    def setUp(self):
        super().setUp()

        self.visit = FannedOutVisit(
            instrument="NotACam",
            detector=42,
            groupId="2023-01-23T23:33:14.762",
            nimages=2,
            filters="k2022",
            coordinateSystem=FannedOutVisit.CoordSys.ICRS,
            position=[134.5454, -65.3261],
            startTime=1_674_516_900.0,
            rotationSystem=FannedOutVisit.RotSys.SKY,
            cameraAngle=135.0,
            survey="TestSurvey",
            salIndex=42,
            scriptSalIndex=42,
            dome=FannedOutVisit.Dome.OPEN,
            duration=35.0,
            totalCheckpoints=1,
            private_sndStamp=1_674_516_794.0,
        )

    def test_main_survey(self):
        config = PipelinesConfig([{"survey": "TestSurvey",
                                   "pipelines": ["${PROMPT_PROCESSING_DIR}/pipelines/NotACam/ApPipe.yaml"],
                                   }])
        self.assertEqual(
            config.get_pipeline_files(self.visit),
            [os.path.normpath(os.path.join(TESTDIR, "..", "pipelines", "NotACam", "ApPipe.yaml"))]
        )

    def test_selection(self):
        config = PipelinesConfig([{"survey": "TestSurvey",
                                   "pipelines": ["/etc/pipelines/SingleFrame.yaml"],
                                   },
                                  {"survey": "CameraTest",
                                   "pipelines": ["${AP_PIPE_DIR}/pipelines/Isr.yaml"],
                                   },
                                  {"survey": "", "pipelines": ["Default.yaml"]},
                                  ])
        self.assertEqual(
            config.get_pipeline_files(self.visit),
            [os.path.normpath(os.path.join("/etc", "pipelines", "SingleFrame.yaml"))]
        )
        self.assertEqual(
            config.get_pipeline_files(dataclasses.replace(self.visit, survey="CameraTest")),
            [os.path.normpath(os.path.join(getPackageDir("ap_pipe"), "pipelines", "Isr.yaml"))]
        )
        self.assertEqual(
            config.get_pipeline_files(dataclasses.replace(self.visit, survey="")),
            [os.path.abspath("Default.yaml")]
        )

    def test_fallback(self):
        config = PipelinesConfig([{"survey": "TestSurvey",
                                   "pipelines": ["/etc/pipelines/SingleFrame.yaml",
                                                 "${AP_PIPE_DIR}/pipelines/Isr.yaml",
                                                 ]}])
        self.assertEqual(
            config.get_pipeline_files(self.visit),
            [os.path.normpath(os.path.join("/etc", "pipelines", "SingleFrame.yaml")),
             os.path.normpath(os.path.join(getPackageDir("ap_pipe"), "pipelines", "Isr.yaml"))]
        )

    def test_none(self):
        config = PipelinesConfig([{"survey": "TestSurvey",
                                   "pipelines": ["None shall pass/pipelines/SingleFrame.yaml"]},
                                  {"survey": "Camera Test", "pipelines": None},
                                  {"survey": "CameraTest", "pipelines": []},
                                  ])
        self.assertEqual(
            config.get_pipeline_files(self.visit),
            [os.path.abspath(os.path.join("None shall pass", "pipelines", "SingleFrame.yaml"))]
        )
        self.assertEqual(config.get_pipeline_files(dataclasses.replace(self.visit, survey="Camera Test")),
                         [])
        self.assertEqual(config.get_pipeline_files(dataclasses.replace(self.visit, survey="CameraTest")),
                         [])

    def test_nomatch(self):
        config = PipelinesConfig([{"survey": "TestSurvey",
                                   "pipelines": ["/etc/pipelines/SingleFrame.yaml"],
                                   },
                                  {"survey": "CameraTest",
                                   "pipelines": ["${AP_PIPE_DIR}/pipelines/Isr.yaml"],
                                   },
                                  ])
        with self.assertRaises(RuntimeError):
            config.get_pipeline_files(dataclasses.replace(self.visit, survey="Surprise"))

    def test_empty(self):
        with self.assertRaises(ValueError):
            PipelinesConfig([])
        with self.assertRaises(ValueError):
            PipelinesConfig(None)

    def test_notlist(self):
        with self.assertRaises(ValueError):
            PipelinesConfig([{"survey": "TestSurvey", "pipelines": "/etc/pipelines/SingleFrame.yaml"}])

    def test_oddlabel(self):
        with self.assertRaises(ValueError):
            PipelinesConfig([{"reason": "TestSurvey", "pipelines": ["/etc/pipelines/SingleFrame.yaml"]}])
        with self.assertRaises(ValueError):
            PipelinesConfig([{"survey": "TestSurvey",
                              "comment": "This is a fancy survey with simple processing.",
                              "pipelines": ["/etc/pipelines/SingleFrame.yaml"]}])

    def test_duplicates(self):
        with self.assertRaises(ValueError):
            PipelinesConfig([{"survey": "TestSurvey",
                              "pipelines": ["/etc/pipelines/ApPipe.yaml",
                                            "${AP_PIPE_DIR}/pipelines/ApPipe.yaml"]}])
        with self.assertRaises(ValueError):
            PipelinesConfig([{"survey": "TestSurvey",
                              "pipelines": ["/etc/pipelines/ApPipe.yaml", "/etc/pipelines/ApPipe.yaml#isr"]}])

    def test_needpipeline(self):
        with self.assertRaises(ValueError):
            PipelinesConfig([{"survey": "TestSurvey"}])
        with self.assertRaises(ValueError):
            PipelinesConfig([{}])

    def test_order(self):
        config = PipelinesConfig([{"survey": "TestSurvey",
                                   "pipelines": ["/etc/pipelines/SingleFrame.yaml"],
                                   },
                                  {"survey": "TestSurvey",
                                   "pipelines": ["${AP_PIPE_DIR}/pipelines/Isr.yaml"],
                                   },
                                  ])
        # Second TestSurvey spec should be ignored
        self.assertEqual(
            config.get_pipeline_files(self.visit),
            [os.path.normpath(os.path.join("/etc", "pipelines", "SingleFrame.yaml"))]
        )

    def test_unconstrained(self):
        config = PipelinesConfig([{"survey": "TestSurvey",
                                   "pipelines": ["/etc/pipelines/SingleFrame.yaml"],
                                   },
                                  {"pipelines": ["${AP_PIPE_DIR}/pipelines/Isr.yaml"]},
                                  {"survey": "", "pipelines": ["Default.yaml"]},
                                  ])
        self.assertEqual(
            config.get_pipeline_files(self.visit),
            [os.path.normpath(os.path.join("/etc", "pipelines", "SingleFrame.yaml"))]
        )
        self.assertEqual(
            # Matches the second node, which has no constraints
            config.get_pipeline_files(dataclasses.replace(self.visit, survey="CameraTest")),
            [os.path.normpath(os.path.join(getPackageDir("ap_pipe"), "pipelines", "Isr.yaml"))]
        )
        self.assertEqual(
            # Matches the second node, which has no constraints
            config.get_pipeline_files(dataclasses.replace(self.visit, survey="")),
            [os.path.normpath(os.path.join(getPackageDir("ap_pipe"), "pipelines", "Isr.yaml"))]
        )
