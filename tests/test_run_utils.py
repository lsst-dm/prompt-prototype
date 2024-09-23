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

import unittest

from lsst.obs.base import Instrument

from shared.run_utils import get_output_chain, get_preload_run, get_output_run

# The short name of the instrument used in the test repo.
instname = "LSSTComCamSim"
# The full class name of the same instrument.
instclass = "lsst.obs.lsst.LsstComCamSim"


class RunUtilsFunctionTest(unittest.TestCase):
    """Test the standalone functions in run_utils.
    """

    def test_get_output_run(self):
        instrument = Instrument.from_string(instclass)
        deploy_id = "test-9660137"
        filename = "ApPipe.yaml"
        date = "2023-01-22"
        out_chain = get_output_chain(instrument, date)
        self.assertEqual(out_chain, f"{instname}/prompt/output-2023-01-22")
        preload_run = get_preload_run(instrument, deploy_id, date)
        self.assertEqual(preload_run, f"{instname}/prompt/output-2023-01-22/NoPipeline/{deploy_id}")
        out_run = get_output_run(instrument, deploy_id, filename, date)
        self.assertEqual(out_run, f"{instname}/prompt/output-2023-01-22/ApPipe/{deploy_id}")
