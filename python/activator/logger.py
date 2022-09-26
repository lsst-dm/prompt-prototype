# This file is part of prompt_prototype.
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

__all__ = ["GCloudStructuredLogFormatter"]

import logging


class GCloudStructuredLogFormatter(logging.Formatter):
    """A formatter that can be parsed by the Google Cloud logging agent.

    The formatter's output is a JSON-encoded message containing keywords
    recognized by the logging agent.

    Parameters
    ----------
    fmt : `str`
        A log output format string compatible with `str.format`.
    """
    def __init__(self, fmt=None):
        super().__init__(fmt=fmt, style="{")