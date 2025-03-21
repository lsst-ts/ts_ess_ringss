# This file is part of ts_ess_common.
#
# Developed for the Vera C. Rubin Observatory Telescope and Site Systems.
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

__all__ = ["RingssDataClient"]

import datetime
import typing

from astropy.time import Time
from lsst.ts.utils import tai_from_utc

from .sqlalchemy_data_client import SqlalchemyDataClient


class RingssDataClient(SqlalchemyDataClient):
    """A data client for the SOAR RINGSS database.

    The database contains rows that are minimally
    adjusted to fit with the Rubin schema, and then
    provided to the EFD.

    Parameters
    ----------
    config : types.SimpleNamespace
        The configuration, after validation by the schema returned
        by `get_config_schema` and conversion to a types.SimpleNamespace.
    topics : `salobj.Controller`
        The telemetry topics this data client can write,
        as a struct with attributes such as ``evt_ringssMeasurement``.
    log : `logging.Logger`
        Logger.
    simulation_mode : `int`, optional
        Simulation mode; 0 for normal operation.
    """

    def get_sql_query(self) -> str:
        return f"SELECT * FROM {self.table_name} WHERE time > :t0"

    def get_simulation_data(self) -> dict[str, typing.Any]:
        return dict(
            time=datetime.datetime.utcnow(),
            star=1234,
            zen=10.0,
            flux=100000.5,
            see2=0.9,
            see=0.8,
            fsee=1.1,
            wind=5.5,
            tau0=12.1,
            theta0=2.2,
            totvar=0.035,
            erms=0.3,
            J0=2.2,
            J025=0.0,
            J05=0.08,
            J1=0,
            J2=0,
            J4=0,
            J8=0.4,
            J16=0.3,
        )

    async def process(self, query_row: dict[str, typing.Any]) -> None:
        timestamp = Time(query_row["time"])
        ringss_event = {
            "timestamp": tai_from_utc(timestamp),
            "hrNum": query_row["star"],
            "zenithDistance": query_row["zen"],
            "flux": query_row["flux"],
            "fwhmScintillation": query_row["see"],
            "fwhmSector": query_row["see2"],
            "fwhmFree": query_row["fsee"],
            "wind": query_row["wind"],
            "tau0": query_row["tau0"],
            "theta0": query_row["theta0"],
            "totalVariance": query_row["totvar"],
            "eRMS": query_row["erms"],
            "turbulenceProfiles": [
                query_row["J0"] * 1e-13,
                query_row["J025"] * 1e-13,
                query_row["J05"] * 1e-13,
                query_row["J1"] * 1e-13,
                query_row["J2"] * 1e-13,
                query_row["J4"] * 1e-13,
                query_row["J8"] * 1e-13,
                query_row["J16"] * 1e-13,
            ],
        }
        await self.topics.evt_ringssMeasurement.set_write(**ringss_event)
        self.log.debug("Emitted ringssMeasurement.")
        self.last_timestamp = max((self.last_timestamp, timestamp))
        self.wrote_event.set()
