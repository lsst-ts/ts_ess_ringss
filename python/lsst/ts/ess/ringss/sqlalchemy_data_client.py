# This file is part of ts_ess_ringss.
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

__all__ = ["SqlalchemyDataClient"]

import abc
import asyncio
import logging
import os
import random
import re
import types
from typing import Any, Sequence
from urllib.parse import quote

import backoff
import sqlalchemy
import yaml
from astropy.time import Time
from lsst.ts import salobj
from lsst.ts.ess.common.data_client import BaseReadLoopDataClient
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine


class SqlalchemyDataClient(BaseReadLoopDataClient):
    """Get environmental data via a connection to a database with sqlalchemy.

    This is an abstract class. SQL database data sources may derive from
    this class with behavior specific to the database they represent.

    Parameters
    ----------
    config : types.SimpleNamespace
        The configuration, after validation by the schema returned
        by `get_config_schema` and conversion to a types.SimpleNamespace.
    topics : `salobj.Controller` or `types.SimpleNamespace`
        The telemetry topics this data client can write,
        as a struct with attributes such as ``evt_ringssMeasurement``.
    log : `logging.Logger`
        Logger.
    simulation_mode : `int`, optional
        Simulation mode; 0 for normal operation.
    """

    def __init__(
        self,
        config: types.SimpleNamespace,
        topics: salobj.Controller | types.SimpleNamespace,
        log: logging.Logger,
        simulation_mode: int = 0,
    ) -> None:
        self.engine: AsyncEngine | None = None
        self.last_timestamp = Time(Time.now(), format="iso")

        super().__init__(
            config=config, topics=topics, log=log, simulation_mode=simulation_mode
        )
        self.configure()

    @classmethod
    def get_config_schema(cls) -> dict[str, Any]:
        return yaml.safe_load(
            """---
$schema: http://json-schema.org/draft-07/schema#
description: Schema for TCP/IP sensors.
type: object
properties:
  db_uri:
    description: URL connection string for the database, e.g., "mysql://{DB_USER}:{DB_PASS}@localhost/db".
    type: string
    format: uri-template
  table_name:
    description: The name of the table in the database to be queried.
    type: string
  poll_interval:
    description: Time between queries to the database
    type: number
    default: 10
  max_read_timeouts:
    description: Maximum timeouts allowed before causing the CSC to fault.
    type: number
    default: 5
required:
  - db_uri
  - table_name
additionalProperties: false
"""
        )

    def configure(self) -> None:
        environment_variable_list = re.findall(r"\{(.*?)\}", self.config.db_uri)
        environment_variables = {
            key: quote(os.getenv(key, "")) for key in environment_variable_list
        }
        self.db_uri = self.config.db_uri.format(**environment_variables)

    @property
    def poll_interval(self) -> float:
        return self.config.poll_interval

    @property
    def table_name(self) -> str:
        return self.config.table_name

    def descr(self) -> str:
        return self.config.db_uri

    async def connect(self) -> None:
        self.log.debug("SqlalchemyDataClient.connect()")
        if not self.simulation_mode:
            self.engine = create_async_engine(self.db_uri)
        await super().connect()

    async def disconnect(self) -> None:
        self.log.debug("SqlalchemyDataClient.disconnect()")
        if self.engine is not None:
            await self.engine.dispose()
        await super().disconnect()

    @abc.abstractmethod
    def get_sql_query(self) -> str:
        """Return the sql query to send to the server.

        Returns
        str
            The SQL query to be used.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    async def process(self, query_row: dict[str, Any]) -> None:
        """Handle the result of a query and process it.

        This function receives a dictionary mapping of a row returned from
        the SQL server. The data should be processed as needed and then
        telemetry or an event emitted.

        """
        raise NotImplementedError()

    @abc.abstractmethod
    def get_simulation_data(self) -> dict[str, Any]:
        """Returns simulated data from the database."""
        raise NotImplementedError()

    @backoff.on_exception(backoff.expo, OperationalError, max_tries=2)
    async def execute_sql_query(self) -> Sequence[sqlalchemy.engine.row.Row]:
        if self.engine is None:
            raise RuntimeError("Not connected.")

        async with self.engine.connect() as conn:
            t0 = (
                self.last_timestamp.iso
                if self.simulation_mode == 1
                else self.last_timestamp.datetime
            )
            stmt = sqlalchemy.text(self.get_sql_query()).bindparams(t0=t0)
            result = await conn.execute(stmt)
            return result.fetchall()

    async def read_data(self) -> None:
        self.log.debug("SqlalchemyDataClient.read_data()")
        if self.simulation_mode == 0:
            rows = await self.execute_sql_query()
            for row in rows:
                await self.process(dict(row._mapping))
        else:
            if random.randint(1, 6) == 6:
                row = self.get_simulation_data()
                await self.process(row)

        await asyncio.sleep(self.poll_interval)
