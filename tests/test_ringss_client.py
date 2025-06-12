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

import asyncio
import contextlib
import logging
import pathlib
import sqlite3
import tempfile
import types
import unittest
from collections.abc import AsyncGenerator
from datetime import datetime, timezone
from typing import Any

from astropy.table import Table
from astropy.table.row import Row
from astropy.time import Time
from lsst.ts import salobj
from lsst.ts.ess.ringss import RingssDataClient
from lsst.ts.utils import tai_from_utc

TIMEOUT = 10


class RingssClientTestCase(unittest.IsolatedAsyncioTestCase):
    async def insert_row_at_time(self, row: Row, db_path: str) -> None:
        """Inserts a row into the sqlite database.

        To simulate the real behavior of the RINGSS
        database, this function waits until the row's
        timestamp is in the past, and only then does
        the insert.
        """
        row_time = datetime.fromisoformat(row["time"]).replace(tzinfo=timezone.utc)

        while datetime.now(timezone.utc) < row_time:
            await asyncio.sleep(0.05)

        values = tuple(str(row[col]) for col in row.colnames)
        placeholders = ", ".join("?" * len(values))

        conn = sqlite3.connect(db_path, uri=True)
        cursor = conn.cursor()
        self.log.debug(f"Inserting hrNum={row['star']}")
        cursor.execute(f"INSERT INTO cpdata_ringss VALUES ({placeholders})", values)
        conn.commit()
        conn.close()

    def setUp(self) -> None:
        self.log = logging.getLogger()

    @contextlib.asynccontextmanager
    async def make_database(self) -> AsyncGenerator[str, None]:
        with tempfile.NamedTemporaryFile(suffix=".sqlite") as tmp_file:
            db_path = tmp_file.name
            ecsv_path = pathlib.Path(__file__).parent / "ringss.ecsv"

            table = Table.read(ecsv_path, format="ascii.ecsv")

            delta_time = Time.now() - Time("2025-02-01T00:00:00.000")
            table["time"] += delta_time
            table["time"] = table["time"].iso

            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            create_table_query = """
                CREATE TABLE cpdata_ringss (
                    time TEXT,
                    star INTEGER,
                    zen REAL,
                    flux REAL,
                    see2 REAL,
                    see REAL,
                    fsee REAL,
                    wind REAL,
                    tau0 REAL,
                    theta0 REAL,
                    totvar REAL,
                    erms REAL,
                    J0 REAL,
                    J025 REAL,
                    J05 REAL,
                    J1 REAL,
                    J2 REAL,
                    J4 REAL,
                    J8 REAL,
                    J16 REAL
                );
            """
            cursor.execute(create_table_query)

            conn.commit()
            conn.close()

            # Make the rows load only when each one's timestamp is before
            # the current time.
            tasks = [
                asyncio.create_task(self.insert_row_at_time(row, db_path))
                for row in table
            ]

            try:
                yield db_path
            finally:
                for task in tasks:
                    task.cancel()
                try:
                    await asyncio.gather(*tasks)
                except asyncio.CancelledError:
                    pass

    @contextlib.asynccontextmanager
    async def make_topics(self) -> AsyncGenerator[types.SimpleNamespace, None]:
        if hasattr(salobj, "set_test_topic_subname"):
            salobj.set_test_topic_subname()
        else:
            salobj.set_random_lsst_dds_partition_prefix()
        async with salobj.make_mock_write_topics(
            name="ESS",
            attr_names=["evt_ringssMeasurement"],
        ) as topics:
            yield topics

    def validate_config(self, **kwargs: Any) -> types.SimpleNamespace:
        config_schema = RingssDataClient.get_config_schema()
        validator = salobj.DefaultingValidator(config_schema)
        config = validator.validate(kwargs)
        return types.SimpleNamespace(**config)

    async def test_constructor_good_minimal(self) -> None:
        """Construct with a minimal configuration object."""
        async with self.make_topics() as topics:
            config = self.validate_config(
                db_uri="mysql://{AN_ENVIRONMENT_VARIABLE}localhost/db",
                table_name="mytable",
                poll_interval=10,
                max_read_timeouts=5,
                connect_timeout=1.0,
            )
            data_client = RingssDataClient(
                config=config,
                topics=topics,
                log=self.log,
            )
            assert data_client.simulation_mode == 0
            assert isinstance(data_client.log, logging.Logger)
            assert config.table_name in data_client.get_sql_query()
            assert config.db_uri in str(data_client)

    async def test_constructor_good_full(self) -> None:
        """Construct with a fully populated configuration object."""
        async with self.make_topics() as topics:
            config = self.validate_config(
                db_uri="mysql://{AN_ENVIRONMENT_VARIABLE}localhost/db",
                table_name="mytable",
                poll_interval=123,
                max_read_timeouts=99,
                connect_timeout=1.0,
            )
            data_client = RingssDataClient(
                config=config,
                topics=topics,
                log=self.log,
            )
            assert data_client.simulation_mode == 0
            assert isinstance(data_client.log, logging.Logger)
            assert data_client.poll_interval == config.poll_interval

    async def test_simulation_event(self) -> None:
        """Collect an event in simulation mode."""
        async with self.make_topics() as topics:
            config = self.validate_config(
                db_uri="mysql://{AN_ENVIRONMENT_VARIABLE}localhost/db",
                table_name="mytable",
                poll_interval=0.1,
                max_read_timeouts=5,
                connect_timeout=1.0,
            )
            data_client = RingssDataClient(
                config=config,
                topics=topics,
                log=self.log,
                simulation_mode=1,
            )

            await data_client.start()
            self.assertFalse(data_client.run_task.done())

            for i in range(100):
                await asyncio.sleep(TIMEOUT / 100)
                if topics.evt_ringssMeasurement.has_data:
                    current_time = tai_from_utc(Time.now())
                    break
            else:
                self.fail("evt_ringssMeasurement was not emitted.")

            await data_client.stop()
            try:
                await asyncio.wait_for(data_client.run_task, timeout=TIMEOUT)
            except asyncio.CancelledError:
                pass

            event = topics.evt_ringssMeasurement.data_list[-1]
            self.assertAlmostEqual(event.timestamp, current_time, 0)
            self.assertEqual(event.hrNum, 1234)

    async def test_sqlite_database(self) -> None:
        async with self.make_database() as db_path:
            async with self.make_topics() as topics:
                config = self.validate_config(
                    db_uri=f"sqlite+aiosqlite:///{db_path}",
                    table_name="cpdata_ringss",
                    poll_interval=0.1,
                    max_read_timeouts=5,
                    connect_timeout=1.0,
                )
                data_client = RingssDataClient(
                    config=config,
                    topics=topics,
                    log=self.log,
                )

                await data_client.start()
                assert not data_client.run_task.done()

                n_events_expected = 5

                # Allow plenty of time for all expected events
                # plus more for extra events that shouldn't
                # be there, if any.
                await asyncio.sleep(n_events_expected + TIMEOUT + 1)
                assert len(topics.evt_ringssMeasurement.data_list) == n_events_expected

                # The five emitted events should match the last 5 stars in the
                # database. The first five are not emitted because they have
                # timestamps before the start of the data client.
                for index, hrNum in enumerate(
                    [evt.hrNum for evt in topics.evt_ringssMeasurement.data_list]
                ):
                    assert hrNum == index + 6

                # The events should be sent about 1 second apart.
                send_time = [
                    evt.private_sndStamp
                    for evt in topics.evt_ringssMeasurement.data_list
                ]
                delta_time = [
                    send_time[i + 1] - send_time[i] for i in range(len(send_time) - 1)
                ]
                assert min(delta_time) > 0.8
                assert max(delta_time) < 1.2

                await data_client.stop()
                await data_client.run_task
