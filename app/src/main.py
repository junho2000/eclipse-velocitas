# Copyright (c) 2022 Robert Bosch GmbH and Microsoft Corporation
#
# This program and the accompanying materials are made available under the
# terms of the Apache License, Version 2.0 which is available at
# https://www.apache.org/licenses/LICENSE-2.0.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
# SPDX-License-Identifier: Apache-2.0

"""A sample skeleton vehicle app."""
import asyncio
import logging
import signal
from datetime import datetime

import pymysql
from vehicle import Vehicle, vehicle  # type: ignore
from velocitas_sdk.util.log import (  # type: ignore
    get_opentelemetry_log_factory,
    get_opentelemetry_log_format,
)
from velocitas_sdk.vdb.reply import DataPointReply
from velocitas_sdk.vehicle_app import VehicleApp

# Configure the VehicleApp logger with the necessary log config and level.
logging.setLogRecordFactory(get_opentelemetry_log_factory())
logging.basicConfig(format=get_opentelemetry_log_format())
logging.getLogger().setLevel("DEBUG")
logger = logging.getLogger(__name__)


def is_mysql_connected(connection):
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            print("success to connect to MySQL")
            return True
    except pymysql.MySQLError as e:
        print(f"Failed to connect to MySQL: {e}")
        return False


class SampleApp(VehicleApp):
    def __init__(self, vehicle_client: Vehicle):
        super().__init__()
        self.Vehicle = vehicle_client

        # SQL Connection Test
        self.connection = pymysql.connect(
            host="db-hackathon.ciskedsbhsct.us-east-2.rds.amazonaws.com",
            port=3306,
            user="root",
            passwd="12341234",
            db="piracer",
        )
        self.cursor = self.connection.cursor()
        is_mysql_connected(self.connection)

        # Data Set for SQL
        self.data_id = 0
        self.time = datetime.now()
        self.personal_id = "piracer"
        self.speed = 0.0
        self.angle = 0
        self.battery = 0.0
        self.motor_temp = 0
        self.is_aircondition_active = 0
        self.window_position = 0

    async def on_start(self):
        """Run when the vehicle app starts"""
        # Here you can subscribe for the Vehicle Signals update (e.g. Vehicle Speed).
        await self.Vehicle.Speed.subscribe(self.on_speed_change)
        await self.Vehicle.Chassis.SteeringWheel.Angle.subscribe(self.on_angle_change)
        await self.Vehicle.Powertrain.TractionBattery.StateOfCharge.Current.subscribe(
            self.on_current_change
        )
        await self.Vehicle.Powertrain.ElectricMotor.Temperature.subscribe(
            self.on_temperature_change
        )
        await self.Vehicle.Cabin.HVAC.IsAirConditioningActive.subscribe(
            self.on_isairconditioningactive_change
        )
        await self.Vehicle.Cabin.Door.Row1.Left.Window.Position.subscribe(
            self.on_position_change
        )

    async def on_speed_change(self, data: DataPointReply):
        self.time = datetime.now()
        self.speed = data.get(self.Vehicle.Speed).value

        # SQL insert statement.
        insert_stmt = (
            "INSERT INTO piracer_status (data_id, time, car_id, speed, angle, battery, "
            "motor_temp, is_aircondition_active, window_position) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        )

        # Datas to insert
        values_to_insert = (
            self.data_id,
            self.time,
            self.personal_id,
            self.speed,
            self.angle,
            self.battery,
            self.motor_temp,
            self.is_aircondition_active,
            self.window_position,
        )

        # Insert at SQL
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(insert_stmt, values_to_insert)
                self.connection.commit()
                logger.info("Speed data inserted successfully")
                self.data_id += 1
        except pymysql.MySQLError as e:
            logger.error(f"Failed to insert speed data into database: {e}")
            self.connection.rollback()

    async def on_angle_change(self, data: DataPointReply):
        self.angle = data.get(self.Vehicle.Chassis.SteeringWheel.Angle).value

    async def on_current_change(self, data: DataPointReply):
        self.battery = data.get(
            self.Vehicle.Powertrain.TractionBattery.StateOfCharge.Current
        ).value

    async def on_temperature_change(self, data: DataPointReply):
        self.motor_temp = data.get(
            self.Vehicle.Powertrain.ElectricMotor.Temperature
        ).value

    async def on_isairconditioningactive_change(self, data: DataPointReply):
        self.is_aircondition_active = data.get(
            self.Vehicle.Cabin.HVAC.IsAirConditioningActive
        ).value

    async def on_position_change(self, data: DataPointReply):
        self.window_position = data.get(
            self.Vehicle.Cabin.Door.Row1.Left.Window.Position
        ).value


async def main():
    """Main function"""
    logger.info("Starting SampleApp...")
    # Constructing SampleApp and running it.
    vehicle_app = SampleApp(vehicle)
    await vehicle_app.run()


LOOP = asyncio.get_event_loop()
LOOP.add_signal_handler(signal.SIGTERM, LOOP.stop)
LOOP.run_until_complete(main())
LOOP.close()
