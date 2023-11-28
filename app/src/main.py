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
import json
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
from velocitas_sdk.vehicle_app import VehicleApp, subscribe_topic

# Configure the VehicleApp logger with the necessary log config and level.
logging.setLogRecordFactory(get_opentelemetry_log_factory())
logging.basicConfig(format=get_opentelemetry_log_format())
logging.getLogger().setLevel("DEBUG")
logger = logging.getLogger(__name__)

# Speed
GET_SPEED_REQUEST_TOPIC = "jetracerapp/getSpeed"
GET_SPEED_RESPONSE_TOPIC = "jetracerapp/getSpeed/response"
DATABROKER_SUBSCRIPTION_TOPIC = "jetracerapp/currentSpeed"


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
        self.personal_id = 'jetracer'
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
        # await self.Vehicle.Chassis.SteeringWheel.Angle.subscribe(self.on_angle_change)
        # await self.Vehicle.Powertrain.TractionBattery.StateOfCharge.Current.subscribe(
        #     self.on_battery_change
        # )
        # await self.Vehicle.Powertrain.ElectricMotor.Temperature.subscribe(
        #     self.on_temperature_change
        # )
        # await self.Vehicle.Cabin.HVAC.IsAirConditioningActive.subscribe(
        #     self.on_aircondition_active_change
        # )
        # await self.Vehicle.Cabin.Door.Row1.Left.Window.Position.subscribe(
        #     self.on_position_change
        # )

    async def on_speed_change(self, data: DataPointReply):
        # Get the current vehicle speed value
        self.time = datetime.now()
        self.speed = data.get(self.Vehicle.Speed).value

        # Publishes current speed to MQTT Topic.
        await self.publish_event(
            DATABROKER_SUBSCRIPTION_TOPIC,
            json.dumps({"speed": self.speed}),
        )

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
            self.window_position
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
        """The on_speed_change callback, this will be executed when receiving a new
        vehicle signal updates."""
        # Get the current vehicle speed value from the received DatapointReply.
        # The DatapointReply containes the values of all subscribed DataPoints of
        # the same callback.
        vehicle_speed = data.get(self.Vehicle.Speed).value

        # Do anything with the received value.
        # Example:
        # - Publishes current speed to MQTT Topic (i.e. DATABROKER_SUBSCRIPTION_TOPIC).
        await self.publish_event(
            DATABROKER_SUBSCRIPTION_TOPIC,
            json.dumps({"speed": vehicle_speed}),
        )

    async def on_battery_change(self, data: DataPointReply):
        """The on_speed_change callback, this will be executed when receiving a new
        vehicle signal updates."""
        # Get the current vehicle speed value from the received DatapointReply.
        # The DatapointReply containes the values of all subscribed DataPoints of
        # the same callback.
        vehicle_speed = data.get(self.Vehicle.Speed).value

        # Do anything with the received value.
        # Example:
        # - Publishes current speed to MQTT Topic (i.e. DATABROKER_SUBSCRIPTION_TOPIC).
        await self.publish_event(
            DATABROKER_SUBSCRIPTION_TOPIC,
            json.dumps({"speed": vehicle_speed}),
        )

    async def on_temperature_change(self, data: DataPointReply):
        """The on_speed_change callback, this will be executed when receiving a new
        vehicle signal updates."""
        # Get the current vehicle speed value from the received DatapointReply.
        # The DatapointReply containes the values of all subscribed DataPoints of
        # the same callback.
        vehicle_speed = data.get(self.Vehicle.Speed).value

        # Do anything with the received value.
        # Example:
        # - Publishes current speed to MQTT Topic (i.e. DATABROKER_SUBSCRIPTION_TOPIC).
        await self.publish_event(
            DATABROKER_SUBSCRIPTION_TOPIC,
            json.dumps({"speed": vehicle_speed}),
        )

    async def on_aircondition_active_change(self, data: DataPointReply):
        """The on_speed_change callback, this will be executed when receiving a new
        vehicle signal updates."""
        # Get the current vehicle speed value from the received DatapointReply.
        # The DatapointReply containes the values of all subscribed DataPoints of
        # the same callback.
        vehicle_speed = data.get(self.Vehicle.Speed).value

        # Do anything with the received value.
        # Example:
        # - Publishes current speed to MQTT Topic (i.e. DATABROKER_SUBSCRIPTION_TOPIC).
        await self.publish_event(
            DATABROKER_SUBSCRIPTION_TOPIC,
            json.dumps({"speed": vehicle_speed}),
        )

    async def on_position_change(self, data: DataPointReply):
        """The on_speed_change callback, this will be executed when receiving a new
        vehicle signal updates."""
        # Get the current vehicle speed value from the received DatapointReply.
        # The DatapointReply containes the values of all subscribed DataPoints of
        # the same callback.
        vehicle_speed = data.get(self.Vehicle.Speed).value

        # Do anything with the received value.
        # Example:
        # - Publishes current speed to MQTT Topic (i.e. DATABROKER_SUBSCRIPTION_TOPIC).
        await self.publish_event(
            DATABROKER_SUBSCRIPTION_TOPIC,
            json.dumps({"speed": vehicle_speed}),
        )

    @subscribe_topic(GET_SPEED_REQUEST_TOPIC)
    async def on_get_speed_request_received(self, data: str) -> None:
        """The subscribe_topic annotation is used to subscribe for incoming
        PubSub events, e.g. MQTT event for GET_SPEED_REQUEST_TOPIC.
        """

        # Use the logger with the preferred log level (e.g. debug, info, error, etc)
        logger.debug(
            "PubSub event for the Topic: %s -> is received with the data: %s",
            GET_SPEED_REQUEST_TOPIC,
            data,
        )

        # Getting current speed from VehicleDataBroker using the DataPoint getter.
        vehicle_speed = (await self.Vehicle.Speed.get()).value

        # Do anything with the speed value.
        # Example:
        # - Publishes the vehicle speed to MQTT topic (i.e. GET_SPEED_RESPONSE_TOPIC).
        await self.publish_event(
            GET_SPEED_RESPONSE_TOPIC,
            json.dumps(
                {
                    "result": {
                        "status": 0,
                        "message": f"""Current Speed = {vehicle_speed}""",
                    },
                }
            ),
        )


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
