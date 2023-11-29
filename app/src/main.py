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
DATABROKER_SUBSCRIPTION_TOPIC_SPEED = "jetracerapp/currentSpeed"

# Angle
GET_ANGLE_REQUEST_TOPIC = "jetracerapp/getAngle"
GET_ANGLE_RESPONSE_TOPIC = "jetracerapp/getAngle/response"
DATABROKER_SUBSCRIPTION_TOPIC_ANGLE = "jetracerapp/currentAngle"

# Battery
GET_BATTERY_REQUEST_TOPIC = "jetracerapp/getBattery"
GET_BATTERY_RESPONSE_TOPIC = "jetracerapp/getBattery/response"
DATABROKER_SUBSCRIPTION_TOPIC_BATTERY = "jetracerapp/currentBattery"

# Temperature
GET_TEMP_REQUEST_TOPIC = "jetracerapp/getTemp"
GET_TEMP_RESPONSE_TOPIC = "jetracerapp/getTemp/response"
DATABROKER_SUBSCRIPTION_TOPIC_TEMP = "jetracerapp/currentTemp"

# Air
GET_AIR_REQUEST_TOPIC = "jetracerapp/getAir"
GET_AIR_RESPONSE_TOPIC = "jetracerapp/getAir/response"
DATABROKER_SUBSCRIPTION_TOPIC_AIR = "jetracerapp/currentAir"

# Window
GET_WINDOW_REQUEST_TOPIC = "jetracerapp/getWindow"
GET_WINDOW_RESPONSE_TOPIC = "jetracerapp/getWindow/response"
DATABROKER_SUBSCRIPTION_TOPIC_WINDOW = "jetracerapp/currentWindow"


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
        self.personal_id = "jetracer"
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
        #     self.on_current_change
        # )
        # await self.Vehicle.Powertrain.ElectricMotor.Temperature.subscribe(
        #     self.on_temperature_change
        # )
        # await self.Vehicle.Cabin.HVAC.IsAirConditioningActive.subscribe(
        #     self.on_isairconditioningactive_change
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
            DATABROKER_SUBSCRIPTION_TOPIC_SPEED,
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
        """The on_speed_change callback, this will be executed when receiving a new
        vehicle signal updates."""
        # Get the current vehicle speed value from the received DatapointReply.
        # The DatapointReply containes the values of all subscribed DataPoints of
        # the same callback.
        self.angle = data.get(self.Vehicle.Speed).value

        # Do anything with the received value.
        # Example:
        # - Publishes current speed to MQTT Topic (i.e. DATABROKER_SUBSCRIPTION_TOPIC).
        await self.publish_event(
            DATABROKER_SUBSCRIPTION_TOPIC_ANGLE,
            json.dumps({"angle": self.angle}),
        )

    async def on_current_change(self, data: DataPointReply):
        """The on_speed_change callback, this will be executed when receiving a new
        vehicle signal updates."""
        # Get the current vehicle speed value from the received DatapointReply.
        # The DatapointReply containes the values of all subscribed DataPoints of
        # the same callback.
        self.battery = data.get(self.Vehicle.Speed).value

        # Do anything with the received value.
        # Example:
        # - Publishes current speed to MQTT Topic (i.e. DATABROKER_SUBSCRIPTION_TOPIC).
        await self.publish_event(
            DATABROKER_SUBSCRIPTION_TOPIC_BATTERY,
            json.dumps({"battery": self.battery}),
        )

    async def on_temperature_change(self, data: DataPointReply):
        """The on_speed_change callback, this will be executed when receiving a new
        vehicle signal updates."""
        # Get the current vehicle speed value from the received DatapointReply.
        # The DatapointReply containes the values of all subscribed DataPoints of
        # the same callback.
        self.motor_temp = data.get(self.Vehicle.Speed).value

        # Do anything with the received value.
        # Example:
        # - Publishes current speed to MQTT Topic (i.e. DATABROKER_SUBSCRIPTION_TOPIC).
        await self.publish_event(
            DATABROKER_SUBSCRIPTION_TOPIC_TEMP,
            json.dumps({"motor temperature": self.motor_temp}),
        )

    async def on_isairconditioningactive_change(self, data: DataPointReply):
        """The on_speed_change callback, this will be executed when receiving a new
        vehicle signal updates."""
        # Get the current vehicle speed value from the received DatapointReply.
        # The DatapointReply containes the values of all subscribed DataPoints of
        # the same callback.
        self.is_aircondition_active = data.get(self.Vehicle.Speed).value

        # Do anything with the received value.
        # Example:
        # - Publishes current speed to MQTT Topic (i.e. DATABROKER_SUBSCRIPTION_TOPIC).
        await self.publish_event(
            DATABROKER_SUBSCRIPTION_TOPIC_AIR,
            json.dumps({"is aircondition active": self.is_aircondition_active}),
        )

    async def on_position_change(self, data: DataPointReply):
        """The on_speed_change callback, this will be executed when receiving a new
        vehicle signal updates."""
        # Get the current vehicle speed value from the received DatapointReply.
        # The DatapointReply containes the values of all subscribed DataPoints of
        # the same callback.
        self.window_position = data.get(self.Vehicle.Speed).value

        # Do anything with the received value.
        # Example:
        # - Publishes current speed to MQTT Topic (i.e. DATABROKER_SUBSCRIPTION_TOPIC).
        await self.publish_event(
            DATABROKER_SUBSCRIPTION_TOPIC_WINDOW,
            json.dumps({"speed": self.window_position}),
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

    @subscribe_topic(GET_ANGLE_REQUEST_TOPIC)
    async def on_get_angle_request_received(self, data: str) -> None:
        logger.debug(
            "PubSub event for the Topic: %s -> is received with the data: %s",
            GET_ANGLE_REQUEST_TOPIC,
            data,
        )

        vehicle_angle = (await self.Vehicle.Speed.get()).value

        await self.publish_event(
            GET_ANGLE_RESPONSE_TOPIC,
            json.dumps(
                {
                    "result": {
                        "status": 0,
                        "message": f"""Current Angle = {vehicle_angle}""",
                    },
                }
            ),
        )

    @subscribe_topic(GET_BATTERY_REQUEST_TOPIC)
    async def on_get_current_request_received(self, data: str) -> None:
        logger.debug(
            "PubSub event for the Topic: %s -> is received with the data: %s",
            GET_BATTERY_REQUEST_TOPIC,
            data,
        )

        vehicle_battery = (await self.Vehicle.Speed.get()).value

        await self.publish_event(
            GET_ANGLE_RESPONSE_TOPIC,
            json.dumps(
                {
                    "result": {
                        "status": 0,
                        "message": f"""Current Battery = {vehicle_battery}""",
                    },
                }
            ),
        )

    @subscribe_topic(GET_TEMP_REQUEST_TOPIC)
    async def on_get_temperature_request_received(self, data: str) -> None:
        logger.debug(
            "PubSub event for the Topic: %s -> is received with the data: %s",
            GET_TEMP_REQUEST_TOPIC,
            data,
        )

        vehicle_temperature = (await self.Vehicle.Speed.get()).value

        await self.publish_event(
            GET_TEMP_RESPONSE_TOPIC,
            json.dumps(
                {
                    "result": {
                        "status": 0,
                        "message": f"""Current Temperature = {vehicle_temperature}""",
                    },
                }
            ),
        )

    @subscribe_topic(GET_AIR_REQUEST_TOPIC)
    async def on_get_isairconditioningactive_request_received(self, data: str) -> None:
        logger.debug(
            "PubSub event for the Topic: %s -> is received with the data: %s",
            GET_AIR_REQUEST_TOPIC,
            data,
        )

        vehicle_airconditioning = (await self.Vehicle.Speed.get()).value

        await self.publish_event(
            GET_AIR_RESPONSE_TOPIC,
            json.dumps(
                {
                    "result": {
                        "status": 0,
                        "message": f"""Current AirC = {vehicle_airconditioning}""",
                    },
                }
            ),
        )

    @subscribe_topic(GET_WINDOW_REQUEST_TOPIC)
    async def on_get_position_request_received(self, data: str) -> None:
        logger.debug(
            "PubSub event for the Topic: %s -> is received with the data: %s",
            GET_WINDOW_REQUEST_TOPIC,
            data,
        )

        vehicle_window = (await self.Vehicle.Speed.get()).value

        await self.publish_event(
            GET_WINDOW_RESPONSE_TOPIC,
            json.dumps(
                {
                    "result": {
                        "status": 0,
                        "message": f"""Current Window Position = {vehicle_window}""",
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
