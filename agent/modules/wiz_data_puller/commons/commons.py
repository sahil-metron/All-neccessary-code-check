import random
import time
import uuid
from datetime import datetime

import pytz
from faker import Faker


class Utils:

    @staticmethod
    def get_datetime_from_str(timestamp_str: str):
        return datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=pytz.utc)

    @staticmethod
    def get_str_from_datetime(timestamp: datetime):
        if timestamp.tzinfo:
            timestamp = timestamp.astimezone(pytz.utc)
        return timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


class MockRequest:

    def __init__(self, persisted_info, max_number_of_messages_per_page: int):
        self.max_number_of_messages_per_page: int = max_number_of_messages_per_page
        self._last_messages_with_same_timestamp = []
        if persisted_info:
            self._last_messages_with_same_timestamp = persisted_info.get("last_messages_with_same_timestamp", [])

    def get(self, url: str = None, access_token: str = None):
        if access_token is None:
            raise Exception("\"access_token\" is missing")
        if "page=" not in url:
            response = MockResponse(url, self.max_number_of_messages_per_page, self._last_messages_with_same_timestamp)
        else:
            response = MockResponse(url, self.max_number_of_messages_per_page)

        self._last_messages_with_same_timestamp = response.last_messages_with_same_timestamp
        response_simulated_wait = random.random()
        time.sleep(response_simulated_wait)
        return response


class MockResponse:

    def __init__(self, request_url: str, max_number_of_messages_per_page: int, first_messages: list = None):
        self.request_url = request_url
        self.content: list = []
        self._last_messages_with_same_timestamp: list = []
        self._generated_last_message_with_same_timestamp: bool = False
        self.next_url = None
        self.next_url: str = self._calculate_next_url(request_url)
        # minimum_number_of_messages: int = int(max_number_of_messages_per_page * 0.1)
        minimum_number_of_messages: int = 1
        if "page=" not in request_url:
            self.content.extend(first_messages)
        if self.next_url:
            self._generate_example_messages(max_number_of_messages_per_page - len(self.content))
        else:
            self._generate_example_messages(
                minimum_number_of_messages +
                random.randrange(max_number_of_messages_per_page - len(self.content) - minimum_number_of_messages)
            )

    @property
    def last_messages_with_same_timestamp(self):
        return self._last_messages_with_same_timestamp

    @property
    def generated_last_message_with_same_timestamp(self) -> bool:
        return self._generated_last_message_with_same_timestamp

    def _generate_example_messages(self, number_of_messages: int) -> None:
        faker = Faker()
        message_timestamp = None
        generated_last_message_with_same_timestamp: bool = False
        if number_of_messages > 1 and random.choice([True, False]):
            generated_last_message_with_same_timestamp = True
            number_of_messages -= 1

        for _ in range(number_of_messages):
            message_timestamp = Utils.get_str_from_datetime(datetime.now())
            self.content.append(
                {
                    "id": uuid.uuid4().hex,
                    "timestamp": message_timestamp,
                    "name": faker.name(),
                    "address": faker.address()
                }
            )
            if number_of_messages < 100:
                time.sleep(0.001)
        if self.content:
            self._last_messages_with_same_timestamp.append(self.content[-1])
            if generated_last_message_with_same_timestamp:
                self._generated_last_message_with_same_timestamp = True
                self.content.append({
                    "id": uuid.uuid4().hex,
                    "timestamp": message_timestamp,
                    "name": faker.name(),
                    "address": faker.address()
                })
                self._last_messages_with_same_timestamp.append(self.content[-1])

    def _calculate_next_url(self, request_url) -> str:
        next_url = None
        if random.choice([True, False, False]):
            if "page=" in self.request_url:
                request_parts = request_url.split("page=")
                next_url = request_parts[0] + "page=" + str(int(request_parts[1]) + 1)
            else:
                next_url = request_url + "&page=1"
        return next_url
