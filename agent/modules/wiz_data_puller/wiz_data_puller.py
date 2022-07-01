"""Contains a Data Puller class to fetch events from a Wiz server and push them to Devo."""
import logging
import re
import time
from datetime import datetime, timedelta
from typing import Any

from agent.inputs.collector_puller_abstract import CollectorPullerAbstract
from agent.inputs.collector_puller_setup_abstract import CollectorPullerSetupAbstract
# noinspection PyUnresolvedReferences
from agent.modules.wiz_data_puller.exceptions.exceptions import InitVariablesError, PullError
from agent.modules.wiz_data_puller.wiz_data_puller_setup import WizDataPullerSetup

from ratelimiter import RateLimiter

import requests

log = logging.getLogger(__name__)

logging.getLogger("faker.factory").setLevel(logging.WARNING)


class WizDataPuller(CollectorPullerAbstract):
    """A Data Puller class to fetch events from a Wiz server and push them to Devo."""

    FORMAT_VERSION = 1

    def create_setup_instance(
            self,
            setup_class_name: str,
            autosetup_enabled: bool,
            collector_variables: dict) -> CollectorPullerSetupAbstract:
        """
        Call the base class method. See SDK for details.

        :param setup_class_name:
        :param autosetup_enabled:
        :param collector_variables:
        :return:
        """
        setup_class = globals()[setup_class_name]
        return setup_class(
            self,
            collector_variables,
            autosetup_enabled,
            self.input_id,
            self.input_name,
            self.input_config,
            self.input_definition,
            self.service_name,
            self.service_type,
            self.service_config,
            self.service_definition,
            self.module_name,
            self.module_config,
            self.module_definition,
            self.persistence_object,
            self.output_queue,
            self.submodule_name,
            self.submodule_config
        )

    def init_variables(self,
                       input_config: dict,
                       input_definition: dict,
                       service_config: dict,
                       service_definition: dict,
                       module_config: dict,
                       module_definition: dict,
                       submodule_config: Any) -> None:
        """
        Initialize Variables on collector startup before the puller setup.

        This method is run once on collector startup before the puller setup, and is used to initialize variables from
        the internal and user configuration. Initialization is the process of reading the variable and adding it to the
        self.collector variables dict.

        The self.collector_variables dict is then accessible by the pre_pull(), pull(), and puller_setup() methods.

        Initialize each key added to the collector_definitions.yaml and config.yaml files using this method.

        Add link to method responsibilities here
        :param input_config: The input variables received from config.yaml
        :param input_definition:
        :param service_config:
        :param service_definition:
        :param module_config:
        :param module_definition:
        :param submodule_config:
        :return:
        """
        log.info("InitVariables Started")
        # Add variables from the internal config to self.collector_variables
        module_def: dict = self.module_definition["module_properties"]

        # Check for the format of the historic_date field
        historic_date_utc: str = self.input_config.get('historic_date_utc')

        self._init_collector_definitions(input_config, module_def)
        self._validate_time_variables(module_def)
        self._validate_format(module_def)
        self._init_api_base_url(input_config, module_def)
        self._init_credentials(input_config)

        if historic_date_utc:
            self._init_historic_date(historic_date_utc)

        log.info("InitVariables Terminated")

    def _init_api_base_url(self, input_config: dict, module_def: dict) -> None:
        """
        Initialize the API Base URL.

        This method is run once on collector startup and is used to initiate the api_base_base_url
        if the api_base_base_url satisfies the regex format then it is saved in collector_variables
        else raises an initialization error.

        :param input_config: The input variables received from config.yaml
        :return:
        """
        log.info("Initialization of api_base_url has started.")
        api_url_regex: str = self.collector_variables['api_url_regex']
        api_base_url: str = input_config.get("override_api_base_url")
        if not api_base_url:
            log.info("Base url is not provided in the config.yaml. Considering the base url specified in the "
                     "collector definitions")
            api_base_url: str = module_def["api_base_url"]

        if not isinstance(api_base_url, str):
            raise InitVariablesError(18, f'{"api_base_url not of expected type: str"}')
        elif not re.match(api_url_regex, api_base_url):
            msg: str = f'api_base_url must match regex: {api_url_regex}'
            raise InitVariablesError(19, msg)
        self.collector_variables["api_base_url"]: str = api_base_url

        log.info("api_base_url has been initialized")

    def _init_credentials(self, input_config: dict) -> None:
        """
        Initialize the credentials.

        This method is run once on collector startup and is used to initiate the credentials
        if the credentials are present and are of proper format then they are saved in collector_variables
        else an initialization error is raised.

        :param input_config: The input variables received from config.yaml The input variables received from config.yaml
        :return:
        """
        log.info("Initialization of credentials has started.")
        if "credentials" not in input_config:
            raise InitVariablesError(20, 'Required setting, credentials not found in user configuration')
        elif not isinstance(input_config["credentials"], dict):
            raise InitVariablesError(21, 'Required setting, credentials not of expected type: dict')
        else:
            log.debug('Credentials setting found and validated')

        # Check that username is found and of the correct type amd add to self.collector_variables
        if "client_id" not in input_config["credentials"]:
            raise InitVariablesError(22, 'Required setting, client_id not found in user configuration')
        elif not isinstance(input_config["credentials"]["client_id"], str):
            raise InitVariablesError(23, 'Required setting, username not of expected type: str')
        else:
            self.collector_variables["client_id"]: str = input_config["credentials"]["client_id"]

        # Check that password is found and of the expected type amd add to self.collector_variables
        if "client_secret" not in input_config["credentials"]:
            raise InitVariablesError(24, 'Required setting, client_secret not found in user configuration')
        elif not isinstance(input_config["credentials"]["client_secret"], str):
            raise InitVariablesError(25, 'Required setting, client_secret not of expected type: str')
        else:
            self.collector_variables["client_secret"]: str = input_config["credentials"]["client_secret"]

        log.info("credentials have been initialized.")

    def _init_historic_date(self, historic_date_utc: str) -> None:
        """
        Initialize the Historic date.

        if the historic_date_utc is specified in the config.yaml file, this method will check if the
        specified historic date is valid and is in correct format.

        @param historic_date_utc: historic datetime provided in config
        @return:
        """
        if not isinstance(historic_date_utc, str):
            raise InitVariablesError(26, 'Required setting, historic_date_utc not of expected type: str')
        historic_date_time_format: str = self.collector_variables['historic_date_time_format']
        current_utc_time: datetime = datetime.utcnow()
        try:
            # Try to parse the datetime string to make sure it is in the right format
            historic_date_utc: datetime = datetime.strptime(historic_date_utc, historic_date_time_format)
        except Exception as e:
            example_date: str = '2022-02-15T14:32:33.043Z'
            err_msg: str = f'Time format for historic date must be {historic_date_time_format}. e.g. {example_date}'
            log.error(f'Error when parsing historic_date_utc string: {str(e)}')
            raise InitVariablesError(27, err_msg)
        if historic_date_utc > current_utc_time:
            raise InitVariablesError(28, "historic datetime cannot be greater than the present UTC time")

        log.debug("The historic_date_utc is found and is of valid type. Saving it's value in the collector definitions")
        self.collector_variables["historic_date_utc"]: datetime = historic_date_utc  # Can be empty

    def _init_collector_definitions(self, input_config: dict, module_def: dict) -> None:
        """
        Initialize the collector definition variables.

        This method checks the variables in the collector definitions and raises the InitVariablesError if
        the value is not found or type of variable is incorrect.

        @param input_config: The input variables received from config.yaml
        @param module_def: The collector variables dict
        @return:
        """
        log.info("Validating variables in collector definitions Started")
        # check the devo_tag is specified and is of correct type
        if not module_def["devo_tag"]:
            raise InitVariablesError(1, "Devo tag is the required field for sending events to Devo. Specify it in "
                                        "collector definitions")
        elif not isinstance(module_def["devo_tag"], str):
            raise InitVariablesError(2, "Required setting. devo_tag is not of expected type: str")

        log.debug("The devo_tag specified is valid. Saving it's value in the collector definitions")
        self.collector_variables['devo_tag']: str = module_def['devo_tag']

        # check the graphql_query is specified and is of correct type
        if not module_def["graphql_query"]:
            raise InitVariablesError(3, "GraphQL query is the required field for querying issues from Wiz. Specify "
                                        "it in collector definitions")
        elif not isinstance(module_def["graphql_query"], str):
            raise InitVariablesError(4, "Required setting. graphql_query is not of expected type: str")
        log.debug("The graphql_query is found and is of valid type. Saving it's value in the collector definitions")
        self.collector_variables['graphql_query']: str = module_def['graphql_query']

        # check the user_agent is specified and is of correct type
        if not module_def["user_agent"]:
            raise InitVariablesError(5, "user_agent is the required field for passing in headers of Wiz AO]PI calls."
                                        "Specify it in collector definitions")
        elif not isinstance(module_def["user_agent"], str):
            raise InitVariablesError(6, "Required setting. user_agent is not of expected type: str")
        log.debug("The user_agent is found and is of valid type. Saving it's value in the collector definitions")
        self.collector_variables['user_agent']: str = module_def['user_agent']

        # check the flatten_data is specified and is of correct type
        flatten_data: bool = input_config["override_flatten_data"]
        if flatten_data is None:
            log.info("Flatten data is not provided in the config.yaml. Considering the flatten data from collector "
                     "definitions")
            flatten_data: bool = module_def["flatten_data"]
        if not isinstance(flatten_data, bool):
            raise InitVariablesError(7, "Optional setting, flatten_data not of expected type: bool")
        self.collector_variables["flatten_data"]: bool = flatten_data

        log.info("Validating collector Variables is terminated")

    def _validate_format(self, module_def: dict) -> None:
        """
        Validate format of regex and historic datetime format.

        @param module_def: The collector variables dict
        @return:
        """
        # check the api_url_regex is specified and is of correct type
        if not module_def['api_url_regex']:
            raise InitVariablesError(14, "api_url_regex is the required field for validating the base url. Specify "
                                         "it in collector definitions")
        elif not isinstance(module_def["api_url_regex"], str):
            raise InitVariablesError(15, "Required setting. api_url_regex is not of expected type: str")
        log.debug("The api_url_regex is found and is of valid type. Saving it's value in the collector definitions")
        self.collector_variables['api_url_regex']: str = module_def['api_url_regex']

        # check the historic_date_time_format is specified and is of correct type
        if not module_def["historic_date_time_format"]:
            raise InitVariablesError(16, "historic_date_time_format is the required field for validating datetime "
                                         "format. Specify it in collector definitions")
        elif not isinstance(module_def["historic_date_time_format"], str):
            raise InitVariablesError(17, "Required setting. historic_date_time_format is not of expected type: str")
        log.debug("The historic_date_time_format is found and is of valid type. Saving it's value in the "
                  "collector definitions")
        self.collector_variables['historic_date_time_format']: str = module_def['historic_date_time_format']

    def _validate_time_variables(self, module_def: dict) -> None:
        """
        Validate access_token_timeout and default_historic_days.

        @param module_def: The collector variables dict
        @return:
        """
        # check the access_token_timeout is specified and is of correct type
        if not module_def["access_token_timeout"]:
            raise InitVariablesError(8, "access_token_timeout is the required field for checking if the token is "
                                        "expired. Specify it in collector definitions")
        elif not isinstance(module_def["access_token_timeout"], int):
            raise InitVariablesError(9, "Required setting. access_token_timeout is not of expected type: int")
        log.debug("The access_token_timeout is found and is of valid type. Saving it's value in the "
                  "collector definitions")
        self.collector_variables['access_token_timeout']: int = module_def['access_token_timeout']

        # check the default_historic_days is specified and is of correct type
        if not module_def["default_historic_days"]:
            raise InitVariablesError(10, "default_historic_days is the required field in case historic_date_utc is "
                                         "not specified. Specify it in collector definitions")
        elif not isinstance(module_def["default_historic_days"], int):
            raise InitVariablesError(11, "Required setting. default_historic_days is not of expected type: int")
        log.debug("The default_historic_days is found and is of valid type. Saving it's value in the "
                  "collector definitions")
        self.collector_variables['default_historic_days']: int = module_def['default_historic_days']

    def pre_pull(self, retrieving_timestamp: datetime) -> None:
        """
        Set up the puller variables and state required by the pull method.

        In the first run of the collector if the user has specified a datetime in the configuration, the
        time range from the specified datetime to now is considered for fetching issues, else default number
        of days specified in collector_definitions.yaml is used for first poll. In either case the value is
        stored in the persistence object.

        If there is a change in the datetime provided in the configuration, last_polled_timestamp, historic_date_utc
        in the state are updated to the specified datetime. The issues are polled from this changed datetime.

        :param retrieving_timestamp: current datetime in utc
        :return:
        """
        log.info("PrePull Started.")

        state: dict = self.persistence_object.load_state(no_log_traces=True)
        historic_date_utc: datetime = self.collector_variables.get('historic_date_utc')

        if historic_date_utc:
            # The user has specified a historic date, use that for historical polling
            log.debug(f"User has specified {historic_date_utc} as the datetime. Historical polling will consider "
                      f"this datetime")
            last_polled_timestamp: datetime = historic_date_utc
        else:
            # The user has not specified a historic date, use a default of "X" days before now
            default_historic_days: int = self.collector_variables["default_historic_days"]
            log.debug(f"User has not provided datetime in the configuration. Using the default {default_historic_days} "
                      f"days provided in the collector_definitions for historical polling")
            last_polled_timestamp: datetime = retrieving_timestamp - timedelta(days=default_historic_days)

        default_state: dict = {
            "last_polled_timestamp": last_polled_timestamp,
            "historic_date_utc": historic_date_utc,
            "ids_with_same_timestamp": []
        }
        if state is None:
            log.info('No saved state found, initializing...')
            state = default_state

        elif historic_date_utc and (historic_date_utc != state['historic_date_utc']):
            # We have a persistent state. If the 'historic_date_utc' in the state differs from what the user
            # has specified, then we need to reset the state to the default, and save the user specified
            # 'historic_date_utc'. If the 'historic_date_utc' is a date string, this allows us to determine
            # when the user had requested the last reset of the persistence object.
            log.warning("Historic datetime in the persistence object and in the configuration are different."
                        "Updating the value in state with the user specified datetime.\n"
                        f"Next poll will consider {historic_date_utc} to now as the date range")
            state: dict = default_state
            state['last_polled_timestamp']: datetime = historic_date_utc
            state['historic_date_utc']: datetime = historic_date_utc

        self.persistence_object.save_state(object_to_save=state)
        log.warning(f'Saved state loaded: {str(state)}')
        log.info('PrePull Terminated')

    def pull(self, retrieving_timestamp: datetime) -> None:
        """
        Fetch issues from Wiz and send them to Devo.

        The pull method fetches the issues from the last_polled_timestamp(that is created from the last issue
        fetched from the Wiz server) to now with rate limiter and delay seconds, removes the
        duplicate events and sends the issues to devo by adding the @devo_pulling_id

        At the end of the poll, the ids saved in issue_ids_till_page are transferred to unique_ids_per_poll
        and state is updated

        @param retrieving_timestamp: current datetime in utc
        @return:
        """
        log.info("Pull Started")
        # Load state
        state: dict = self.persistence_object.load_state(no_log_traces=True)
        last_polled_issue_timestamp: datetime = state['last_polled_timestamp']
        time_format: str = self.collector_variables['historic_date_time_format']
        last_polled_issue_timestamp_in_iso_format: str = last_polled_issue_timestamp.isoformat().replace("+00:00", "")
        rate_limiter: RateLimiter = RateLimiter(max_calls=self.collector_variables['requests_per_second'], period=1)
        # The variables sent along with the query to filter the results
        variables: dict = {
            'first': 500,
            'filterBy': {
                'createdAt': {
                    'after': last_polled_issue_timestamp_in_iso_format + 'Z'
                },
                'status': [
                    'OPEN',
                    'IN_PROGRESS'
                ]
            },
            'orderBy': {
                'field': 'CREATED_AT',
                'direction': 'ASC'
            }
        }
        log.info(f"Fetching for issues from {last_polled_issue_timestamp_in_iso_format}")
        with rate_limiter:
            result: dict = self._get_wiz_issues(variables)
            log.info(f"Total number of issues in this poll: {result['data']['issues']['totalCount']}")
            events_list: list = result['data']['issues']['nodes']
            unique_events_list: list = self._remove_duplicates(events_list, state)
            self._send_events_in_batches(unique_events_list, retrieving_timestamp)
            if len(unique_events_list) > 0:
                timestamp_from_last_event: str = unique_events_list[-1]["createdAt"]
                state['last_polled_timestamp']: datetime = datetime.strptime(timestamp_from_last_event, time_format)
                self.persistence_object.save_state(object_to_save=state)
                log.warning("State last_polled_timestamp is updated with last fetched issue creation time")
                log.warning(f'Saved state: {str(state)}')
            while result['data']['issues']['pageInfo']['hasNextPage']:
                variables['after']: str = result['data']['issues']['pageInfo']['endCursor']
                result: dict = self._get_wiz_issues(variables)
                events_list: list = result['data']['issues']['nodes']
                unique_events_list: list = self._remove_duplicates(events_list, state)
                self._send_events_in_batches(unique_events_list, retrieving_timestamp)
                if len(unique_events_list) > 0:
                    timestamp_from_last_event: str = unique_events_list[-1]["createdAt"]
                    state['last_polled_timestamp']: datetime = datetime.strptime(timestamp_from_last_event, time_format)
                    self.persistence_object.save_state(object_to_save=state)
                    log.warning("State last_polled_timestamp is updated with last fetched issue creation time")
                    log.warning(f'Saved state: {str(state)}')

        log.info("Pull Terminated")

    def _get_wiz_issues(self, variables: dict) -> dict:
        """
        Fetch issues from Wiz server.

        This method fetches the issues from the Wiz server for a selected time range and
        returns them in the sorted dict. In case of any exception, it raises PullError

        @param variables: payload of the Wiz server
        @return:
        """
        graphql_query: str = self.collector_variables['graphql_query']
        log.info("Requesting Wiz API for issues")

        api_base_url: str = self.collector_variables['api_base_url']
        user_agent: str = self.collector_variables['user_agent']
        access_token: str = self.collector_variables['access_token']

        url: str = api_base_url + "/graphql"
        headers: dict = {'Authorization': 'Bearer ' + access_token,
                         'User-Agent': user_agent}
        try:
            log.debug(f"Sending POST request to Wiz API. URL: {url}")
            log.debug(f"Payload = 'query': {graphql_query}, 'variables' : {variables}")
            response = requests.request("POST", url=url, headers=headers, verify=True,
                                        json={'query': graphql_query, "variables": variables})
            count_of_retries: int = 0
            while count_of_retries < 3:
                response = requests.request("POST", url=url, headers=headers, verify=True,
                                            json={'query': graphql_query, "variables": variables})
                if response.status_code == 429:
                    retry_after: int = response.headers.get("Retry-After", 60)
                    msg: str = "Too many requests are made in short period of time. the API hit the rate limit. " \
                               f"The collector will wait for {retry_after} seconds before making another API call"
                    log.warning(msg)
                    time.sleep(retry_after)
                    count_of_retries += 1
                else:
                    break
            if count_of_retries == 2:
                msg: str = f"After {count_of_retries + 1} retries still getting the too many requests error."
                raise PullError(306, msg)
        except Exception as e:
            msg: str = f"Error occurred while requesting issues from the Wiz server. Error message: {str(e)}"
            raise PullError(300, msg)
        if response.status_code == 200:
            log.info("successfully retried issues from Wiz")
            return response.json()
        elif response.status_code == 401:
            msg: str = f"The access token provided has been expired. New token will be created in the next poll" \
                       f"\nStatus code: 401\nError message: {response.text}"
            raise PullError(301, msg)
        elif response.status_code == 403:
            msg: str = f"The access token does not have necessary permissions to fetch issues from Wiz." \
                       f"\nStatus code: 403\nError message: {response.text}"
            raise PullError(302, msg)
        elif response.status_code == 404:
            msg: str = f"The requested URL {response.url} is not found. The URL may have been depreciated" \
                       f"\nStatus code: 404\nError message: {response.text}"
            raise PullError(303, msg)
        elif response.status_code >= 500:
            msg: str = f"The server has returned {response.status_code} status code. The server may not be available " \
                       f"for fetching issues. Try after sometime. Error message from server: {response.text}"
            raise PullError(304, msg)
        else:
            msg: str = f"Unexpected error occurred while getting issues from the Wiz server" \
                       f"\nStatus code: {response.status_code}\nError message: {response.text}"
            raise PullError(305, msg)

    @staticmethod
    def _remove_duplicates(issues: list, state: dict) -> list:
        """
        Compare the ids received in the poll with the ids stored in states and removes duplicates.

        This method compares the ids received in this Poll to that of already saved ids in the state
        and removes the event if it is already existing.

        All the ids that are fetched in a poll (of all the pages) are stored in the issue_ids_till_page variable

        @param issues: events received from Wiz server
        @param state: persistence object
        @return:
        """
        ids_with_same_timestamp: list = []
        unique_issues: list = []

        log.info("Removing the duplicate issues if present")
        for issue in issues:
            if issue["id"] not in state["ids_with_same_timestamp"]:
                unique_issues.append(issue)
            if issue["createdAt"] == issues[-1]["createdAt"]:
                ids_with_same_timestamp.append(issue["id"])

        log.debug("Updating the state with last polled issue ids which has same Created times")
        state["ids_with_same_timestamp"]: list = ids_with_same_timestamp
        return unique_issues

    @staticmethod
    def _get_flattened_event(event: dict, retrieving_timestamp: datetime) -> dict:
        """
        Flattens the data received.

        If the flatten_data variable is true, this method will flatten the events and
        adds the devo_pulling_id to the events

        @param event: event received from Wiz
        @param retrieving_timestamp: current datetime in utc
        @return:
        """
        node_arr: list = ["control", "entity", "entitySnapshot"]
        for node_name in node_arr:
            if event.get(node_name) is not None:
                for key, value in event[node_name].items():
                    event[node_name + "_" + key] = value
                del event[node_name]

        event["@devo_pulling_id"]: str = str(retrieving_timestamp.timestamp())
        event["is_flattened"]: bool = True
        return event

    def _send_events_in_batches(self, unique_events_list: list, retrieving_timestamp: datetime) -> None:
        """
        Split the list of unique events to multiple batches and sends each batch of events to Devo.

        @param unique_events_list: list of unique events
        @param retrieving_timestamp: current datetime in utc
        @return:
        """
        flatten_data: bool = self.collector_variables["flatten_data"]
        events_list_modified: list = []

        if flatten_data:
            log.info("Flatten data is set to True. Flattening the data and adding 'devo_pulling_id' to events")
            for event in unique_events_list:
                flattened_event: dict = self._get_flattened_event(event, retrieving_timestamp)
                events_list_modified.append(flattened_event)
        else:
            log.info("Flatten data is set to False. Adding 'devo_pulling_id' to events")
            for event in unique_events_list:
                event["@devo_pulling_id"]: str = str(retrieving_timestamp.timestamp())
                event["is_flattened"]: bool = False
                events_list_modified.append(event)

        log.debug("splitting the list of issues into batch of 50 before sending it to Devo")
        number_of_issues_in_batch: int = 50
        number_of_issues: int = len(events_list_modified)

        batch_of_events: list = [events_list_modified[i: i + number_of_issues_in_batch] for i in
                                 range(0, number_of_issues, number_of_issues_in_batch)]

        log.info(f"Sending the issues in batches of {number_of_issues_in_batch} to Devo")
        # Events we get from the API request are sorted. So sending the data to Devo without any sorting function
        for events in batch_of_events:
            self.send_standard_messages(msg_date=retrieving_timestamp,
                                        msg_tag=self.collector_variables["devo_tag"],
                                        list_of_messages=events)
        log.info(f"Sent {len(events_list_modified)} issues to Devo")

    def pull_stop(self) -> None:
        """Not required for this collector."""
        pass

    def pull_pause(self) -> None:
        """Not required for this collector."""
        pass
