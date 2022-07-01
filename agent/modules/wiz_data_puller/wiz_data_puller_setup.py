"""Contains a Data Puller setup class for Wiz."""
import logging
from datetime import datetime, timedelta

from agent.inputs.collector_puller_setup_abstract import CollectorPullerSetupAbstract
from agent.modules.wiz_data_puller.exceptions.exceptions import SetupError

import requests

log = logging.getLogger(__name__)


class WizDataPullerSetup(CollectorPullerSetupAbstract):
    """This class implements the "setup" method for wiz data puller service."""

    def setup(self, execution_timestamp: datetime) -> None:
        """
        Create the access token if it gets expired.

        The setup method creates a new access token in the first run of the collector. If a token is already
        present, it checks if the is valid. If the token is invalid it will create a new token and stores in the
        collector_variables. If the token is valid it will use the same token for puller

        @param execution_timestamp: current datetime in utc
        @return:
        """
        log_message = "Puller Setup Started"
        log.info(log_message)
        epoch_time: int = int(execution_timestamp.timestamp())
        buffer_seconds: int = 120
        epoch_timeout: int = self.collector_variables.get("access_token_timeout")
        if not self.collector_variables.get("access_token"):
            generate_new_token = True
            log.info("This is the first run of collector. Generating the access token")
        elif epoch_timeout and epoch_timeout > epoch_time + buffer_seconds:
            generate_new_token = False
            log.info("Previously generated token is still valid. Skipping the generation of new access token ")
        else:
            generate_new_token = True
            log.info("Token has expired. Generating the new one")

        if generate_new_token:
            access_token: str = self._get_access_token()
            self.collector_variables["access_token"]: str = access_token

        does_credentials_have_permissions: bool = \
            self._check_credential_permissions(execution_timestamp, self.collector_variables["access_token"])

        if does_credentials_have_permissions is False:
            msg: str = "The credentials does not have valid permissions to fetch issues from the Wiz server"
            raise SetupError(105, msg)

        log.info("Puller Setup Terminated")

    def _get_access_token(self) -> str:
        """
        Generate new access token in case of a fresh run of the collector or when the access token is expired.

        It will raise the SetupError in case of failure in creation of the token
        @return:
        """
        client_id: str = self.collector_variables["client_id"]
        client_secret: str = self.collector_variables["client_secret"]
        api_base_url: str = self.collector_variables["api_base_url"]
        auth_endpoint: str = self._get_auth_url(api_base_url)
        headers: dict = {
            "content-type": "application/x-www-form-urlencoded"
        }
        payload: str = f"grant_type=client_credentials&client_id={client_id}&" + \
                       f"client_secret={client_secret}&audience=beyond-api"

        try:
            response: requests.Response = requests.post(auth_endpoint + "/oauth/token", data=payload, headers=headers)
        except Exception as e:
            msg: str = f"Error occurred while requesting access token from the Wiz server. Error message: {str(e)}"
            raise SetupError(100, msg)
        if response.status_code == 200:
            log.info("successfully generated new access token")
            return response.json()["access_token"]
        elif response.status_code == 401:
            msg: str = f"The credentials provided in the config.yaml are incorrect. Please provide the correct " \
                       f"credentials.\nStatus code: 401\nError type: {response.json()['error']}\n " \
                       f"Error message: {response.json()['error_description']}"
            raise SetupError(101, msg)
        elif response.status_code == 403:
            msg: str = f"The credentials provided in the config.yaml file does not have necessary permissions to " \
                       f"create access token.\nStatus code: 403\nError type: {response.json()['error']}\n" \
                       f"Error message: {response.json()['error_description']}"
            raise SetupError(102, msg)
        elif response.status_code == 404:
            msg: str = f"The requested URL {response.url} is not found. The URL may have been depreciated" \
                       f"\nStatus code: 404\nError message: {response.text}"
            raise SetupError(103, msg)
        else:
            msg: str = f"Unexpected error occurred while getting access token from the Wiz server" \
                       f"\nStatus code: {response.status_code}\nError message: {response.text}"
            raise SetupError(104, msg)

    def _check_credential_permissions(self, execution_timestamp: datetime, access_token: str) -> bool:
        """
        Check if the credentials have valid permissions.

        The method will call the Wiz endpoint to get issue that may or may not be generated in the last 5
        seconds. If the response status code is 200 the credentials have the required permissions.

        @param execution_timestamp: current datetime in utc
        @param access_token: JWT token for making API calls
        @return:
        """
        timestamp = execution_timestamp - timedelta(seconds=5)
        timestamp_in_iso_format = timestamp.isoformat().replace("+00:00", "Z")
        graphql_query: str = self.collector_variables['graphql_query']
        variables: dict = {
            'first': 1,
            'filterBy': {
                'createdAt': {
                    'after': timestamp_in_iso_format
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
        api_base_url: str = self.collector_variables["api_base_url"]
        user_agent: str = self.collector_variables['user_agent']
        url: str = api_base_url + "/graphql"
        headers: dict = {'Authorization': 'Bearer ' + access_token,
                         'User-Agent': user_agent}
        try:
            response: requests.Response = requests.request("POST", url=url, headers=headers, verify=True,
                                                           json={'query': graphql_query, "variables": variables})
        except Exception as e:
            msg: str = f"Failed to check if the provided credentials have valid permissions." \
                       f"Error message: {str(e)}"
            raise SetupError(106, msg)
        if response.status_code == 200:
            log.info("The credentials provided in the configuration have required permissions to request issues "
                     "from Wiz server")
            return True
        return False

    @staticmethod
    def _get_auth_url(api_base_url: str) -> str:
        """
        Get the access token url based on the given base url.

        @param api_base_url: base URL of Wiz server
        @return:
        """
        log.info("Getting the auth token url based on provided api_base_url")
        if "gov.wiz.io" in api_base_url:
            return "https://auth0.gov.wiz.io"
        elif "test.wiz.io" in api_base_url:
            return "https://auth0.test.wiz.io"
        else:
            log.info("Using default Authentication Domain auth.wiz.io for fetching Access Token")
            return "https://auth.wiz.io"
