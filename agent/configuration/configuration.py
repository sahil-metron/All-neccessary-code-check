import json
import logging
import os
import pathlib
import re
import yaml
from collections import namedtuple
from yaml.parser import ParserError

from agent.configuration.defaults import GLOBAL_DEFAULTS
from agent.configuration.exceptions import CollectorConfigurationException
from agent.outputs import outputs

# Global definitions
log = logging.getLogger(__name__)

# Collections
try:
    # Only works in python 3.7
    Validation = namedtuple('ConfigValidation', ['validated', 'default'], defaults=[None, None])
except TypeError:
    # Works in python 3.6
    Validation = namedtuple('ConfigValidation', ['validated', 'default'])
    Validation.__new__.__defaults__ = (None,) * len(Validation._fields)


class NoDatesSafeLoader(yaml.SafeLoader):
    """ Custom loader for yaml files """

    @classmethod
    def remove_implicit_resolver(cls, tag_to_remove):
        """
        Remove implicit resolvers for a particular tag

        Takes care not to modify resolvers in super classes.

        We want to load datetime objects as strings, not date objects, because we
        go on to serialise as json which doesn't have the advanced types
        of yaml, and leads to incompatibilities down the track.
        """
        if 'yaml_implicit_resolvers' not in cls.__dict__:
            cls.yaml_implicit_resolvers = cls.yaml_implicit_resolvers.copy()

        for first_letter, mappings in cls.yaml_implicit_resolvers.items():
            cls.yaml_implicit_resolvers[first_letter] = \
                [(tag, regexp) for tag, regexp in mappings if tag != tag_to_remove]


NoDatesSafeLoader.remove_implicit_resolver('tag:yaml.org,2002:timestamp')


class CollectorConfiguration:
    """ This class is used to build and store the Collector Configuration inside and outside of the
     Collector Server """

    CONSTANT_YAML: [str] = ['.yaml', '.yml']
    CONSTANT_JSON: str = '.json'
    COLLECTOR_NAME_PATTERN = re.compile(r"^[0-9a-zA-Z-_]+$")
    COLLECTOR_METADATA_FILENAME = "metadata.json"
    COLLECTOR_SERVER_RENAMING_RULES: [dict] = [
        {'key': 'dir', 'rename_to': 'directory_name'},
        {'key': 'chain_loc', 'rename_to': 'chain'},
        {'key': 'cert_loc', 'rename_to': 'cert'},
        {'key': 'key_loc', 'rename_to': 'key'},
        {'key': 'location', 'rename_to': 'address'},
        {'key': 'url', 'rename_to': 'address'},
    ]
    DEFAULTS = {
        'default_config_inputs_filename': '/etc/devo/job/job_config.json',
        'default_config_global_filename': '/etc/devo/collector/collector_config.json',
    }
    GLOBALS_ALLOWED = [
        'id',
        'name',
        'persistence',
    ]
    GLOBALS_OVERRIDES_ALLOWED = [
        'debug',
        'queue_max_size_in_mb',
        'queue_max_size_in_messages',
        'queue_max_elapsed_time_in_sec',
        'queue_wrap_max_size_in_messages',
        'queue_generate_metrics',
        'sender_stats_period_in_sec',
        'devo_sender_threshold_for_using_gzip_in_transport_layer',
        'devo_sender_compression_level',
        'devo_sender_compression_buffer_in_bytes',
        'generate_collector_details',
    ]
    CONFIG_GLOBAL_TRANSCRIPTION = [
        {'remote_key': 'job_id', 'local_key': 'id'},
        {'remote_key': 'debug', 'local_key': 'debug'},
        {'remote_key': 'state_store', 'local_key': 'persistence'},
    ]
    OUTPUT_EXTRA_SETTINGS = [
        {'setting_key': 'sender_concurrent_connections', 'running_key': 'concurrent_connections'},
        {'setting_key': 'sender_stats_period_in_sec', 'running_key': 'period_sender_stats_in_seconds'},
        {'setting_key': 'sender_activate_final_queue', 'running_key': 'activate_final_queue'},
        {
            'setting_key': 'devo_sender_threshold_for_using_gzip_in_transport_layer',
            'running_key': 'threshold_for_using_gzip_in_transport_layer'
        },
        {'setting_key': 'devo_sender_compression_level', 'running_key': 'compression_level'},
        {'setting_key': 'devo_sender_compression_buffer_in_bytes', 'running_key': 'compression_buffer_in_bytes'},
        {'setting_key': 'queue_generate_metrics', 'running_key': 'generate_metrics'},
    ]

    def __init__(self,
                 config_full_filename: str = None,
                 config_inputs_filename: str = None,
                 config_global_filename: str = None,
                 no_save_state: bool = False):

        log.info(
            "Processed values for config files parameters: "
            "{{"
            "\"config\": {}, "
            "\"job_config_loc\": {}, "
            "\"collector_config_loc\": {} }}".format(
                "\"{}\"".format(config_full_filename) if config_full_filename else "null",
                "\"{}\"".format(config_inputs_filename) if config_inputs_filename else "null",
                "\"{}\"".format(config_global_filename) if config_global_filename else "null"
            )
        )

        # Instantiate properties from signature
        self.no_save_state = no_save_state

        # Create extra required properties
        self.collector_name = None
        self.collector_version = None
        self.collector_owner = None

        # Validate configuration origins
        config_full_validation: Validation = self.__set_config_full_filename(config_full_filename)
        config_inputs_validation: Validation = self.__set_config_inputs_filename(config_inputs_filename)
        config_global_validation: Validation = self.__set_config_global_filename(config_global_filename)

        log.info(
            "Results of validation of config files parameters: "
            "{{"
            "\"config\": {}, "
            "\"config_validated\": {}, "
            "\"job_config_loc\": {}, "
            "\"job_config_loc_default\": {}, "
            "\"job_config_loc_validated\": {}, "
            "\"collector_config_loc\": {}, "
            "\"collector_config_loc_default\": {}, "
            "\"collector_config_loc_validated\": {}"
            "}}".format(
                "\"{}\"".format(self.config_full_filename) if self.config_full_filename else "null",
                config_full_validation.validated,
                "\"{}\"".format(self.config_inputs_filename) if self.config_inputs_filename else "null",
                config_inputs_validation.default,
                config_inputs_validation.validated,
                "\"{}\"".format(self.config_global_filename) if self.config_global_filename else "null",
                config_global_validation.default,
                config_global_validation.validated
            )
        )

        # Detect the environment (local/collector server) and build the config
        if config_inputs_validation.validated and config_global_validation.validated:
            # We are running on the Collector Server
            self.config: dict = self.__load_config_for_collector_server()
            self.__rename_configuration_from_collector_server()
            self.__add_extras_to_output_definitions()
        elif config_full_validation.validated:
            # We are running local
            self.config: dict = self.__load_config_for_local_environment()
            self.__add_extras_to_output_definitions()
        else:
            # We are not running
            raise CollectorConfigurationException(6, "Required configuration data is missing")

        # Complete the configuration
        collector_globals = self.config["globals"]
        self.collector_name = collector_globals.get("name")
        self.__load_collector_metadata()

    def __add_extras_to_output_definitions(self):
        """ Populate new parameters into output definitions from defaults or globals (overrides) """

        # Populate all defined fields
        for extra in self.OUTPUT_EXTRA_SETTINGS:
            setting_key = extra['setting_key']
            running_key = extra['running_key']

            # Populate for each output
            for output in self.config["outputs"].values():
                # Create config key if not exists
                if 'config' not in output:
                    output['config'] = {}

                # Create a default param if not defined or overwrite it if there is a request.
                if running_key not in output['config'] and setting_key not in self.config['globals']:
                    output['config'][running_key] = GLOBAL_DEFAULTS[setting_key]

                # If the setting_key is in globals, means that the user wants to override it.
                elif setting_key in self.config['globals']:
                    output['config'][running_key] = self.config["globals"][setting_key]

    def __set_config_full_filename(self, config_full_filename: str) -> Validation:
        """Validates the full configuration file.

        :param config_full_filename: File name of the config.yaml file.
        :return: Validation object
        """

        config_full_filename_validated: bool = False

        self.config_full_filename: str = config_full_filename

        # Validation procedure
        if self.config_full_filename:

            # Get the absolute path for this file
            if os.path.isabs(self.config_full_filename) is False:
                self.config_full_filename = os.path.join(os.getcwd(), "config", self.config_full_filename)
            config_full_file_path = pathlib.Path(self.config_full_filename)

            # The path cannot be a folder and must exist to be valid.
            if config_full_file_path.is_dir():
                raise CollectorConfigurationException(0, f"\"{self.config_full_filename}\" is not a file")
            elif config_full_file_path.exists() is False:
                CollectorConfiguration.__show_local_dirs(config_full_file_path)
                log.debug(f"File \"{self.config_full_filename} does not exists")
            else:
                config_full_filename_validated = True

        return Validation(config_full_filename_validated)

    def __set_config_inputs_filename(self, config_inputs_filename: str) -> Validation:
        """Validates the input configuration file.

        :param config_inputs_filename: File name of the full configuration file.
        :return: Validation object
        """

        config_inputs_filename_validated: bool = False
        config_inputs_filename_default_path: bool = False

        self.config_inputs_filename = config_inputs_filename

        # Use default input_filename if not provided
        if self.config_inputs_filename is None:
            self.config_inputs_filename = self.DEFAULTS["default_config_inputs_filename"]
            config_inputs_filename_default_path = True
            log.info(
                f'Using the default location for "job_config_loc" file: "{self.config_inputs_filename}"'
            )

        # Get the absolute path for this file
        current_directory = os.path.join(os.getcwd(), "config")
        if os.path.isabs(self.config_inputs_filename) is False:
            log.info(
                f'"{self.config_inputs_filename}" is in relative way, '
                f'it will be transformed to absolute using {current_directory}'
            )
            self.config_inputs_filename = os.path.join(current_directory, self.config_inputs_filename)
        config_inputs_file_path = pathlib.Path(self.config_inputs_filename)

        # The path cannot be a folder and must exist to be valid.
        if config_inputs_file_path.is_dir():
            raise CollectorConfigurationException(
                2,
                f'"{self.config_inputs_filename}" is not a file'
            )
        elif config_inputs_file_path.exists() is False:
            CollectorConfiguration.__show_local_dirs(config_inputs_file_path)
            if config_inputs_filename_default_path is False:
                raise CollectorConfigurationException(
                    3,
                    f'File "{self.config_inputs_filename}" does not exists'
                )
            log.debug(
                f'File "{self.config_inputs_filename}" does not exists'
            )
        else:
            config_inputs_filename_validated = True

        return Validation(config_inputs_filename_validated, config_inputs_filename_default_path)

    def __set_config_global_filename(self, config_global_filename: str) -> Validation:
        """Validates the global configuration file.

        :param config_global_filename: File name of the xxxxx
        :return: Validation object
        """

        config_global_filename_validated: bool = False
        config_global_filename_default_path: bool = False

        self.config_global_filename = config_global_filename

        # Use default global_filename if not provided
        if self.config_global_filename is None:
            self.config_global_filename = self.DEFAULTS["default_config_global_filename"]
            config_global_filename_default_path = True
            log.info(
                f'Using the default location for "collector_config_loc" file: "{self.config_global_filename}"'
            )

        # Get the absolute path for this file
        current_directory = os.path.join(os.getcwd(), "config")
        if os.path.isabs(self.config_global_filename) is False:
            log.info(
                f"\"{self.config_global_filename}\" is in relative way, "
                f"it will be transformed to absolute using {current_directory}"
            )
            self.config_global_filename = os.path.join(current_directory, self.config_global_filename)
        config_global_file_path = pathlib.Path(self.config_global_filename)

        # The path cannot be a folder and must exit to be valid
        if config_global_file_path.is_dir():
            raise CollectorConfigurationException(
                4,
                f'"{self.config_global_filename}" is not a file'
            )
        elif config_global_file_path.exists() is False:
            CollectorConfiguration.__show_local_dirs(config_global_file_path)
            if config_global_filename_default_path is False:
                raise CollectorConfigurationException(
                    5,
                    f'File "{self.config_global_filename}" does not exists'
                )
            log.debug(
                f'File "{self.config_global_filename}" does not exists'
            )
        else:
            config_global_filename_validated = True

        return Validation(config_global_filename_validated, config_global_filename_default_path)

    def __load_collector_metadata(self):
        """Extract the metadata of the collector.

        :return:
        """

        # Get the metadata file path
        collector_metadata_filename = os.path.join(os.getcwd(), CollectorConfiguration.COLLECTOR_METADATA_FILENAME)
        collector_metadata_file_path = pathlib.Path(collector_metadata_filename)

        # Extract the information if the file exist
        if collector_metadata_file_path.exists():
            with open(collector_metadata_file_path, encoding="utf-8") as collector_metadata_file:
                metadata_json = json.load(collector_metadata_file)
                if metadata_json.get("version"):
                    self.collector_version = metadata_json.get("version")
                if metadata_json.get("owner"):
                    self.collector_owner = metadata_json.get("owner")

    @staticmethod
    def __show_local_dirs(file_path: pathlib.Path):
        """Send to the log a list of the files inside the given folder.

        :param file_path: Path to the file.
        :return:
        """

        # Build the path to the folder
        file_directory_name = os.path.dirname(file_path)
        file_directory = pathlib.Path(file_directory_name)

        # Extract the files only if they exist and is a folder
        if file_directory.exists():
            if file_directory.is_dir():
                only_files = \
                    [f for f in os.listdir(file_directory_name) if os.path.isfile(os.path.join(file_directory_name, f))]
                log.info(
                    f'List of files in directory "{file_directory_name}": {only_files}'
                )
            else:
                log.info(
                    f'"{file_directory_name}" is not a directory'
                )
        else:
            log.info(
                f'"{file_directory_name}" does not exists'
            )

    def __load_config_for_local_environment(self) -> dict:
        """Build the configuration to run into outside of Devo Collector Server.

        :return: A dict with ready-to-use configuration
        """

        # Load the local settings file
        with open(self.config_full_filename, encoding="utf-8") as config_full_file:
            ext = os.path.splitext(config_full_file.name)[-1].lower()
            config_full_file_content = None

            # Detect file extension and load
            if ext in self.CONSTANT_YAML:
                try:
                    config_full_file_content = yaml.load(config_full_file, Loader=NoDatesSafeLoader)
                except ParserError as ex:
                    cause = f"{ex.context} ({str(ex.problem_mark).strip()})"
                    raise CollectorConfigurationException(
                        7, f"Configuration content file is not correct, details: {cause}"
                    )
                except Exception as ex:
                    raise CollectorConfigurationException(
                        8,
                        f"Error loading configuration file, details: {ex}"
                    )
            elif ext == self.CONSTANT_JSON:
                try:
                    config_full_file_content = json.load(config_full_file)
                except Exception as ex:
                    raise CollectorConfigurationException(
                        9,
                        f"Error loading configuration file, details: {ex}"
                    )

        assert config_full_file_content is not None, '[INTERNAL LOGIC ERROR] -> config_full_file_content cannot be NULL'

        # Build the final config dict
        config: dict = {
            "globals": config_full_file_content.get("globals", None),
            "inputs": config_full_file_content.get("inputs", None),
            "outputs": config_full_file_content.get("outputs", None)
        }

        self.__establish_persistence(config)
        self.__validate_global_settings(config=config, local_environment=True)
        return config

    def __validate_global_settings(self, config: dict, local_environment: bool = False):
        """Validate the retrieved global configuration by verifying that it is allowed.

        This validator takes into consideration that the global overrides are defined in
        the global section when running in a local environment.

        :param: Dictionary with the extracted configuration
        :param local_environment: Defines if the collector is running in local environment.
        :return:
        """

        valid_globals = {}

        # Get the allowed globals
        given_globals = config['globals']
        for key, value in given_globals.items():
            # Is this setting allowed?
            if key not in self.GLOBALS_ALLOWED:
                # Is the collector running in the local environment?
                if local_environment and key in self.GLOBALS_OVERRIDES_ALLOWED:
                    valid_globals[key] = value
                else:
                    log.warning(
                        f'[WARNING] Illegal global setting has been ignored -> {key}: {value}'
                    )
            else:
                valid_globals[key] = value

        # Dump the filtered settings
        config['globals'] = valid_globals

    def __load_config_for_collector_server(self) -> dict:
        """Build the configuration to run into the Devo Collector Server.

        :return: A dict with ready-to-use configuration
        """

        config: dict = {
            "globals": {},
            "inputs": {},
            "outputs": {}
        }

        # Load and customize config_inputs
        config_inputs_file_content = self.__load_inputs_config_from_collector_server()
        self.__customize_config_inputs_from_collector_server(config, config_inputs_file_content)

        # Load, customize config_global and check overrides
        config_global_file_content = self.__load_global_config_from_collector_server()
        self.__customize_config_global_from_collector_server(config, config_global_file_content)
        self.__validate_global_settings(config)
        self.__apply_global_overrides(config)

        # If config has been defined, establish persistence
        if config and len(config) > 0:
            self.__establish_persistence(config)

        return config

    def __apply_global_overrides(self, config: dict):
        """This method allows override the global settings based on the config_inputs definition when running in the
        collector server environment.

        :param config: Settings dictionary
        :return:
        """

        # Apply Global Overrides if defined
        if 'global_overrides' in config and config['global_overrides']:
            for key, value in config['global_overrides'].items():
                if key in self.GLOBALS_OVERRIDES_ALLOWED:
                    config['globals'][key] = value
                else:
                    log.warning(f'[WARNING] Illegal override setting has been ignored -> {key}: {value}')

            # Delete global_overrides from final config
            config.pop('global_overrides', None)

    def __customize_config_global_from_collector_server(self, config, config_global_file_content):
        """Customize the collector server's config_global to fit to the expected local
        format and remove the persistence if it has been defined.

        :param config: A dict with all the configuration.
        :param config_global_file_content: A dict returned by __load_global_config_from_collector_server method.
        :return:
        """

        self.__customize_collector_server_transcriptions(config, config_global_file_content)

        job_name: str = config_global_file_content.get("job_name")
        assert job_name is not None, '[INTERNAL LOGIC ERROR] -> job_name cannot be NULL'

        if not CollectorConfiguration.COLLECTOR_NAME_PATTERN.match(job_name):
            job_name_fixed = job_name.replace(".", "_").replace("#", "_").replace(" ", "_")
            if not CollectorConfiguration.COLLECTOR_NAME_PATTERN.match(job_name_fixed):
                raise CollectorConfigurationException(
                    13,
                    f"Collector name is having some not supported symbol, "
                    f"valid pattern \"{CollectorConfiguration.COLLECTOR_NAME_PATTERN.pattern}\","
                    f" current value: {job_name}"
                )
            else:
                log.warning(
                    f"Detected some not valid symbol in job_name property, it will be replaced by \"_\" symbol. "
                    f"Original value: {job_name}, final value: {job_name_fixed}"
                )
            job_name = job_name_fixed

        config["globals"]["name"] = job_name

        self.__customize_collector_server_loggers(config, config_global_file_content)

    @staticmethod
    def __customize_collector_server_loggers(config: dict, config_global_file_content: dict):
        """Customize collector servers loggers.

        :param config: A dict with all the configuration.
        :param config_global_file_content: A dict returned by __load_inputs_config_from_collector_server method.
        :return:
        """

        if "loggers" in config_global_file_content and isinstance(config_global_file_content["loggers"], list):
            output_counter = {}

            for output_config in config_global_file_content["loggers"]:
                output_type = output_config["type"]

                if output_type in output_counter:
                    output_counter[output_type] += 1
                else:
                    output_counter[output_type] = 0

                if output_type in outputs:
                    output_name = f"{output_type}_{output_counter[output_type]}"
                    config["outputs"][output_name] = output_config
                else:
                    raise CollectorConfigurationException(
                        14,
                        f"Output type \"{output_config['type']}\" does not exists"
                    )
        elif isinstance(config_global_file_content["loggers"], dict):
            config["outputs"] = config_global_file_content["loggers"]
        else:
            raise AssertionError('[INTERNAL LOGIC ERROR] -> Loggers not found in Collector Server settings')

    def __customize_collector_server_transcriptions(self, config: dict, config_global_file_content: dict):
        """Apply the customization that can be done in a simple way (transcription).

        :param config: A dict with all the configuration.
        :param config_global_file_content: A dict returned by __load_inputs_config_from_collector_server method.
        :return:
        """

        for customization in self.CONFIG_GLOBAL_TRANSCRIPTION:
            remote = customization['remote_key']
            local = customization['local_key']

            # Use only if defined
            if remote in config_global_file_content:
                config["globals"][local] = config_global_file_content[remote]

    @staticmethod
    def __customize_config_inputs_from_collector_server(config: dict, config_inputs_file_content: dict):
        """Customize the collector server's config_inputs to fit to the expected local
        format and remove the persistence if it has been defined.

        :param config: A dict with all the configuration.
        :param config_inputs_file_content: A dict returned by __load_inputs_config_from_collector_server method.
        :return:
        """

        config["inputs"] = config_inputs_file_content.get("inputs", config_inputs_file_content)
        config["global_overrides"] = config_inputs_file_content.get("global_overrides", {})

        # Delete persistence if exist
        for input_value in config["inputs"].values():
            if "persistence" in input_value:
                del input_value["persistence"]

    def __load_global_config_from_collector_server(self) -> dict:
        """Load the global_config from collector server environment.

        :return: A dict with the configuration
        """

        # Load global_config
        with open(self.config_global_filename, encoding="utf-8") as config_global_file:
            ext = os.path.splitext(config_global_file.name)[-1].lower()
            config_global_file_content = None
            if ext in self.CONSTANT_YAML:
                try:
                    config_global_file_content = yaml.load(config_global_file, Loader=yaml.SafeLoader)
                except Exception as ex:
                    raise CollectorConfigurationException(
                        11,
                        f"Error loading configuration file, details: {ex}"
                    )
            elif ext == self.CONSTANT_JSON:
                try:
                    config_global_file_content = json.load(config_global_file)
                except Exception as ex:
                    raise CollectorConfigurationException(
                        12,
                        f"Error loading configuration file, details: {ex}"
                    )

        assert config_global_file_content is not None, '[INTERNAL LOGIC ERROR] -> ' \
                                                       'config_inputs_file_content cannot be NULL'
        return config_global_file_content

    def __load_inputs_config_from_collector_server(self) -> dict:
        """Load the inputs_config from collector server environment.

        :return: A dict with the configuration
        """

        # Load inputs_config
        with open(self.config_inputs_filename, encoding="utf-8") as config_inputs_file:
            ext = os.path.splitext(config_inputs_file.name)[-1].lower()
            config_inputs_file_content = None

            # Detect file extension and load
            if ext in self.CONSTANT_YAML:
                try:
                    config_inputs_file_content = yaml.load(config_inputs_file, Loader=NoDatesSafeLoader)
                except ParserError as ex:
                    cause = f"{ex.context} ({str(ex.problem_mark).strip()})"
                    raise CollectorConfigurationException(
                        10,
                        f"Configuration content file is not correct, details: {cause}"
                    )
                except Exception as ex:
                    raise CollectorConfigurationException(
                        11,
                        f"Error loading configuration file, details: {ex}"
                    )
            elif ext == self.CONSTANT_JSON:
                try:
                    config_inputs_file_content = json.load(config_inputs_file)
                except Exception as ex:
                    raise CollectorConfigurationException(
                        12,
                        f"Error loading configuration file, details: {ex}"
                    )

        assert config_inputs_file_content is not None, '[INTERNAL LOGIC ERROR] -> ' \
                                                       'config_inputs_file_content cannot be NULL'

        return config_inputs_file_content

    def __establish_persistence(self, config: dict):
        """Configure the collector persistence using the configuration.

        :param config: Configuration
        :return:
        """

        persistence = None

        # Check persistence in the config
        if "persistence" in config["globals"]:
            persistence = config["globals"]["persistence"]

        # Check if the persistence must be ignored or is not con
        if self.no_save_state is True or persistence is None:
            persistence = {
                "type": "memory"
            }

        assert persistence is not None, '[INTERNAL LOGIC ERROR] -> Persistence cannot be NULL'

        # Deploy the persistence
        for input_name in config["inputs"]:
            input_value = config["inputs"][input_name]
            if isinstance(input_value, dict) and "persistence" not in input_value:
                config["inputs"][input_name]["persistence"] = persistence

    def get_max_consumers(self) -> int:
        """Gets the maximum number of consumers based on the multiprocessing status.

        :return: Maximum number of consumers
        """

        self.is_ready()

        if self.is_multiprocessing():
            max_consumers: int = GLOBAL_DEFAULTS['max_consumers_for_multiprocessing']
        else:
            max_consumers: int = GLOBAL_DEFAULTS['max_consumers_for_multithreading']

        return max_consumers

    def is_multiprocessing(self) -> bool:
        """Checks if the multiprocessing property is active.

        :return: True if yes. False if not.
        """

        self.is_ready()

        globals_section = self.config.get('globals')
        if globals_section and \
                globals_section.get('multiprocessing', GLOBAL_DEFAULTS['multiprocessing']):
            return True

        return False

    def is_balanced_output(self) -> bool:
        """
        Checks if the multiprocessing property is active
        :return: True if yes. False if not.
        """
        self.is_ready()

        if self.config.get('globals') and \
                self.config.get('globals').get('balanced_output', GLOBAL_DEFAULTS['balanced_output']):
            return True

        return False

    def generate_collector_details(self) -> bool:
        """

        :return:
        """

        self.is_ready()

        globals_section = self.config.get('globals')
        if globals_section:
            generate_collector_details = \
                globals_section.get('generate_collector_details', GLOBAL_DEFAULTS['generate_collector_details'])
            if generate_collector_details:
                return True

        return False

    def is_ready(self) -> bool:
        """Checks that the configuration is ready.

        :return: True if is correct. Raises a CollectorConfigurationException if not.
        """

        if self.config:
            # Complex checking
            globals_conf_entry = self.config.get("globals")
            # Check that all required keys are defined
            if globals_conf_entry:
                collector_name = globals_conf_entry.get("name")
                if not CollectorConfiguration.COLLECTOR_NAME_PATTERN.match(collector_name):
                    collector_name_fixed = collector_name.replace(".", "_").replace("#", "_").replace(" ", "_")
                    if not CollectorConfiguration.COLLECTOR_NAME_PATTERN.match(collector_name_fixed):
                        raise CollectorConfigurationException(
                            13,
                            f"Collector name is having some not supported symbol, "
                            f"valid pattern \"{CollectorConfiguration.COLLECTOR_NAME_PATTERN.pattern}\", "
                            f"current value: {collector_name}"
                        )
                    else:
                        log.warning(
                            f"[MAIN] Detected some not valid symbol in globals.name property, it will be replaced by "
                            f"\"_\" symbol. "
                            f"Original value: {collector_name}, final value: {collector_name_fixed}"
                        )

                    globals_conf_entry["name"] = collector_name_fixed

            inputs_conf_entry = self.config.get("inputs")
            outputs_conf_entry = self.config.get("outputs")

            if globals_conf_entry and inputs_conf_entry and outputs_conf_entry:
                return True

        raise CollectorConfigurationException(
            0,
            "The configuration structure is not correct"
        )

    def get_queue_building_arguments(self) -> dict:
        """This method returns the internal queue arguments to be used to create the queue.

        :return: A dict to be used as queue payload.
        """

        self.is_ready()

        # Get configuration
        global_settings: dict = self.config.get('globals')

        # Create Hooks
        qmsimb = 'queue_max_size_in_mb'
        qmsim = 'queue_max_size_in_messages'
        qmetis = 'queue_max_elapsed_time_in_sec'
        qwsim = 'queue_wrap_max_size_in_messages'
        qgm = 'queue_generate_metrics'
        gcd = 'generate_collector_details'

        return {
            'max_size_in_mb': global_settings.get(qmsimb, GLOBAL_DEFAULTS[qmsimb]),
            'max_size_in_messages': global_settings.get(qmsim, GLOBAL_DEFAULTS[qmsim]),
            'max_elapsed_time_in_sec': global_settings.get(qmetis, GLOBAL_DEFAULTS[qmetis]),
            'max_wrap_size_in_messages': global_settings.get(qwsim, GLOBAL_DEFAULTS[qwsim]),
            'generate_metrics': global_settings.get(qgm, GLOBAL_DEFAULTS[qgm]),
            'generate_collector_details': global_settings.get(gcd, GLOBAL_DEFAULTS[gcd])
        }

    def __str__(self):
        """Returns a json representation of the class.

        :return:
        """

        return f"{json.dumps(self.config)}"

    def __rename_configuration_from_collector_server(self) -> None:
        """The Collector Server Settings call some settings in a different way. This method rename them using
        the local rules.

        :return:
        """

        if "globals" in self.config:
            self.__rename_globals()

        if "outputs" in self.config:
            _ = [self.__rename_outputs(output) for output in self.config["outputs"].values()]

    def __rename_globals(self):
        """
        This method helps to self.__rename_configuration_from_collector_server() to reduce his cognitive complexity.
        This method is in charge to update the key name of the globals configuration.
        """
        if "config" in self.config["globals"]["persistence"] \
                and "dir" in self.config["globals"]["persistence"]["config"]:
            self.config["globals"]["persistence"]["config"]["directory_name"] = \
                self.config["globals"]["persistence"]["config"].pop("dir")

    def __rename_outputs(self, output: dict):
        """This method helps to __fix_field_names() method to reduce his cognitive complexity.

        This method is in charge to update the keys name of the output configuration.

        :param output: Output item from self.config['outputs'].
        :return:
        """
        if "config" in output:
            # Apply renaming rules
            for rule in self.COLLECTOR_SERVER_RENAMING_RULES:
                key = rule['key']
                rename_to = rule['rename_to']
                if rule['key'] in output["config"]:
                    output["config"][rename_to] = output["config"].pop(key)

    def get_collector_name(self) -> str:
        """Returns the collector name.

        :return:
        """

        return self.collector_name

    def get_collector_version(self) -> str:
        """Returns the collector version.

        :return:
        """

        return self.collector_version

    def get_collector_owner(self) -> str:
        """Returns the collector owner name.

        :return:
        """

        return self.collector_owner
