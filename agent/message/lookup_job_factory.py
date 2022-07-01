from collections import namedtuple
from typing import Union, List, Optional, Any
from ipaddress import ip_address, IPv4Address
from typing import Union, List, Optional, Any

LookupPayload = Union[List[str], List[List]]

LookupSettings = namedtuple(
    'LookupSettings',
    ['lookup_name', 'headers', 'key', 'historic_tag', 'send_start', 'send_end', 'action', 'field_types', 'key_type']
)

LookupFilterResult = namedtuple('LookupFilterResult', ['accepted_items', 'rejected_items'])
LookupSize = namedtuple('LookupSize', ['to_create', 'to_modify', 'to_remove'])

class LookupJob(object):
    """Class that instantiates a LookupJob"""

    def __init__(self, lookup_settings: LookupSettings, to_do: list):
        """
        Builder
        :param lookup_settings: a LookupSettings instance.
        :param to_do: List of items to be created.
        """
        # Properties
        self.settings: LookupSettings = lookup_settings
        self.to_do: list = to_do

    def __str__(self):
        """ Str representation of LookupJob """
        rep = {
            'settings': self.settings,
            'to_do': self.to_do
        }
        return rep


class LookupJobFactory(object):
    """Class that instantiates a LookupJobFactory"""
    DEVO_VALID_TYPES_FOR_KEY = ['str', 'ip4', 'int']
    DEVO_TYPES = ['str', 'int', 'int4', 'int8', 'float', 'bool', 'ip4']
    COMPATIBLE_TYPES = {
        'str': ['str', 'ip4', 'null'],
        'int': ['int8', 'int4', 'int', 'null'],
        'int4': ['int4', 'null'],
        'int8': ['int8', 'int4', 'int', 'null'],
        'float': ['float', 'int8', 'int4', 'int', 'null'],
        'bool': ['bool', 'null'],
        'ip4': ['ip4', 'null'],
    }

    def __init__(self,
                 lookup_name: str,
                 headers: [str],
                 key: str,
                 field_types: Optional[List[str]] = None,
                 get_field_types_from: List[List[Any]] = None,
                 historic_tag: str = None):
        """
        Builder.
        :param lookup_name: Name of the lookup table.
        :param headers: List of the headers name.
        :param field_types: List of field_types, if calculate_field_types is True, this param will be filled with
            a sample of data to be used for types calculation.
        :param key: Name of the header used as key in Devo
        :param get_field_types_from: If defined, it contains the models to be used to calculate the data types.
        :param historic_tag: Deprecated. Use None Always.
        """
        # Properties
        self._lookup_name = None
        self._headers = None
        self._field_types = None
        self._key = None
        self._historic_tag = None
        self._start_control = None
        self._end_control = None
        self._key_type = None

        # Batches
        self._to_initialize = []
        self._to_remove = []
        self._to_modify = []

        # Assignation
        self.lookup_name: str = lookup_name
        self.headers: [str] = headers
        if field_types is None:
            self.field_types: [str] = self.__calculate_field_types_from(get_field_types_from)
        else:
            self.field_types: [str] = field_types
        self.key: str = key
        self.historic_tag: Optional[str] = historic_tag
        self.start_control: bool = False
        self.end_control: bool = False

        self.__validate_structure()

    def clean_all_lists(self):
        """ Clean all batches lists """
        self.clean_initialize_list()
        self.clean_remove_list()
        self.clean_modify_list()

    def clean_initialize_list(self):
        """ Clean the batch list _to_initialize """
        self._to_initialize = []

    def clean_remove_list(self):
        """ Clean the batch list _to_remove """
        self._to_remove = []

    def clean_modify_list(self):
        """ Clean the batch list _to_modify """
        self._to_modify = []

    def add_item_to_list_to_initialize(self, payload: LookupPayload) -> [int, [List]]:
        """
        Add one or more lookups to the creation list.
        :param payload: an instance with LookupPayload format.
        :return: [x, y] x -> Count of rejected items. y -> Rejected items with rejection detail.
        """

        self.__validate_payload_instance(payload=payload)
        validation: LookupFilterResult = self.__validate_payload_content(payload=payload)
        self._to_initialize.extend(validation.accepted_items)

        return [len(validation.rejected_items), validation.rejected_items]

    def add_item_to_list_to_remove(self, payload: LookupPayload) -> [int, [List]]:
        """
        Add one or more lookups to the remove list.
        :param payload: an instance with LookupPayload format.
        :return: [x, y] x -> Count of rejected items. y -> Rejected items with rejection detail.
        """

        self.__validate_payload_instance(payload=payload)
        validation: LookupFilterResult = self.__validate_payload_content(payload=payload, to_remove=True)
        self._to_remove.extend(validation.accepted_items)

        return [len(validation.rejected_items), validation.rejected_items]

    def add_item_to_list_to_modify(self, payload: LookupPayload) -> [int, [List]]:
        """
        Add one or more lookups to the update list.
        :param payload: an instance with LookupPayload format.
        :return: [x, y] x -> Count of rejected items. y -> Rejected items with rejection detail.
        """
        self.__validate_payload_instance(payload=payload)
        validation: LookupFilterResult = self.__validate_payload_content(payload=payload)
        self._to_modify.extend(validation.accepted_items)

        return [len(validation.rejected_items), validation.rejected_items]

    def get_size(self) -> LookupSize:
        """
        Calculate the size of each lookup buffer.

        :return: (LookupSize) with the size of each buffer
        """
        return LookupSize(
            to_create=len(self._to_initialize),
            to_modify=len(self._to_modify),
            to_remove=len(self._to_remove)
        )

    def create_job_to_initialize_items(self, send_start: bool, send_end: bool) -> LookupJob:
        """
        Create a job with the to_create content.
        :param send_start: True if <start> control should be sent. False if not.
        :param send_end: True if <end> control should be sent. False if not.
        :return: a LookupJob instance.
        """
        # Create job
        work_to_do = self._to_initialize.copy()
        lookup_job: LookupJob = self.__create_job(send_start, send_end, 'INITIALIZE', work_to_do)
        # Empty the original list
        self._to_initialize = []

        return lookup_job

    def create_job_to_remove_items(self, send_start: bool, send_end: bool) -> LookupJob:
        """
        Create a job with the to_delete content.
        :param send_start: True if <start> control should be sent. False if not.
        :param send_end: True if <end> control should be sent. False if not.
        :return: a LookupJob instance.
        """
        # Create job
        work_to_do = self._to_remove.copy()
        lookup_job: LookupJob = self.__create_job(send_start, send_end, 'REMOVE', work_to_do)
        # Empty the original list
        self._to_remove = []

        return lookup_job

    def create_job_to_modify_items(self, send_start: bool, send_end: bool) -> LookupJob:
        """
        Create a job with the to_update content.
        :param send_start: True if <start> control should be sent. False if not.
        :param send_end: True if <end> control should be sent. False if not.
        :return: a LookupJob instance.
        """
        # Create job
        work_to_do = self._to_modify.copy()
        lookup_job: LookupJob = self.__create_job(send_start, send_end, 'MODIFY', work_to_do)
        # Empty the original list
        self._to_modify = []

        return lookup_job

    def __validate_structure(self):
        """ Validate the structure of the lookup factory """
        self.__key_type_validator()
        self.__headers_and_types_validator()

    def __key_type_validator(self):
        """ Validate the key type """
        key_id: int = self.headers.index(self.key)
        key_type: str = self.field_types[key_id]
        if key_type not in self.DEVO_VALID_TYPES_FOR_KEY:
            raise LookupError(
                f'Invalid key type for <{self.key}> field. It should be on of '
                f'<{self.DEVO_VALID_TYPES_FOR_KEY}> instance, not <{key_type}>.'
            )

        self.key_type: str = key_type

    def __headers_and_types_validator(self):
        """Validates that the headers and types count are the same"""
        headers_count: int = len(self.headers)
        types_count: int = len(self.field_types)
        if headers_count != types_count:
            raise LookupError(
                f'Invalid fields configuration. {headers_count} headers defined for {types_count} data types.'
            )

    @staticmethod
    def __validate_ipv4(payload: str) -> bool:
        """
        Validate if the payload is IPv4 compatible format.
        :param payload: String with the value to be tested.
        :return: True if is IPv4. False if not.
        """
        try:
            ip_payload = ip_address(payload)
            if isinstance(ip_payload, IPv4Address) is True:
                return True
        except ValueError as _:
            return False

    def __calculate_field_types_from(self, payload: List[List[Any]]) -> [str]:
        """
        Calculate the data type for each field using the payload as model.

        :param payload: A list of lists of any elements.
        :return: A list of types.
        """
        possible_types_per_field: dict = {}

        # Calculate all possibilities
        for model in payload:
            x = -1
            for field_value in model:
                x += 1
                all_types_in_the_field: [str] = possible_types_per_field.get(x, [])
                all_types_in_the_field.append(self.__get_devo_type_of(field_value))
                possible_types_per_field[x] = all_types_in_the_field

        calculated_types: [str] = []
        # Choose the winner
        for index, value in possible_types_per_field.items():
            winners = set(value)
            calculated_types.append(self.__get_the_most_specific_field_type_from(list(winners)))

        return calculated_types

    @staticmethod
    def __get_the_most_specific_field_type_from(types: [str]):
        """
        Calculate from the given list of types, the most specific.

        :param types: ([str]) Instance
        """
        winner: Optional[str] = None
        whiles: int = 0
        while winner is None:
            if len(types) == 1 and types[0] == 'null':
                winner = 'str'
            elif len(types) == 1:
                winner = types[0]
            elif 'null' in types:
                types.remove('null')
            elif 'str' in types and 'ip4' in types:
                types.remove('ip4')
            elif 'str' in types:
                types.remove('str')
            elif 'int' in types and ('int4' in types or 'int8' in types):
                types.remove('int')
            elif 'int4' in types and 'int8' in types:
                types.remove('int4')
            elif 'ip4' in types:
                types = ['ip4']
            elif 'float' in types:
                types = ['float']

            if whiles > 10:
                raise LookupError(
                    f'LookupService cannot find a winner from <{types}>'
                )
            whiles += 1

        return winner

    def __get_devo_type_of(self, sample: Union[str, int, float, bool]) -> str:
        """
        Calculate the devo_tools corresponding data type based on the given sample

        :param sample: Value sample to be treated
        :return: Devo value in string format
        """

        if isinstance(sample, bool):
            value_type = 'bool'
        elif isinstance(sample, float):
            value_type = 'float'
        elif isinstance(sample, int):
            if sample > 2147483647 or sample < -2147483648:
                value_type = 'int8'
            else:
                value_type = 'int4'
        elif self.__validate_ipv4(sample):
            value_type = 'ip4'
        elif isinstance(sample, str):
            value_type = 'str'
        else:
            value_type = 'null'

        return value_type

    @staticmethod
    def __validate_payload_instance(payload: LookupPayload):
        """
        Validate the format of the LookupPayload.
        :param payload: a valid LookupPayload compatible instance.
        :raise: ValueError if the type is invalid.
        """
        if not isinstance(payload, list) or len(payload) == 0:
            raise LookupError(
                f'Payload should be a populated instance of List[List] or List[str/int/bool] not <{type(payload)}>'
            )

    def __validate_payload_content(self, payload: LookupPayload, to_remove: bool = False) -> LookupFilterResult:
        """
        Validate the new items added to the batches.

        :param payload: an instance with LookupPayload format.
        :param to_remove: True if the validation is requested by add_item_to_list_to_remove method. False if not.
        :return: an instance with LookupFilterResult format.
        """
        # Results
        accepted_items = []
        rejected_items = []

        # Detect the items and validate them
        if isinstance(payload[0], list):
            items_to_validate = payload
        else:
            items_to_validate = [payload]

        # Execute all validations
        valid_length = len(self.field_types)
        for items in items_to_validate:

            rejection_reason = None

            # Length validation taking into account to_remove status
            item_length = len(items)
            # When to_remove is False, the length should be the same
            length_check_1: bool = to_remove is False and valid_length != item_length
            # When to_remove is True, the length should be the same or provide only one item
            length_check_2: bool = to_remove is True and valid_length != item_length and len(items) != 1

            if length_check_1 is True or length_check_2 is True:
                rejection_reason = f'Invalid number of fields ({item_length}/{valid_length})'

            # Data types validation
            if not rejection_reason:
                result, reason = self.__validate_payload_content_types(items)
                if result is False:
                    rejection_reason = f'Invalid data type -> {reason}'

            # Items sanitization
            items: [Any] = self.__sanitize_strings(items)

            # Execute acceptation or rejection
            if rejection_reason:
                rejected_items.append([items, rejection_reason])
            else:
                accepted_items.append(items)

        result: LookupFilterResult = LookupFilterResult(accepted_items=accepted_items, rejected_items=rejected_items)

        return result

    @staticmethod
    def __sanitize_strings(payload: [Any]) -> [Any]:
        """
        Sanitize all strings available in the payload. This method is called before append the items to the buffers.

        :param payload: ([Any]) instance with the items to be added to the buffer.
        :return: ([Any]) Instance with the sanitized content.
        """
        sanitized: [Any] = []
        for load in payload:
            if isinstance(load, str):
                load: str = load.replace('"', '""')
            sanitized.append(load)

        return sanitized

    def __validate_payload_content_types(self, item: [str]) -> [bool, str]:
        """
        Validate the data types of a lookup item based in the defined types.
        :param item: Lookup item to be validated.
        :return: Index 0 -> True if passed. False if not.
                 Index 1 -> Reason of rejection.
        """
        for index, value in enumerate(item, start=0):

            detected_value: str = self.__get_devo_type_of(value)
            defined_value: str = self.field_types[index]

            if detected_value not in self.COMPATIBLE_TYPES[defined_value]:
                reason = f'Field {self.headers[index]} | Index {index} | Value {value} | Expected {defined_value} | ' \
                         f'Found {detected_value} | Compatible Types {self.COMPATIBLE_TYPES[defined_value]}'

                return [False, reason]

        return [True, None]

    def __create_job(self, send_start: bool, send_end: bool, action: str, content: list) -> LookupJob:
        """
        Create a job with all the work that should be performed.
        :param send_start: True if <start> control should be sent. False if not.
        :param send_end: True if <end> control should be sent. False if not.
        :param action: one of the available actions -> CREATE, REMOVE, UPDATE
        :return: a LookupJob instance.
        """

        # Create the settings to be used as LookupJob payload
        lookup_settings: LookupSettings = LookupSettings(
            lookup_name=self.lookup_name,
            headers=self.headers,
            field_types=self.field_types,
            key=self.key,
            historic_tag=self.historic_tag,
            send_start=send_start,
            send_end=send_end,
            action=action,
            key_type=self._key_type
        )

        # Create the Lookup Job
        lookup_job: LookupJob = LookupJob(lookup_settings=lookup_settings, to_do=content)

        return lookup_job

    @property
    def start_control(self) -> bool:
        return self._start_control

    @start_control.setter
    def start_control(self, value: bool):

        # Validations
        if not isinstance(value, bool):
            raise LookupError(
                f'Invalid type for start_control property. Expected: <bool>, received: <{type(value)}>.'
            )

        # Assignations
        self._start_control = value

    @property
    def end_control(self) -> bool:
        return self._end_control

    @end_control.setter
    def end_control(self, value: bool):
        # Validations
        if not isinstance(value, bool):
            raise LookupError(
                f'Invalid type for end_control property. It should be an <bool> instance, not <{type(value)}>.'
            )

        # Assignations
        self._end_control = value

    @property
    def lookup_name(self) -> str:
        return self._lookup_name

    @lookup_name.setter
    def lookup_name(self, value: str):

        # Validations
        if value is None:
            raise LookupError(
                'Invalid lookup_name property value. It cannot be NULL.'
            )
        if not isinstance(value, str):
            raise LookupError(
                f'Invalid lookup_name property value. It should be an <str> instance not, <{type(value)}>.'
            )
        if len(value) <= 5:
            raise LookupError(
                f'Invalid lookup_name property value. The <str> length should be higher than 5 characters.'
            )

        # Assignation
        self._lookup_name = value

    @property
    def headers(self) -> [str]:
        return self._headers

    @headers.setter
    def headers(self, value: str):

        # Validations
        if value is None:
            raise LookupError('Invalid headers property value. It cannot be NULL.')
        if not isinstance(value, list):
            raise LookupError(f'Invalid headers property value. It should be an <list> instance not <{type(value)}>.')
        if len(value) == 0:
            raise LookupError('Invalid headers property value. The <list> instance cannot be empty.')

        # Assignation
        self._headers = value

    @property
    def key(self) -> str:
        return self._key

    @key.setter
    def key(self, value: str):

        # Validations
        if value is None:
            raise LookupError('Invalid key property value. It cannot be NULL.')
        if not isinstance(value, str):
            raise LookupError(f'Invalid key property value. It should be an <str> instance not <{type(value)}>')
        if len(value) == 0:
            raise LookupError('Invalid key property value. The <str> cannot be empty.')

        # Assignation
        self._key = value

    @property
    def historic_tag(self) -> Union[bool, None]:
        return self._historic_tag

    @historic_tag.setter
    def historic_tag(self, value: str):

        # Validations
        if value is not None:
            raise LookupError('Invalid historic_tag property value. It should be NULL.')

        # Assignation
        self._historic_tag = value

    @property
    def key_type(self) -> str:
        return self._key_type

    @key_type.setter
    def key_type(self, value: str):
        # Validations
        if value not in self.DEVO_VALID_TYPES_FOR_KEY:
            raise LookupError(
                f'Invalid type for key field. It should be on of <{self.DEVO_VALID_TYPES_FOR_KEY}> instance, '
                f'not <{value}>.'
            )

    @property
    def field_types(self) -> [str]:
        return self._field_types

    @field_types.setter
    def field_types(self, value: [str]):

        # Validations
        if value is None:
            raise LookupError(
                'Invalid field_types property value. It cannot be NULL.'
            )
        if not isinstance(value, list):
            raise LookupError(
                f'Invalid field_types property value. It should be an <list> instance not <{type(value)}>.'
            )
        if len(value) == 0:
            raise LookupError(
                'Invalid field_types property value. The <list> cannot be empty.'
            )

        for v in value:
            if v not in self.DEVO_TYPES:
                raise LookupError(
                    f'Invalid field_types property value. The Devo Type <{v}> does not exist'
                )

        # Assignation
        self._field_types = value
