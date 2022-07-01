from agent.queues.content.communication_queue_notification import CommunicationQueueNotification


class CollectorNotification(CommunicationQueueNotification):
    """
    This class will represent a notification
    """

    # Related to Input
    SUBMODULE_IS_NOT_WORKING: str = "submodule_is_not_working"
    MODULE_IS_NOT_WORKING: str = "module_is_not_working"
    SERVICE_IS_NOT_WORKING: str = "service_is_not_working"
    INPUT_IS_NOT_WORKING: str = "input_is_not_working"

    ALL_INPUT_OBJECTS_ARE_PAUSED: str = "all_input_objects_are_paused"
    ALL_INPUT_OBJECTS_ARE_STOPPED: str = "all_input_objects_are_stopped"

    INPUT_PROCESS_IS_STOPPED: str = "input_process_is_stopped"

    # Related to Output
    FINAL_SENDER_IS_NOT_WORKING: str = "final_sender_is_not_working"

    ALL_SENDER_MANAGERS_ARE_PAUSED: str = "all_sender_managers_are_paused"
    ALL_SENDER_MANAGERS_ARE_FLUSHED: str = "all_sender_managers_are_flushed"
    ALL_SENDER_MANAGERS_ARE_STOPPED: str = "all_sender_managers_are_stopped"

    ALL_CONSUMERS_ARE_PAUSED: str = "all_consumers_are_paused"
    ALL_CONSUMERS_ARE_FLUSHED: str = "all_consumers_are_flushed"
    ALL_CONSUMERS_ARE_STOPPED: str = "all_consumers_are_stopped"

    OUTPUT_PROCESS_IS_STOPPED: str = "output_process_is_stopped"

    def __init__(self, notification_id: str = None, details: str = None):
        super().__init__(notification_id=notification_id, details=details)

    # Input notifications

    @staticmethod
    def create_submodule_is_not_working_notification(details: str):
        """

        :return:
        """
        return CollectorNotification(
            notification_id=CollectorNotification.SUBMODULE_IS_NOT_WORKING,
            details=details
        )

    @staticmethod
    def create_module_is_not_working_notification(details: str):
        """

        :return:
        """
        return CollectorNotification(
            notification_id=CollectorNotification.MODULE_IS_NOT_WORKING,
            details=details
        )

    @staticmethod
    def create_service_is_not_working_notification(details: str):
        """

        :return:
        """
        return CollectorNotification(
            notification_id=CollectorNotification.SERVICE_IS_NOT_WORKING,
            details=details
        )

    @staticmethod
    def create_input_is_not_working_notification(details: str):
        """

        :return:
        """
        return CollectorNotification(
            notification_id=CollectorNotification.INPUT_IS_NOT_WORKING,
            details=details
        )

    @staticmethod
    def create_all_inputs_are_paused_notification(details: str):
        """

        :return:
        """
        return CollectorNotification(
            notification_id=CollectorNotification.ALL_INPUT_OBJECTS_ARE_PAUSED,
            details=details
        )

    @staticmethod
    def create_all_inputs_are_stopped_notification(details: str):
        """

        :return:
        """
        return CollectorNotification(
            notification_id=CollectorNotification.ALL_INPUT_OBJECTS_ARE_STOPPED,
            details=details
        )

    @staticmethod
    def create_input_process_is_stopped_notification(details: str):
        """

        :return:
        """
        return CollectorNotification(
            notification_id=CollectorNotification.INPUT_PROCESS_IS_STOPPED,
            details=details
        )

    # Output notifications

    @staticmethod
    def create_final_sender_is_not_working_notification(details: str):
        """

        :return:
        """
        return CollectorNotification(
            notification_id=CollectorNotification.FINAL_SENDER_IS_NOT_WORKING,
            details=details
        )

    @staticmethod
    def create_all_sender_managers_are_paused_notification(details: str):
        """

        :return:
        """
        return CollectorNotification(
            notification_id=CollectorNotification.ALL_SENDER_MANAGERS_ARE_PAUSED,
            details=details
        )

    @staticmethod
    def create_all_sender_managers_are_flushed_notification(details: str):
        """

        :return:
        """
        return CollectorNotification(
            notification_id=CollectorNotification.ALL_SENDER_MANAGERS_ARE_FLUSHED,
            details=details
        )

    @staticmethod
    def create_all_sender_managers_are_stopped_notification(details: str):
        """

        :return:
        """
        return CollectorNotification(
            notification_id=CollectorNotification.ALL_SENDER_MANAGERS_ARE_STOPPED,
            details=details
        )

    @staticmethod
    def create_all_consumers_are_paused_notification(details: str):
        """

        :return:
        """
        return CollectorNotification(
            notification_id=CollectorNotification.ALL_CONSUMERS_ARE_PAUSED,
            details=details
        )

    @staticmethod
    def create_all_consumers_are_flushed_notification(details: str):
        """

        :return:
        """
        return CollectorNotification(
            notification_id=CollectorNotification.ALL_CONSUMERS_ARE_FLUSHED,
            details=details
        )

    @staticmethod
    def create_all_consumers_are_stopped_notification(details: str):
        """

        :return:
        """
        return CollectorNotification(
            notification_id=CollectorNotification.ALL_CONSUMERS_ARE_STOPPED,
            details=details
        )

    @staticmethod
    def create_output_process_is_stopped_notification(details: str):
        """

        :return:
        """
        return CollectorNotification(
            notification_id=CollectorNotification.OUTPUT_PROCESS_IS_STOPPED,
            details=details
        )
