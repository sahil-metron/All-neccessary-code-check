from agent.queues.content.communication_queue_order import CommunicationQueueOrder


class CollectorOrder(CommunicationQueueOrder):
    """
    This class will represent an order
    """

    # Related to Input
    PAUSE_ALL_INPUT_OBJECTS: str = "pause_all_input_objects"
    STOP_ALL_INPUT_OBJECTS: str = "stop_all_input_objects"
    STOP_INPUT_CONTROLLER: str = "stop_input_controller"

    # Related to Output
    STOP_OUTPUT_CONTROLLER: str = "stop_output_controller"

    PAUSE_ALL_SENDER_MANAGERS: str = "pause_all_sender_managers"
    FLUSH_ALL_SENDER_MANAGERS: str = "flush_all_sender_managers"
    STOP_ALL_SENDER_MANAGERS: str = "stop_all_sender_managers"

    PAUSE_ALL_CONSUMERS: str = "pause_all_consumers"
    FLUSH_ALL_CONSUMERS: str = "flush_all_consumers"
    STOP_ALL_CONSUMERS: str = "stop_all_consumers"

    def __init__(self, order_id: str = None, details: str = None, order_type: str = None):
        super().__init__(order_id=order_id, details=details, order_type=order_type)

    # Input orders

    @staticmethod
    def create_pause_all_inputs_order(details: str):
        """

        :return:
        """
        return CollectorOrder(
            order_id=CollectorOrder.PAUSE_ALL_INPUT_OBJECTS,
            details=details,
            order_type=CollectorOrder.INPUT_TYPE
        )

    @staticmethod
    def create_stop_all_inputs_order(details: str):
        """

        :return:
        """
        return CollectorOrder(
            order_id=CollectorOrder.STOP_ALL_INPUT_OBJECTS,
            details=details,
            order_type=CollectorOrder.INPUT_TYPE
        )

    @staticmethod
    def create_stop_input_controller_order(details: str):
        """

        :return:
        """
        return CollectorOrder(
            order_id=CollectorOrder.STOP_INPUT_CONTROLLER,
            details=details,
            order_type=CollectorOrder.INPUT_TYPE
        )

    # Output orders

    @staticmethod
    def create_stop_output_controller_order(details: str):
        """

        :return:
        """
        return CollectorOrder(
            order_id=CollectorOrder.STOP_OUTPUT_CONTROLLER,
            details=details,
            order_type=CollectorOrder.OUTPUT_TYPE
        )

    @staticmethod
    def create_pause_all_sender_managers_order(details: str):
        """

        :return:
        """
        return CollectorOrder(
            order_id=CollectorOrder.PAUSE_ALL_SENDER_MANAGERS,
            details=details,
            order_type=CollectorOrder.OUTPUT_TYPE
        )

    @staticmethod
    def create_flush_all_sender_managers_order(details: str):
        """

        :return:
        """
        return CollectorOrder(
            order_id=CollectorOrder.FLUSH_ALL_SENDER_MANAGERS,
            details=details,
            order_type=CollectorOrder.OUTPUT_TYPE
        )

    @staticmethod
    def create_stop_all_sender_managers_order(details: str):
        """

        :return:
        """
        return CollectorOrder(
            order_id=CollectorOrder.STOP_ALL_SENDER_MANAGERS,
            details=details,
            order_type=CollectorOrder.OUTPUT_TYPE
        )

    @staticmethod
    def create_pause_all_consumers_order(details: str):
        """

        :return:
        """
        return CollectorOrder(
            order_id=CollectorOrder.PAUSE_ALL_CONSUMERS,
            details=details,
            order_type=CollectorOrder.OUTPUT_TYPE
        )

    @staticmethod
    def create_flush_all_consumers_order(details: str):
        """

        :return:
        """
        return CollectorOrder(
            order_id=CollectorOrder.FLUSH_ALL_CONSUMERS,
            details=details,
            order_type=CollectorOrder.OUTPUT_TYPE
        )


    @staticmethod
    def create_stop_all_consumers_order(details: str):
        """

        :return:
        """
        return CollectorOrder(
            order_id=CollectorOrder.STOP_ALL_CONSUMERS,
            details=details,
            order_type=CollectorOrder.OUTPUT_TYPE
        )
