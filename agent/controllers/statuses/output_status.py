from typing import Optional

from agent.controllers.statuses.component_status import ComponentStatus


class OutputStatus:

    def __init__(self):

        self.controller_process: Optional[ComponentStatus] = None
        self.consumers: Optional[dict] = None
        self.sender_lists: Optional[dict] = None
        self.controller_process: Optional[int] = None
