from agent.outputs.senders.abstracts.sender_manager_monitor_abstract import SenderManagerMonitorAbstract


class DevoSenderManagerMonitor(SenderManagerMonitorAbstract):
	""" Monitor for Console Sender Monitor """

	def custom_hook_01(self):
		""" Method that request to senders instance to check if the senders should be disabled """
		self._manager_to_monitor.senders.check_if_senders_should_be_disabled()
