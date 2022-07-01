from agent.outputs.senders.devo_sender_manager import DevoSenderManager
from agent.outputs.senders.console_sender_manager import ConsoleSenderManager
from agent.outputs.senders.syslog_sender_manager import SyslogSenderManager

outputs = {
  'devo_platform': DevoSenderManager,
  'relay': DevoSenderManager,
  'in_house_relay': SyslogSenderManager,
  'syslog': SyslogSenderManager,
  'sidecar': SyslogSenderManager,
  'console': ConsoleSenderManager,
  'stdout': ConsoleSenderManager
}
