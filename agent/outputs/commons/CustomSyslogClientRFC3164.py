import pysyslogclient


class CustomSyslogClientRFC3164(pysyslogclient.SyslogClientRFC3164):

    def __init__(self, server: str, port: int):
        pysyslogclient.SyslogClientRFC3164.__init__(
            self,
            server=server,
            port=port,
            octet=pysyslogclient.OCTET_STUFFING)
        self.max_message_length = None

    def send(self, message_data: str) -> None:
        """

        :param message_data:
        :return:
        """

        if self.socket is not None or self.connect():
            try:
                if self.max_message_length is not None:
                    self.socket.sendall(message_data[:self.max_message_length])
                else:
                    self.socket.sendall(message_data)
            except Exception as e:
                self.close()
                raise e
        else:
            raise Exception("Connection can not be established")

    def __str__(self):
        return f'{{"client_name": "{self.client_name}", "url": "{self.server}:{self.port}", "object_id": "{id(self)}"}}'
