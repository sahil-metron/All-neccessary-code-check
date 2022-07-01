import time
from threading import Thread


class EmergencyPersistenceService(Thread):

    def start(self) -> None:
        super().start()

    def run(self) -> None:
        while True:
            time.sleep(600)
