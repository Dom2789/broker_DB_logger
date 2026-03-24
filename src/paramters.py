from dataclasses import dataclass

@dataclass
class Parameters:
    path : str
    file_name : str
    broker_IP : str
    broker_port : int
    topic : str

    def __str__(self):
        return (
            f"Parameters:\n"
            f"  path        : {self.path}\n"
            f"  file_name   : {self.file_name}\n"
            f"  broker_IP   : {self.broker_IP}\n"
            f"  broker_port : {self.broker_port}\n"
            f"  topic       : {self.topic}"
        )
