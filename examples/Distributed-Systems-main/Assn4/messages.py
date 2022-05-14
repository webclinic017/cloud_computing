from dataclasses import dataclass

@dataclass
class DissMessage:

    topic: str
    value: str
    history_length: int

    def info(self):
        return self.topic, self.value, self.history_length

@dataclass
class RegistrationMessage:

    type: str
    topics: list
    id: str
    history_length: int

    def info(self):

        return self.type, self.topics, self.id, self.history_length
