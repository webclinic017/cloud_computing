from dataclasses import dataclass

@dataclass
class DissMessage:

    topic: str
    value: str

    def info(self):
        return self.topic, self.value

@dataclass
class RegistrationMessage:

    type: str
    topics: list
    id: str

    def info(self):

        return self.type, self.topics, self.id
