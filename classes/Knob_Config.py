from dataclasses import dataclass
from typing import List, Union


@dataclass
class Knob:
    name: str
    value: Union[int, float]


@dataclass
class KnobConfig:
    knobs: List[Knob]

    @classmethod
    def from_dict(cls, config_dict: dict) -> "KnobConfig":
        knobs = [Knob(name=key, value=value) for key, value in config_dict.items()]
        return cls(knobs=knobs)

    def to_dict(self) -> dict:
        return {knob.name: knob.value for knob in self.knobs}
