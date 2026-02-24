from dataclasses import dataclass
from typing import List, Union


@dataclass
class Knob:
    name: str
    value: Union[int, float]


@dataclass
class KnobConfig:
    """A class representing a configuration of database knobs, which can be used to optimize performance for a given workload.
    Attributes:
        knobs: A list of Knob objects, each representing a specific database configuration knob and its corresponding value.

    Methods:
        from_dict: A class method that creates a KnobConfig instance from a dictionary of knob names and values.
        to_dict: An instance method that converts the KnobConfig instance back into a dictionary format for easy manipulation and storage.

    Example usage:
    ```
    config_dict = {
        "shared_buffers": "128MB",
        "work_mem": "4MB",
        "effective_cache_size": "512MB"
    }
    knob_config = KnobConfig.from_dict(config_dict)
    print(knob_config)
    print(knob_config.to_dict())
    ```
    """

    knobs: List[Knob]

    @classmethod
    def from_dict(cls, config_dict: dict) -> "KnobConfig":
        knobs = [Knob(name=key, value=value) for key, value in config_dict.items()]
        return cls(knobs=knobs)

    def to_dict(self) -> dict:
        return {knob.name: knob.value for knob in self.knobs}

    def items(self):
        return self.to_dict().items()
