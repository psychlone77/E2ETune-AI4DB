from dataclasses import dataclass
from typing import List, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from classes.base_classes.Knob_Settings import KnobSettingsSet


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
    def from_dict(cls, config_dict: dict, knob_settings: "KnobSettingsSet" = None) -> "KnobConfig":
        """Create a KnobConfig from a dictionary, converting values to proper types.
        
        Args:
            config_dict: Dictionary mapping knob names to values
            knob_settings: Optional KnobSettingsSet to determine proper types
            
        Returns:
            KnobConfig with properly typed values
        """
        knobs = []
        for name, value in config_dict.items():
            # Convert to proper type based on knob settings
            if knob_settings is not None:
                try:
                    knob_setting = knob_settings.get_knob(name)
                    if knob_setting.type == "integer":
                        value = int(round(value))
                    elif knob_setting.type == "float":
                        value = float(value)
                except KeyError:
                    # Knob not found in settings, keep original value
                    pass
            
            knobs.append(Knob(name=name, value=value))
        
        return cls(knobs=knobs)

    def to_dict(self) -> dict:
        return {knob.name: knob.value for knob in self.knobs}

    def items(self):
        return self.to_dict().items()
        
