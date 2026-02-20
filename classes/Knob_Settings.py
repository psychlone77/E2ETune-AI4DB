from dataclasses import dataclass
from typing import Dict, Any, List, Union, Literal
import json


@dataclass
class KnobSettings:
    name: str
    describe: str
    type: Literal["integer", "float"]
    default: Union[int, float]
    min: Union[int, float]
    max: Union[int, float]
    step: Union[int, float]

    @classmethod
    def from_dict(cls, name: str, config: Dict[str, Any]) -> "KnobSettings":
        """Create a Knob instance from dictionary config."""
        return cls(
            name=name,
            describe=config["describe"],
            type=config["type"],
            default=config["default"],
            min=config["min"],
            max=config["max"],
            step=config["step"],
        )


@dataclass
class KnobSettingsSet:
    knobs: List[KnobSettings]

    @classmethod
    def from_json_file(cls, filepath: str) -> "KnobSettingsSet":
        """Load knob configuration from JSON file."""
        with open(filepath, "r") as f:
            config_dict = json.load(f)

        knobs = [
            KnobSettings.from_dict(name, config) for name, config in config_dict.items()
        ]
        return cls(knobs=knobs)

    def to_dict(self) -> Dict[str, Dict[str, Any]]:
        """Convert back to dictionary format."""
        return {
            knob.name: {
                "describe": knob.describe,
                "type": knob.type,
                "default": knob.default,
                "min": knob.min,
                "max": knob.max,
                "step": knob.step,
            }
            for knob in self.knobs
        }

    def get_knob(self, name: str) -> KnobSettings:
        """Get a specific knob by name."""
        for knob in self.knobs:
            if knob.name == name:
                return knob
        raise KeyError(f"Knob '{name}' not found")

    def get_default_knob_settings(self) -> Dict[str, Union[int, float]]:
        """Get dictionary of all default values."""
        return {knob.name: knob.default for knob in self.knobs}
