from abc import ABC, abstractmethod
from classes.base_classes.Knob_Config import KnobConfig


class Tuner(ABC):
    """A class representing a tuner that optimizes database configuration knobs for a given workload.
    This class serves as a base for specific tuning algorithms, such as HEBO, which will implement the actual tuning logic.

    Methods:
        tune: An abstract method that should be implemented by subclasses to perform the tuning process and return the optimized knob configuration.

    Example usage:
    ```
    class MyTuner(Tuner):
        def tune(self):
            # Implement tuning logic here
            return KnobConfig(knobs=[Knob(name="shared_buffers", value="128MB")])
    my_tuner = MyTuner()
    optimized_config = my_tuner.tune()
    print(optimized_config)
    ```
    """

    @abstractmethod
    def tune(self) -> KnobConfig:
        """Tune the database configuration knobs to optimize performance for the given workload.
        Returns:
            A KnobConfig object containing the optimized knob settings.
        """
        pass
