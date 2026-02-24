from dataclasses import dataclass
from typing import Dict, Any
import yaml


@dataclass
class DatabaseConfig:
    """Configuration for connecting to a database cluster.

    Attributes:
        host: Database host address or IP.
        port: TCP port number the database listens on.
        user: Username for authenticating to the database.
        password: Password for authenticating to the database.
        name: Logical database name to connect to.
        data_path: Filesystem path to the database data directory.
        pg_version: Major PostgreSQL version number (as int).
        cluster_name: Identifier for the database cluster.
    """

    host: str
    port: int
    user: str
    password: str
    name: str
    data_path: str
    pg_version: int
    cluster_name: str

    @classmethod
    def from_dict(cls, config: Dict[str, Any]) -> "DatabaseConfig":
        """Create a DatabaseConfig instance from dictionary."""
        return cls(
            host=config["host"],
            port=int(config["port"]),
            user=config["user"],
            password=config["password"],
            name=config["name"],
            data_path=config["data_path"],
            pg_version=int(config["pg_version"]),
            cluster_name=config["cluster_name"],
        )


@dataclass
class TuningConfig:
    """Configuration options for the tuning process.

    Attributes:
        method: Name of the tuning method (e.g., 'hebo', 'random').
        sample_num: Number of samples to draw during tuning.
        suggest_num: Number of suggestions to propose each iteration.
        early_stop_plateau: Number of iterations without improvement
            before early stopping.
    """

    method: str
    sample_num: int
    suggest_num: int
    early_stop_plateau: int

    @classmethod
    def from_dict(cls, config: Dict[str, Any]) -> "TuningConfig":
        """Create a TuningConfig instance from dictionary."""
        return cls(
            method=config["method"],
            sample_num=int(config["sample_num"]),
            suggest_num=int(config["suggest_num"]),
            early_stop_plateau=int(config["early_stop_plateau"]),
        )


@dataclass
class BenchmarkConfig:
    """Configuration describing the benchmark/benchmarking tool.

    Attributes:
        type: Benchmark category (e.g., 'oltp', 'olap').
        name: Human-readable name for the benchmark.
        path: Filesystem path to benchmark assets or scripts.
        workload_execution: Command or method to run the workload.
        performance_record_path: Path where performance records are stored.
        benchbase_jar: Path to the BenchBase JAR (if applicable).
    """

    type: str
    name: str
    path: str
    workload_execution: str
    performance_record_path: str
    benchbase_jar: str

    @classmethod
    def from_dict(cls, config: Dict[str, Any]) -> "BenchmarkConfig":
        """Create a BenchmarkConfig instance from dictionary."""
        return cls(
            type=config["type"],
            name=config["name"],
            path=config["path"],
            workload_execution=config["workload_execution"],
            performance_record_path=config["performance_record_path"],
            benchbase_jar=config["benchbase_jar"],
        )


@dataclass
class SurrogateConfig:
    """Configuration for a surrogate model used during tuning.

    Attributes:
        model_path: Filesystem path to the surrogate model artifact.
    """

    model_path: str

    @classmethod
    def from_dict(cls, config: Dict[str, Any]) -> "SurrogateConfig":
        """Create a SurrogateConfig instance from dictionary."""
        return cls(
            model_path=config["model_path"],
        )


@dataclass
class ScriptConfig:
    """Top-level script configuration container.

    This aggregates `DatabaseConfig`, `TuningConfig`, `BenchmarkConfig`,
    and `SurrogateConfig` into a single object that can be loaded from a
    YAML file and passed around the tuning and benchmarking code.

    Attributes:
        database_config: Settings required to connect to and identify the DB.
        tuning_config: Settings that control the tuner behavior.
        benchmark_config: Settings that describe the benchmark and execution.
        surrogate_config: Settings related to any surrogate model used.
    """

    database_config: DatabaseConfig
    tuning_config: TuningConfig
    benchmark_config: BenchmarkConfig
    surrogate_config: SurrogateConfig

    @classmethod
    def from_yaml_file(cls, filepath: str) -> "ScriptConfig":
        """Load script configuration from YAML file."""
        with open(filepath, "r") as f:
            config_dict = yaml.safe_load(f)

        return cls(
            database_config=DatabaseConfig.from_dict(config_dict["database"]),
            tuning_config=TuningConfig.from_dict(config_dict["tuning"]),
            benchmark_config=BenchmarkConfig.from_dict(config_dict["benchmark"]),
            surrogate_config=SurrogateConfig.from_dict(config_dict["surrogate"]),
        )

    def to_dict(self) -> Dict[str, Dict[str, Any]]:
        """Convert back to dictionary format."""
        return {
            "database": self.database_config.__dict__,
            "tuning": self.tuning_config.__dict__,
            "benchmark": self.benchmark_config.__dict__,
            "surrogate": self.surrogate_config.__dict__,
        }
