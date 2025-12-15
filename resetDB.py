from typing import Dict, Any
from Database import reset_db_knobs
from config import parse_config


if __name__ == '__main__':
    args = parse_config.parse_args("config/config.ini")
    reset_db_knobs(args)