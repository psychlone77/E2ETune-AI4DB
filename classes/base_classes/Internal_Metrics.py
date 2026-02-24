from typing import TypedDict


class InternalMetrics(TypedDict):
    xact_commit: float
    xact_rollback: float
    blks_read: float
    blks_hit: float
    tup_returned: float
    tup_fetched: float
    tup_inserted: float
    conflicts: float
    tup_updated: float
    tup_deleted: float
    disk_read_count: float
    disk_write_count: float
    disk_read_bytes: float
    disk_write_bytes: float
