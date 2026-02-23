from typing import TypedDict


class InternalMetrics(TypedDict):
    xact_commit: int
    xact_rollback: int
    blks_read: int
    blks_hit: int
    tup_returned: int
    tup_fetched: int
    tup_inserted: int
    conflicts: int
    tup_updated: int
    tup_deleted: int
    disk_read_count: int
    disk_write_count: int
