"""Shelve DB session management for MasterDB translation cache."""

import shelve

_db_handle = None
_db_depth = 0


class db_session:
    """Context manager for batched shelve access.

    Opens the DB once and reuses the handle for all DB_save/DB_get calls
    within the block. Supports nesting — inner sessions reuse the outer handle.

    Usage:
        with db_session():
            DB_save("key", "value")
            val = DB_get("key")
    """

    def __enter__(self):
        global _db_handle, _db_depth
        if _db_handle is None:
            _db_handle = shelve.open('DB.dat')
        _db_depth += 1
        return _db_handle

    def __exit__(self, *exc):
        global _db_handle, _db_depth
        _db_depth -= 1
        if _db_depth <= 0 and _db_handle is not None:
            _db_handle.close()
            _db_handle = None
            _db_depth = 0


def DB_save(key, value):
    global _db_handle
    if _db_handle is not None:
        _db_handle[key] = value
    else:
        with shelve.open('DB.dat') as d:
            d[key] = value


def DB_get(key, default=""):
    global _db_handle
    if _db_handle is not None:
        return _db_handle.get(key, default)
    else:
        with shelve.open('DB.dat') as d:
            return d.get(key, default)
