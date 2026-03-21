"""Tests for shelve session management improvement."""

import pytest


class TestDBSession:
    """Test that DB_save/DB_get can work in session mode."""

    def test_session_save_and_get(self, shelve_test_cleanup):
        """Multiple save/get within a session should be efficient."""
        from scripts.masterdb2 import DB_save, DB_get, db_session

        with db_session():
            DB_save("session_key1", "value1")
            DB_save("session_key2", "value2")
            assert DB_get("session_key1") == "value1"
            assert DB_get("session_key2") == "value2"

    def test_session_persists_after_close(self, shelve_test_cleanup):
        """Data saved in one session should be readable in another."""
        from scripts.masterdb2 import DB_save, DB_get, db_session

        with db_session():
            DB_save("persist_key", "persist_value")

        with db_session():
            assert DB_get("persist_key") == "persist_value"

    def test_without_session_still_works(self, shelve_test_cleanup):
        """DB_save/DB_get should still work without explicit session."""
        from scripts.masterdb2 import DB_save, DB_get

        DB_save("no_session_key", "no_session_value")
        assert DB_get("no_session_key") == "no_session_value"

    def test_nested_session_reuses(self, shelve_test_cleanup):
        """Nested session calls should reuse the same handle."""
        from scripts.masterdb2 import DB_save, DB_get, db_session

        with db_session():
            DB_save("outer_key", "outer")
            with db_session():
                DB_save("inner_key", "inner")
                assert DB_get("outer_key") == "outer"
            assert DB_get("inner_key") == "inner"
