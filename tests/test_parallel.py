"""Tests for parallel processing helpers."""

import pytest

from scripts.parallel import run_parallel, collect_errors_and_successes, collect_dict_results


# Top-level functions for pickling (multiprocessing requirement)
def _square(x):
    return x * x


def _failing(x):
    if x == 3:
        raise ValueError("bad value")
    return x


def _error_success(x):
    if x % 2 == 0:
        return ([], [x])
    else:
        return ([(f"err_{x}", x)], [])


class TestRunParallel:
    def test_basic(self):
        results = run_parallel(_square, [1, 2, 3, 4])
        assert sorted(results) == [1, 4, 9, 16]

    def test_empty_items(self):
        results = run_parallel(_square, [])
        assert results == []

    def test_single_item(self):
        results = run_parallel(_square, [5])
        assert results == [25]

    def test_pool_cleanup_on_success(self):
        """Pool should be properly cleaned up after successful run."""
        # No zombie processes — if this hangs, cleanup is broken
        for _ in range(3):
            run_parallel(_square, [1, 2])

    def test_large_batch(self):
        items = list(range(100))
        results = run_parallel(_square, items)
        assert sorted(results) == [x * x for x in items]


class TestCollectErrorsAndSuccesses:
    def test_mixed_results(self):
        results = [
            ([], ["file1.txt"]),
            ([("error", "file2.txt")], []),
            ([], ["file3.txt", "file4.txt"]),
        ]
        errors, successes = collect_errors_and_successes(results)
        assert len(errors) == 1
        assert len(successes) == 3
        assert errors[0] == ("error", "file2.txt")

    def test_all_success(self):
        results = [([], ["a"]), ([], ["b"])]
        errors, successes = collect_errors_and_successes(results)
        assert errors == []
        assert successes == ["a", "b"]

    def test_all_errors(self):
        results = [([("e1", "f1")], []), ([("e2", "f2")], [])]
        errors, successes = collect_errors_and_successes(results)
        assert len(errors) == 2
        assert successes == []

    def test_empty(self):
        errors, successes = collect_errors_and_successes([])
        assert errors == []
        assert successes == []


class TestCollectDictResults:
    def test_merge(self):
        results = [{"a": 1}, {"b": 2}, {"c": 3}]
        merged = collect_dict_results(results)
        assert merged == {"a": 1, "b": 2, "c": 3}

    def test_empty_dicts(self):
        results = [{}, {}, {}]
        merged = collect_dict_results(results)
        assert merged == {}

    def test_empty_list(self):
        merged = collect_dict_results([])
        assert merged == {}

    def test_overlapping_keys_last_wins(self):
        results = [{"a": 1}, {"a": 2}]
        merged = collect_dict_results(results)
        assert merged["a"] == 2
