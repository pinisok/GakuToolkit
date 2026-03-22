"""Common multiprocessing helpers with proper resource management."""

import multiprocessing
import tqdm

from .log import *


def run_parallel(worker_fn, items, desc="Processing"):
    """Run worker_fn on items in parallel with progress bar.

    Uses context manager for proper Pool cleanup on exceptions.

    Args:
        worker_fn: Callable that takes a single item and returns a result.
            Must be picklable (top-level function).
        items: List of items to process.
        desc: Description for the progress bar.

    Returns list of results (one per item, in arbitrary order).
    """
    if not items:
        return []

    results = []
    with multiprocessing.Pool() as pool:
        with tqdm.tqdm(total=len(items), desc=desc) as pbar:
            for result in pool.imap_unordered(worker_fn, items):
                results.append(result)
                pbar.update()
                pbar.refresh()
    return results


def collect_errors_and_successes(results):
    """Collect (error_list, success_list) from parallel results.

    Each result is expected to be (error_list, success_list) tuple.
    """
    errors = []
    successes = []
    for err, succ in results:
        errors.extend(err)
        successes.extend(succ)
    return errors, successes


def collect_dict_results(results):
    """Merge list of dicts into one dict."""
    merged = {}
    for d in results:
        merged.update(d)
    return merged
