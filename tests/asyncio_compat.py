import unittest

try:
    import uvloop
except ImportError:  # uvloop is unavailable on Windows and some Python builds.
    uvloop = None


TEST_CLIENT_BACKEND_OPTIONS = {"use_uvloop": True} if uvloop is not None else {}


class ThreadedAsyncTestCase(unittest.IsolatedAsyncioTestCase):
    """Run tests that require reliable cross-thread event-loop wakeups."""

    if uvloop is not None:
        loop_factory = staticmethod(uvloop.new_event_loop)
