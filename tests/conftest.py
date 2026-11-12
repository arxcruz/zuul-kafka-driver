import sys
from unittest import mock

import pytest

from zuul.driver import Driver, WrapperInterface


class FakeBubblewrapDriver(Driver, WrapperInterface):
    name = 'bubblewrap'

    def getExecutionContext(self, ro_paths=None, rw_paths=None, secrets=None):
        pass


@pytest.fixture
def registry():
    # The real bubblewrap driver reads /proc, which only exists on Linux.
    patcher = None
    if not sys.platform.startswith('linux'):
        patcher = mock.patch('zuul.driver.bubblewrap.BubblewrapDriver',
                             FakeBubblewrapDriver)
        patcher.start()
    try:
        from zuul.lib.connections import ConnectionRegistry
        yield ConnectionRegistry()
    finally:
        if patcher:
            patcher.stop()
