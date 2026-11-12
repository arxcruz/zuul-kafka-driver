# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

"""Register out-of-tree drivers with Zuul without modifying Zuul.

Zuul builds its driver list by hand in
``zuul.lib.connections.ConnectionRegistry.__init__``.  This module
installs a meta path finder that waits for ``zuul.lib.connections`` to
be imported, then wraps that constructor so it also registers every
driver advertised in the ``zuul.drivers`` entry point group.

``install()`` is called from ``zuul_kafka_driver.pth`` at interpreter
startup, so it must stay cheap and must not import Zuul itself.
"""

import importlib.abc
import logging
import sys

TARGET = 'zuul.lib.connections'
ENTRY_POINT_GROUP = 'zuul.drivers'

log = logging.getLogger('zuul.ExternalDrivers')


def load_external_drivers(registry):
    from importlib.metadata import entry_points

    for ep in entry_points(group=ENTRY_POINT_GROUP):
        try:
            driver_class = ep.load()
            if driver_class.name in registry.drivers:
                continue
            registry.registerDriver(driver_class())
            log.debug("Registered external driver %s from %s",
                      driver_class.name, ep.value)
        except Exception:
            log.exception("Unable to load external Zuul driver %s",
                          ep.value)


def patch(module):
    registry_class = module.ConnectionRegistry
    if getattr(registry_class, '_external_drivers_patched', False):
        return
    orig_init = registry_class.__init__

    def __init__(self, *args, **kw):
        orig_init(self, *args, **kw)
        load_external_drivers(self)

    registry_class.__init__ = __init__
    registry_class._external_drivers_patched = True


class _Finder(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path, target=None):
        if fullname != TARGET:
            return None
        for finder in sys.meta_path:
            if finder is self or not hasattr(finder, 'find_spec'):
                continue
            spec = finder.find_spec(fullname, path, target)
            if spec is not None:
                break
        else:
            return None
        loader = spec.loader
        orig_exec_module = loader.exec_module

        def exec_module(module):
            orig_exec_module(module)
            patch(module)

        loader.exec_module = exec_module
        return spec


def install():
    if TARGET in sys.modules:
        patch(sys.modules[TARGET])
        return
    if not any(isinstance(f, _Finder) for f in sys.meta_path):
        sys.meta_path.insert(0, _Finder())
