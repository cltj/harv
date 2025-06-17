import pkgutil
import importlib
import inspect

__all__ = []

"""
This script will dynamically import all classes from the submodules 
and make them available at the top level of the harv package.
"""

package_name = __name__
for _, module_name, _ in pkgutil.walk_packages(__path__, package_name + "."):
    module = importlib.import_module(module_name)
    for name, obj in inspect.getmembers(module):
        if inspect.isclass(obj) and obj.__module__ == module_name:
            globals()[name] = obj
            __all__.append(name)