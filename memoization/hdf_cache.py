import os
from pandas import HDFStore
from tempfile import gettempdir

"""
Module supporting the caching of pandas objects in a hdf store by use of a decorator to wrap functions which
return them.
HDF structure:
/meta:
                                        | pk |
    func_name |(args, kwargs.items())   | ...|
              |                         |    |

/primary_keys:

    returned object from func_name
"""

__default_store_path = None


def default_hdf_cache(func):
    global __default_store_path
    if __default_store_path is None:
        __default_store_path = os.path.join(gettempdir(), '_scipig_default.h5')

    cache_decorator = _hdf_cache(store_path=__default_store_path, size_limit=None)

    return cache_decorator(func)


def _hdf_cache(store_path, size_limit):
    def decorator(func):
        def decorated_func(*args, **kwargs):
            pass
        return decorated_func

    return decorator


def hdf_cache(*args, **kwargs):
    if len(args) == 1 and len(kwargs) == 0 and callable(args[0]):  # If being used directly as decorator
        return default_hdf_cache(args[0])
    else:
        return _hdf_cache(*args, **kwargs)  # If being used as a decorator factory
