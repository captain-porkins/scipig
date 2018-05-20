import os
import pandas as pd
from tempfile import gettempdir
from datetime import datetime
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


def _empty_meta():
    index = pd.MultiIndex.from_tuples([], names=['function_name', 'argument_hash'])
    columns = ['key', 'added']

    return pd.DataFrame(index=index, columns=columns)


def default_hdf_cache(func):
    global __default_store_path
    if __default_store_path is None:
        __default_store_path = os.path.join(gettempdir(), '_scipig_default.h5')

    cache_decorator = _hdf_cache(store_path=__default_store_path, size_limit=None)

    return cache_decorator(func)


def make_arg_hash(*args, **kwargs):
    # ToDo : limitation for equivalent sets of args/kwargs with different division between the two
    return hash((tuple(args), tuple(kwargs.items())))


def _input_data(df, store, func_name, *args, **kwargs):
    meta = store['meta']
    new_key = meta['key'].max() + 1

    if new_key != new_key:  # i.e. it's NaN because the meta table is empty
        new_key = 0

    meta.loc[(func_name, make_arg_hash(*args, **kwargs)), :] = [new_key, datetime.now()]

    store['meta'] = meta
    store[str(new_key)] = df


def _tidy_store(store, size_limit):
    # ToDo:
    # clean the hdf
    # check size_limit against hdf store size
    # as subprocess?
    pass


def _hdf_cache(store_path, size_limit):
    def decorator(func):
        def decorated_func(*args, **kwargs):
            with pd.HDFStore(path=store_path) as hdf_store:
                try:
                    meta = hdf_store['meta']
                except KeyError:
                    meta = _empty_meta()
                    hdf_store['meta'] = meta

                try:
                    row = meta.loc[func.__name__, make_arg_hash(*args, **kwargs), :]
                except KeyError:
                    ret = func(*args, **kwargs)
                    _input_data(df=ret, store=hdf_store, func_name=func.__name__, *args, **kwargs)
                else:
                    ret = hdf_store[str(row['key'].item())]
                finally:
                    _tidy_store(store=hdf_store, size_limit=size_limit)

            return ret

        return decorated_func

    return decorator


def hdf_cache(*args, **kwargs):
    if len(args) == 1 and len(kwargs) == 0 and callable(args[0]):  # If being used directly as decorator
        return default_hdf_cache(args[0])
    else:
        return _hdf_cache(*args, **kwargs)  # If being used as a decorator factory


@hdf_cache
def __test():
    from time import sleep
    print('doing the stuff')
    sleep(60)
    return pd.DataFrame([range(3), range(3)])


if __name__ == '__main__':
    print(__test())
    print(__test())
