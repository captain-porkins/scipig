import pytest
import pandas as pd
from scipig.memoization import hdf_cache


@pytest.fixture()
def tst_dataframe():
    return pd.DataFrame([range(3), range(3)], columns=['a', 'b', 'c'])


# default:
def test_stored(tst_dataframe):
    """ Test result of function doesn't change if cached """
    decorated_func = hdf_cache(lambda _: tst_dataframe)
    result_1 = decorated_func()
    result_2 = decorated_func()
    assert result_1 == result_2


# with custom: