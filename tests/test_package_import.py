import dask_elasticsearch


def test_package_metadata():
    assert dask_elasticsearch.__author__
    assert dask_elasticsearch.__email__
    assert dask_elasticsearch.__version__
