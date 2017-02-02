# -*- coding: utf-8 -*-
"""An Elasticsearch reader for Dask.
"""
from dask import delayed
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan


__author__ = 'Rolando (Max) Espinoza'
__email__ = 'rolando at rmax.io'
__version__ = '0.1.0-dev'


def _elasticsearch_scan(client_cls, client_kwargs, **params):
    # This method is executed in the worker's process and here we instantiate
    # the ES client as it cannot be serialized.
    client = client_cls(**(client_kwargs or {}))
    return list(scan(client, **params))


def read_elasticsearch(query=None, npartitions=8, client_cls=None,
                       client_kwargs=None, **kwargs):
    """Reads documents from Elasticsearch.

    By default, documents are sorted by ``_doc``. For more information see the
    scrolling section in Elasticsearch documentation.

    Parameters
    ----------
    query : dict, optional
        Search query.
    npartitions : int, optional
        Number of partitions, default is 8.
    client_cls : elasticsearch.Elasticsearch, optional
        Elasticsearch client class.
    client_kwargs : dict, optional
        Elasticsearch client parameters.
    **params
        Additional keyword arguments are passed to the the
        ``elasticsearch.helpers.scan`` function.

    Returns
    -------
    out : List[Delayed]
        A list of ``dask.Delayed`` objects.

    Examples
    --------

    Get all documents in elasticsearch.

    >>> docs = dask.bag.from_delayed(read_elasticsearch())

    Get documents matching a given query.

    >>> query = {"query": {"match_all": {}}}
    >>> docs = dask.bag.from_delayed(read_elasticsearch(query, index="myindex", doc_type="stuff"))


    """
    query = query or {}
    # Sorting by _doc is preferred for scrolling.
    query.setdefault('sort', ['_doc'])
    if client_cls is None:
        client_cls = Elasticsearch
    values = []
    # We load documents in parallel using the scrolling + slicing feature.
    for idx in range(npartitions):
        slice = {'id': idx, 'max': npartitions}
        scan_kwargs = dict(kwargs, query=dict(query, slice=slice))
        values.append(
            delayed(_elasticsearch_scan)(client_cls, client_kwargs, **scan_kwargs)
        )
    return values
