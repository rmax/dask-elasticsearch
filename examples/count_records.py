import sys

import dask.bag

from dask_elasticsearch import read_elasticsearch
from dask.diagnostics import progress


def main():
    if not sys.argv[1:]:
        sys.stderr.write("Usage: python %s <query>\n" % sys.argv[0])
        sys.exit(1)

    search_body = {"query": {"match": {"_all": sys.argv[1]}}}
    data = dask.bag.from_delayed(read_elasticsearch(search_body, npartitions=8))
    print("Partitions: %s" % data.npartitions)

    task = data.count()
    with progress.ProgressBar():
        count = task.compute()

    print("Result: %s" % count)


if __name__ == "__main__":
    main()
