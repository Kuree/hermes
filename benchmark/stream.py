import argparse
import os
import time

import pyhermes


def parse_args():
    parser = argparse.ArgumentParser("Benchmark for S3 backend")
    parser.add_argument("-n", help="number of events", default=100000, type=int, dest="num_events")
    parser.add_argument("--transaction-size", help="average transaction size", default=10, type=int,
                        dest="transaction_size")
    parser.add_argument("-s", "--s3", help="s3 folder name", type=str, required=True, dest="s3")
    parser.add_argument("--endpoint", help="s3 endpoint", type=str, default="", required=False, dest="endpoint")
    parser.add_argument("--access-id", help="AWS access key id", type=str, default="", dest="access_id")
    parser.add_argument("--secret-key", help="AWS secret key", type=str, default="", dest="secret_key")
    return parser.parse_args()


def main():
    args = parse_args()
    # make sure aws credential is set
    assert "AWS_ACCESS_KEY_ID" in os.environ or args.access_id
    assert "AWS_SECRET_ACCESS_KEY" in os.environ or args.secret_key
    access_key = os.environ["AWS_ACCESS_KEY_ID"] if not args.access_id else args.access_id
    secret_key = os.environ["AWS_SECRET_ACCESS_KEY"] if not args.secret_key else args.secret_key

    fs = pyhermes.FileSystemInfo(args.s3)
    if args.endpoint:
        fs.endpoint = args.endpoint
    fs.access_key = access_key
    fs.secret_key = secret_key

    # set up serializer
    logger = pyhermes.Logger("test")
    dummy_serializer = pyhermes.DummyEventSerializer()
    serializer = pyhermes.Serializer(fs)
    dummy_serializer.connect(serializer)

    start = time.time()
    t = None
    # start to log event
    for i in range(args.num_events):
        e = pyhermes.Event(i)
        if i % args.transaction_size == 0:
            t = pyhermes.Transaction()
        # fake some values
        e.a = f"{i}"
        e.b = i
        e.c = i % 2 == 0
        t.add_event(e)
        logger.log(e)
        if (i + 1) % args.transaction_size == 0:
            logger.log(t)

    pyhermes.default_bus().flush()
    serializer.finalize()

    end = time.time()
    writing_speed = end - start

    # test out reading speed
    start = time.time()
    loader = pyhermes.Loader(fs)
    end = time.time()
    meta_loading = end - start
    loader.print_files()
    print("-" * 40)
    start = time.time()
    stream = loader["test"]
    count = 0
    for e in stream:
        count += 1
        assert e.id >= 0
    end = time.time()
    reading_speed = end - start
    num_transaction = args.num_events / args.transaction_size
    assert count == num_transaction
    print("Write:", "{0:.2f} ms/t".format(writing_speed / num_transaction * 1000), "Total:", f"{writing_speed:.2f}s")
    print("Read:", "{0:.2f} ms/t".format(reading_speed / num_transaction * 1000), "Total:", f"{reading_speed:.2f}s")
    print("Loading:", f"{meta_loading:.2f}s")

    # clean up
    fs.clear()


if __name__ == "__main__":
    main()
