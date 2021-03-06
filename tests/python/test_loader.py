import pyhermes
import tempfile
import json
import os
import socket
import pytest


class Tracker(pyhermes.Tracker):
    def __init__(self):
        pyhermes.Tracker.__init__(self, "*")
        self.current_transaction = None
        self.transaction_name = "test"

    def track(self, event: pyhermes.Event):
        if event.id % 10 == 0:
            self.current_transaction = self.get_new_transaction()
        self.current_transaction.add_event(event)
        if event.id % 10 == 9:
            self.current_transaction.finish()


def setup_loader_test(temp):
    tracker = Tracker()
    logger = pyhermes.Logger("test")
    dummy_serializer = pyhermes.DummyEventSerializer()
    serializer = pyhermes.Serializer(temp)
    dummy_serializer.connect(serializer)
    tracker.connect(serializer)

    for i in range(100):
        e = pyhermes.Event(i)
        e.add_value("v", i)
        logger.log(e)

    pyhermes.default_bus().flush()
    serializer.finalize()
    return serializer


def test_loader_stream():
    with tempfile.TemporaryDirectory() as temp:
        setup_loader_test(temp)
        loader = pyhermes.Loader(temp)
        transactions = loader["test"]
        assert len(transactions) == 10
        values = set()
        for trans in transactions:
            assert len(trans) == 10
            for e in trans:
                values.add(e.v)
            assert trans[-1] is not None
            assert trans.name == "test"

        assert len(values) == 100
        assert transactions[4].id > 0
        assert transactions[4].name == "test"


def test_transaction_group_stream():
    with tempfile.TemporaryDirectory() as temp:
        logger = pyhermes.Logger("test")
        dummy_serializer = pyhermes.DummyEventSerializer()
        serializer = pyhermes.Serializer(temp)
        dummy_serializer.connect(serializer)

        time = 0
        for i in range(100):
            g = pyhermes.TransactionGroup()
            for t in range(10):
                transaction = pyhermes.Transaction()
                for e in range(5):
                    e = pyhermes.Event(time)
                    time += 1
                    transaction.add_event(e)
                    logger.log(e)
                g.add_transaction(transaction)
                logger.log(transaction)
            # use a new topic to avoid stream conflict
            logger.log("test-g", g)

            if i == 49:
                dummy_serializer.flush()

        pyhermes.default_bus().flush()
        serializer.finalize()

        loader = pyhermes.Loader(temp)
        groups = loader["test-g"]
        assert len(groups) == 100
        count = 0
        for group in groups:
            assert group.id == count
            assert group.is_group
            assert len(group) == 10
            t = group[0]
            assert not t.is_group
            e = t[1]
            assert isinstance(e, pyhermes.Event)
            count += 1
        assert count == len(groups)
        group_data = json.loads(groups.json())
        assert len(group_data) == 100
        group = group_data[0]
        assert len(group) == 10
        assert len(group[0]["events"]) == 5

        # try to load half of it
        groups = loader["test-g", (0, 49)]
        assert len(groups) == 50


def test_event_schema():
    with tempfile.TemporaryDirectory() as temp:
        setup_loader_test(temp)
        loader = pyhermes.Loader(temp)
        schema = loader.get_event_schema("test")
        assert len(schema) > 0


def test_stream_filter():
    with tempfile.TemporaryDirectory() as temp:
        setup_loader_test(temp)
        loader = pyhermes.Loader(temp)
        stream = loader["test"]
        stream = stream.where(lambda x: x.id % 2 == 0)
        assert len(stream) == 5


def is_port_open(port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    result = sock.connect_ex(('localhost', port))
    sock.close()
    return result == 0


def test_s3_fs():
    if not is_port_open(4566):
        pytest.skip("localstack not available")
    # use localstack to test
    path = "s3://test/test"
    endpoint = "http://localhost:4566"
    fs = pyhermes.FileSystemInfo(path)
    fs.endpoint = endpoint
    fs.secret_key = "test"
    fs.access_key = "test"

    # set credential
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"

    setup_loader_test(fs)
    loader = pyhermes.Loader(fs)
    stream = loader["test"]
    assert len(stream) > 0


if __name__ == "__main__":
    test_stream_filter()
