import pyhermes
import tempfile


def test_parquet_loader_events():
    with tempfile.TemporaryDirectory() as temp:
        serializer = pyhermes.Serializer(temp)
        logger = pyhermes.Logger("test")
        dummy_serializer = pyhermes.DummyEventSerializer()
        dummy_serializer.connect(serializer)

        for i in range(100):
            e = pyhermes.Event(i)
            e.add_value("v", i)
            logger.log(e)

        pyhermes.default_bus().flush()
        serializer.finalize()
        loader = pyhermes.ParquetLoader(temp)
        assert len(loader.events_df) == 1
        df = loader.events_df[0]
        assert len(df) == 100


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
        return self.current_transaction


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


def test_parquet_loader_transaction():
    with tempfile.TemporaryDirectory() as temp:
        setup_loader_test(temp)
        loader = pyhermes.ParquetLoader(temp)
        assert len(loader.transactions_df) > 0
        df = loader.transactions_df["test"]
        assert len(df) == 10


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
        assert len(values) == 100


if __name__ == "__main__":
    test_loader_stream()
