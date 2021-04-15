import pyhermes
import tempfile


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
        assert len(values) == 100
        assert transactions[4].id > 0


def test_transaction_group_stream():
    with tempfile.TemporaryDirectory() as temp:
        temp = "temp"
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

        pyhermes.default_bus().flush()
        serializer.finalize()

        loader = pyhermes.Loader(temp)
        groups = loader["test-g"]
        assert len(groups) == 100
        count = 0
        for group in groups:
            assert group.is_group
            assert len(group) == 10
            t = group[0]
            assert not t.is_group
            e = t[1]
            assert isinstance(e, pyhermes.Event)
            count += 1
        assert count == len(groups)


if __name__ == "__main__":
    test_transaction_group_stream()
