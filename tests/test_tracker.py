import pyhermes
import tempfile


class Checker1(pyhermes.Checker):
    def __init__(self):
        super(Checker1, self).__init__()

    def check(self, transaction, _):
        for e in transaction:
            self.assert_(e.v == e.time, f"Expect {e.time}, got {e.v}")


def test_checker():
    with tempfile.TemporaryDirectory() as temp:
        temp = "temp"
        serializer = pyhermes.Serializer(temp)
        logger = pyhermes.Logger("test")
        dummy_serializer = pyhermes.DummyEventSerializer()
        dummy_serializer.connect(serializer)

        t = None
        for i in range(100):
            e = pyhermes.Event(i)
            e.add_value("v", i if i != 99 else 101)
            if i % 10 == 0:
                t = pyhermes.Transaction()
            t.add_event(e)
            if i % 10 == 9:
                logger.log(t)

        pyhermes.default_bus().flush()
        serializer.finalize()

        loader = pyhermes.Loader(temp)
        checker = Checker1()
        checker.run("test", loader)


if __name__ == "__main__":
    test_checker()
