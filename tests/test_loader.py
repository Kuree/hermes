import pyhermes
import _pyhermes
import tempfile
import time


def test_loader():
    with tempfile.TemporaryDirectory() as temp:
        temp = "temp"
        serializer = _pyhermes.Serializer(temp)
        logger = _pyhermes.Logger("test")
        dummy_serializer = _pyhermes.DummyEventSerializer()
        dummy_serializer.connect(serializer)

        for i in range(100):
            e = _pyhermes.Event(i)
            e.add_value("v", i)
            logger.log(e)

        _pyhermes.default_bus().flush()
        serializer.finalize()
        loader = pyhermes.Loader(temp)
        assert len(loader.events_df) == 1


if __name__ == "__main__":
    test_loader()
