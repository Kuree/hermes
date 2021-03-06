import pyhermes
import json


def test_event():
    e = pyhermes.Event(0)
    # test the setattr interface
    e.a = 0
    e.b = False
    e.c = "42"
    # test getattr interface
    assert e.a == 0
    assert not e.b
    assert e.c == "42"
    # testing the add_value interface
    e.add_value("d", 42)
    assert e.d == 42
    # set get/set item
    e["e"] = "e"
    assert e["e"] == "e"
    # invalid access
    try:
        _ = e["f"]
        assert False
    except ValueError:
        pass
    data_e = json.loads(e.json())
    assert data_e["value"]["e"] == "e"


def test_event_batch():
    batch = pyhermes.EventBatch()
    for i in range(42):
        e = pyhermes.Event(i)
        e.a = i + 1
        batch.append(e)
    assert len(batch) == 42
    for i, e in enumerate(batch):
        assert e.time == i

    assert batch[-1] == batch[42-1]
    res = batch.where(lambda event: event.time >= 20)
    assert len(res) == 42 - 20
    new_batch = batch[0:-1:2]
    assert len(new_batch) == 21
    batch_data = json.loads(batch.json())
    assert len(batch_data) == 42
    assert batch_data[20]["value"]["a"] == 21


def test_transaction():
    transaction_batch = pyhermes.TransactionBatch()
    t = None
    for i in range(100):
        e = pyhermes.Event(i)
        e.a = i
        if i % 10 == 0:
            t = pyhermes.Transaction()
        t.add_event(e)
        if i % 10 == 9:
            transaction_batch.append(t)
        t.aa = i
    assert len(transaction_batch) == 10
    assert transaction_batch[9].aa == 99


if __name__ == "__main__":
    test_event_batch()
