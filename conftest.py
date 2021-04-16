import pytest
import pyhermes


@pytest.fixture(autouse=True)
def reset():
    pyhermes.reset()
