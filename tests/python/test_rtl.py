import pyhermes
import os


def get_vector(filename):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    vector_dir = os.path.join(os.path.dirname(current_dir), "vectors")
    return os.path.join(vector_dir, filename)


def test_enum_parsing():
    enum_file = get_vector("test_enum.sv")
    rtl = pyhermes.RTL(enum_file)
    assert not rtl.has_error
    assert rtl.TEST is None
    assert rtl.B == 1
    assert rtl.test_pkg.F == 2
    assert rtl.pkg is None
    assert rtl.test_pkg.AA is None
    assert rtl["test_pkg"]["F"] == 2
    assert rtl["test_pkg"].ENUM_2[2] == "F"
    assert rtl.ENUM_1[1] == "B"
    assert len(rtl.ENUM_1) == 2


def test_enum_lookup():
    enum_file = get_vector("test_enum.sv")
    rtl = pyhermes.RTL(enum_file)
    assert not rtl.has_error

    name = rtl.lookup(1)
    # random values will be returned if a value is found
    assert name in {"B", "D"}
    name = rtl.lookup(1, "ENUM_2")
    assert name == "D"
    name = rtl.lookup(3, "test_pkg", "ENUM_2")
    assert name == "G"


def test_enum_flags():
    enum_file = get_vector("test_enum.sv")
    rtl = pyhermes.RTL(enum_file)
    assert not rtl.has_error

    pkg = rtl["test_pkg"]
    enum = pkg["FLAGS"]
    s = enum.flags(1 | 3 | 5)
    assert s == "FLAG_1 | FLAG_2 | FLAG_3"


if __name__ == "__main__":
    test_enum_flags()
