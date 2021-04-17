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


if __name__ == "__main__":
    test_enum_parsing()
