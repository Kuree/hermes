enum logic [1:0] {
    A = 0,
    B = 1
} ENUM_1;

enum logic [2:0] {
    C = 0,
    D = 1
} ENUM_2;

package test_pkg;
enum int {
    A = 0,
    B = 1,
    C = 2
} ENUM_1;

enum int {
    D = 0,
    E = 1,
    F = 2,
    G = 3
} ENUM_2;

enum int {
    FLAG_1 = 1,
    FLAG_2 = 1 << 1,
    FLAG_3 = 1 << 2,
    FLAG_4 = 1 << 3
} FLAGS;

endpackage
