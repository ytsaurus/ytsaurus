#include <library/cpp/pybind/example/v2_example.h>

#define PY_SSIZE_T_CLEAN
#include <Python.h>

PyMODINIT_FUNC initpybindexample() {
    DoInitExample();
}
