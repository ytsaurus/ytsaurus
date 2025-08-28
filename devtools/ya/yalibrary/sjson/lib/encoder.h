#pragma once

#include <Python.h>

#include <util/generic/yexception.h>

namespace NSJson {
    void Encode(PyObject* obj, PyObject* stream);
}
