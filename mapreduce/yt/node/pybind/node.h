#pragma once

#include <mapreduce/yt/node/node.h>

#include <Python.h>

namespace NYT {
    PyObject* BuildPyObject(const TNode& val);
}
