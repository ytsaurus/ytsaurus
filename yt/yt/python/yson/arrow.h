#pragma once

#include <util/system/compiler.h>

#include <CXX/Extensions.hxx> // pycxx
#include <CXX/Objects.hxx> // pycxx

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

Py::Object DumpParquet(Py::Tuple& args, Py::Dict& kwargs);

Py::Object AsyncDumpParquet(Py::Tuple& args, Py::Dict& kwargs);

Py::Object UploadParquet(Py::Tuple& args, Py::Dict& kwargs);

////////////////////////////////////////////////////////////////////////////////

Py::Object DumpOrc(Py::Tuple& args, Py::Dict& kwargs);

Py::Object AsyncDumpOrc(Py::Tuple& args, Py::Dict& kwargs);

Py::Object UploadOrc(Py::Tuple& args, Py::Dict& kwargs);

////////////////////////////////////////////////////////////////////////////////

void InitArrowIteratorType();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython

