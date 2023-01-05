#pragma once

#include "public.h"

#include <CXX/Extensions.hxx> // pycxx
#include <CXX/Objects.hxx> // pycxx

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

Py::Object DumpSkiff(Py::Tuple& args, Py::Dict& kwargs);
Py::Object DumpSkiffStructured(Py::Tuple& args, Py::Dict& kwargs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
