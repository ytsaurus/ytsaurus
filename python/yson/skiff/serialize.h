#pragma once

#include "public.h"

#include <yt/python/common/helpers.h>
#include <yt/python/common/stream.h>

#include <yt/core/skiff/skiff_schema.h>

#include <Extensions.hxx> // pycxx
#include <Objects.hxx> // pycxx

#include <util/generic/string.h>
#include <util/generic/hash.h>

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

Py::Object DumpSkiff(Py::Tuple& args, Py::Dict& kwargs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
