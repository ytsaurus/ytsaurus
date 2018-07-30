#pragma once

#include "public.h"

#include <yt/core/misc/ref_counted.h>

#include <Extensions.hxx> // pycxx
#include <Objects.hxx> // pycxx

#include <util/generic/string.h>
#include <util/generic/hash.h>

namespace NYT {
namespace NPython {

////////////////////////////////////////////////////////////////////////////////

class TSkiffTableSwitchPython
    : public Py::PythonClass<TSkiffTableSwitchPython>
{
public:
    TSkiffTableSwitchPython(Py::PythonClassInstance* self, Py::Tuple& args, Py::Dict& kwargs);

    virtual ~TSkiffTableSwitchPython();

    ui16 GetTableIndex();

    static void InitType();

private:
    ui16 TableIndex_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT
