#include "switch.h"

#include <yt/python/common/helpers.h>

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

TSkiffTableSwitchPython::TSkiffTableSwitchPython(Py::PythonClassInstance* self, Py::Tuple& args, Py::Dict& kwargs)
    : Py::PythonClass<TSkiffTableSwitchPython>::PythonClass(self, args, kwargs)
{
    auto tableIndexArg = ExtractArgument(args, kwargs, "table_index");
    ValidateArgumentsEmpty(args, kwargs);

    auto tableIndex = ConvertToLongLong(tableIndexArg);
    if (tableIndex < 0 or tableIndex > std::numeric_limits<ui16>::max()) {
        THROW_ERROR_EXCEPTION("Invalid table index, it must fit into ui16")
            << TErrorAttribute("table_index", tableIndex);
    }
    TableIndex_ = tableIndex;
}

TSkiffTableSwitchPython::~TSkiffTableSwitchPython() = default;

ui16 TSkiffTableSwitchPython::GetTableIndex()
{
    return TableIndex_;
}

void TSkiffTableSwitchPython::InitType()
{
    behaviors().name("SkiffTableSwitcher");
    behaviors().doc("Skiff table switcher");

    behaviors().supportGetattro();
    behaviors().supportSetattro();

    behaviors().readyType();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
