#include "contrib/python/py3c/py3c.h"

#include <library/cpp/pybind/v2.h>

#include <Python.h>

void ExportState();

MODULE_INIT_FUNC(bindings) 
{
    ::NPyBind::TPyModuleDefinition::InitModule("bindings");
    ExportState();
    return ::NPyBind::TPyModuleDefinition::GetModule().M.RefGet();
}
