#include "common.h"

namespace NYT {
namespace NPython {

///////////////////////////////////////////////////////////////////////////////

Py::Object ExtractArgument(Py::Tuple& args, Py::Dict& kwargs, const std::string& name)
{
    Py::Object result;
    if (kwargs.hasKey(name)) {
        result = kwargs[name];
        kwargs.delItem(name);
    } else {
        if (args.length() == 0) {
            throw Py::RuntimeError("Missing argument '" + name + "'");
        }
        result = args.front();
        args = args.getSlice(1, args.length());
    }
    return result;
}

bool HasArgument(Py::Tuple& args, Py::Dict& kwargs, const std::string& name)
{
    if (kwargs.hasKey(name)) {
        return true;
    } else {
        return args.length() > 0;
    }
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT
