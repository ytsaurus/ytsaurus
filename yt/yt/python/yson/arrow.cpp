#include "arrow.h"

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

Y_WEAK Py::Object DumpParquet(Py::Tuple& /*args*/, Py::Dict& /*kwargs*/)
{
    throw Py::NotImplementedError("Implementation of DumpParquet was not found");
}

Y_WEAK Py::Object UploadParquet(Py::Tuple& /*args*/, Py::Dict& /*kwargs*/)
{
    throw Py::NotImplementedError("Implementation of UploadParquet was not found");
}

Y_WEAK void InitArrowIteratorType()
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
