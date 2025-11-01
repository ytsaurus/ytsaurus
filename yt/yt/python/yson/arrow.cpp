#include "arrow.h"

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

Y_WEAK Py::Object DumpParquet(Py::Tuple& /*args*/, Py::Dict& /*kwargs*/)
{
    throw Py::NotImplementedError("Implementation of DumpParquet was not found");
}

Y_WEAK Py::Object AsyncDumpParquet(Py::Tuple& /*args*/, Py::Dict& /*kwargs*/)
{
    throw Py::NotImplementedError("Implementation of AsyncDumpParquet was not found");
}

Y_WEAK Py::Object UploadParquet(Py::Tuple& /*args*/, Py::Dict& /*kwargs*/)
{
    throw Py::NotImplementedError("Implementation of UploadParquet was not found");
}

////////////////////////////////////////////////////////////////////////////////

Y_WEAK Py::Object DumpOrc(Py::Tuple& /*args*/, Py::Dict& /*kwargs*/)
{
    throw Py::NotImplementedError("Implementation of DumpOrc was not found");
}

Y_WEAK Py::Object AsyncDumpOrc(Py::Tuple& /*args*/, Py::Dict& /*kwargs*/)
{
    throw Py::NotImplementedError("Implementation of AsyncDumpOrc was not found");
}

Y_WEAK Py::Object UploadOrc(Py::Tuple& /*args*/, Py::Dict& /*kwargs*/)
{
    throw Py::NotImplementedError("Implementation of UploadOrc was not found");
}

////////////////////////////////////////////////////////////////////////////////

Y_WEAK void InitArrowIteratorType()
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
