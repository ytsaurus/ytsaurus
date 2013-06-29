#pragma once

#include <contrib/libs/pycxx/Extensions.hxx>

#include <ytlib/driver/driver.h>

namespace NYT {
namespace NPython {

///////////////////////////////////////////////////////////////////////////////

class TPythonCommandDescriptor
    : public Py::PythonClass<TPythonCommandDescriptor>
{
public:
    TPythonCommandDescriptor(Py::PythonClassInstance *self, Py::Tuple &args, Py::Dict &kwds);

    void SetDescriptor(const NDriver::TCommandDescriptor& descriptor);
    
    Py::Object InputType(Py::Tuple& args, Py::Dict &kwds);
    PYCXX_KEYWORDS_METHOD_DECL(TPythonCommandDescriptor, InputType);
    
    Py::Object OutputType(Py::Tuple& args, Py::Dict &kwds);
    PYCXX_KEYWORDS_METHOD_DECL(TPythonCommandDescriptor, OutputType);

    Py::Object IsVolatile(Py::Tuple& args, Py::Dict &kwds);
    PYCXX_KEYWORDS_METHOD_DECL(TPythonCommandDescriptor, IsVolatile);
    
    Py::Object IsHeavy(Py::Tuple& args, Py::Dict &kwds);
    PYCXX_KEYWORDS_METHOD_DECL(TPythonCommandDescriptor, IsHeavy);
    
    virtual ~TPythonCommandDescriptor();
    
    static void InitType();

private:
    NDriver::TCommandDescriptor Descriptor_;
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT
