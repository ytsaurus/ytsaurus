#include "descriptor.h"

namespace NYT {
namespace NPython {

///////////////////////////////////////////////////////////////////////////////

TPythonCommandDescriptor::TPythonCommandDescriptor(Py::PythonClassInstance *self, Py::Tuple &args, Py::Dict &kwds)
    : Py::PythonClass<TPythonCommandDescriptor>::PythonClass(self, args, kwds)
{ }

void TPythonCommandDescriptor::SetDescriptor(const NDriver::TCommandDescriptor& descriptor)
{
    Descriptor_ = descriptor;
}

Py::Object TPythonCommandDescriptor::InputType(Py::Tuple& args, Py::Dict &kwds)
{
    return Py::String(Descriptor_.InputType.ToString());
}

Py::Object TPythonCommandDescriptor::OutputType(Py::Tuple& args, Py::Dict &kwds)
{
    return Py::String(Descriptor_.OutputType.ToString());
}
    
Py::Object TPythonCommandDescriptor::IsVolatile(Py::Tuple& args, Py::Dict &kwds)
{
    return Py::Boolean(Descriptor_.IsVolatile);
}

Py::Object TPythonCommandDescriptor::IsHeavy(Py::Tuple& args, Py::Dict &kwds)
{
    return Py::Boolean(Descriptor_.IsHeavy);
}

void TPythonCommandDescriptor::InitType()
{
    behaviors().name("CommandDescriptor");
    behaviors().doc("Describe command properties");
    behaviors().supportGetattro();
    behaviors().supportSetattro();

    PYCXX_ADD_KEYWORDS_METHOD(input_type, InputType, "Input type of the command");
    PYCXX_ADD_KEYWORDS_METHOD(output_type, OutputType, "Output type of the command");
    PYCXX_ADD_KEYWORDS_METHOD(is_volatile, IsVolatile, "Check that command is volatile");
    PYCXX_ADD_KEYWORDS_METHOD(is_heavy, IsHeavy, "Check that command is heavy");

    behaviors().readyType();
}


TPythonCommandDescriptor::~TPythonCommandDescriptor()
{ }

///////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT
