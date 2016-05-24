#include "descriptor.h"

namespace NYT {
namespace NPython {

///////////////////////////////////////////////////////////////////////////////

TCommandDescriptor::TCommandDescriptor(Py::PythonClassInstance *self, Py::Tuple& args, Py::Dict& kwargs)
    : Py::PythonClass<TCommandDescriptor>::PythonClass(self, args, kwargs)
{ }

void TCommandDescriptor::SetDescriptor(const NDriver::TCommandDescriptor& descriptor)
{
    Descriptor_ = descriptor;
}

Py::Object TCommandDescriptor::InputType(Py::Tuple& args, Py::Dict& kwargs)
{
    return Py::String(ToString(Descriptor_.InputType));
}

Py::Object TCommandDescriptor::OutputType(Py::Tuple& args, Py::Dict& kwargs)
{
    return Py::String(ToString(Descriptor_.OutputType));
}
    
Py::Object TCommandDescriptor::IsVolatile(Py::Tuple& args, Py::Dict& kwargs)
{
    return Py::Boolean(Descriptor_.IsVolatile);
}

Py::Object TCommandDescriptor::IsHeavy(Py::Tuple& args, Py::Dict& kwargs)
{
    return Py::Boolean(Descriptor_.IsHeavy);
}

void TCommandDescriptor::InitType()
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


TCommandDescriptor::~TCommandDescriptor()
{ }

///////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT
