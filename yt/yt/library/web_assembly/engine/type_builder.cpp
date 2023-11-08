#include <yt/yt/library/web_assembly/api/type_builder.h>

namespace NYT::NWebAssembly {

////////////////////////////////////////////////////////////////////////////////

TWebAssemblyRuntimeType GetTypeId(bool /*intrinsic*/, EWebAssemblyValueType /*returnType*/, TRange<EWebAssemblyValueType> /*arguments*/)
{
    return {}; // Unimplemented.
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NWebAssembly
