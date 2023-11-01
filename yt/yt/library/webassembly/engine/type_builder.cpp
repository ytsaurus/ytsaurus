#include <yt/yt/library/webassembly/api/type_builder.h>

namespace NYT::NWebAssembly {

////////////////////////////////////////////////////////////////////////////////

TWebAssemblyRuntimeType GetTypeId(bool, EWebAssemblyValueType, TRange<EWebAssemblyValueType>)
{
    return {}; // Unimplemented.
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NWebAssembly
