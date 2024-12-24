#include "service_id.h"

#include <util/system/type_name.h>

namespace NYT::NFusion {

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, TServiceId id, TStringBuf /*spec*/)
{
    builder->AppendString(CppDemangle(TString(id.Underlying().name())));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFusion
