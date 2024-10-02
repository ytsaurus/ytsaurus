#include "object_filter.h"

namespace NYT::NOrm::NClient::NObjects {

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TObjectFilter& filter, TStringBuf spec)
{
    FormatValue(builder, Format("{Query: %v}", filter.Query), spec);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NObjects
