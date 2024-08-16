#ifndef QUERY_CONTEXT_INL_H_
#error "Direct inclusion of this file is not allowed, include format.h"
// For the sake of sane code completion.
#include "query_context.h"
#endif

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

template <class T>
void TQueryContext::SetRuntimeVariable(const TString& key, const T& value)
{
    auto guard = Guard(QueryLogLock_);
    YT_VERIFY(RuntimeVariables_);
    if (RuntimeVariables_->Contains(key)) {
        YT_LOG_DEBUG(
            "Overwriting existing runtime variable in query context (Key: %v, OldValue: %v, NewValue: %v)",
            key,
            NYson::ConvertToYsonString(RuntimeVariables_->GetYson(key), NYson::EYsonFormat::Text),
            NYson::ConvertToYsonString(value, NYson::EYsonFormat::Text));
    }
    RuntimeVariables_->Set(key, value);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
