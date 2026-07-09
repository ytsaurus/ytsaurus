#include "context.h"

namespace NYT::NFlow::NTables {

////////////////////////////////////////////////////////////////////////////////

TContextPtr TContext::WithTableName(TStringBuf name) const
{
    auto namedContext = New<TContext>();
    namedContext->Client = Client;
    namedContext->PipelinePath = PipelinePath;
    namedContext->LoadThroughputThrottler = LoadThroughputThrottler;
    namedContext->Logger = Logger.WithTag("Table: %v", name);
    namedContext->Profiler = Profiler.WithTag("table", std::string(name));
    namedContext->Tag = Tag;
    return namedContext;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTables
