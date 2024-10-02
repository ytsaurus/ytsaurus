#include "profiling.h"

#include "private.h"

namespace NYT::NOrm::NServer::NApi {

////////////////////////////////////////////////////////////////////////////////

TObjectServiceSensorsHolder::TObjectServiceSensorsHolder()
    : Profiler_(Profiler.WithPrefix("/object_service"))
{ }

TObjectServiceSensorsHolder::TMethodSensors* TObjectServiceSensorsHolder::FindOrCreateSelectObjectHistorySensors(
    const TString& methodName,
    const NRpc::TAuthenticationIdentity& identity,
    bool indexUsed)
{
    TMethodSensors* methodSensors = SelectObjectHistorySensorsMap_.FindOrInsert({identity, indexUsed}, [&] {
        auto profiler = Profiler_
            .WithRequiredTag("method", methodName)
            .WithTag("user", identity.User)
            .WithTag("user_tag", identity.UserTag)
            .WithTag("index_used", std::to_string(indexUsed))
            .WithSparse();

        return TMethodSensors{
            .TotalTimeCounter = profiler.Timer("/total_time"),
        };
    }).first;

    return methodSensors;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NApi
