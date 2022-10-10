#include "yql_commands.h"
#include "config.h"

#include <yt/yt/client/api/config.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NDriver {

using namespace NConcurrency;
using namespace NApi;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TStartYqlQueryCommand::TStartYqlQueryCommand()
{
    RegisterParameter("query", Query);
}

void TStartYqlQueryCommand::DoExecute(NYT::NDriver::ICommandContextPtr context)
{
    auto client = context->GetClient();
    auto asyncResult = client->StartYqlQuery(Query, Options);

    auto result = WaitFor(asyncResult)
        .ValueOrThrow();

    context->ProduceOutputValue(BuildYsonStringFluently()
        .BeginMap()
            .OptionalItem("result", result.Result)
            .OptionalItem("plan", result.Plan)
            .OptionalItem("statistics", result.Statistics)
            .OptionalItem("task_info", result.TaskInfo)
        .EndMap());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
