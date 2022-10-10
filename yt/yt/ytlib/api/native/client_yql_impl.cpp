#include "client_impl.h"

#include <yt/yt/ytlib/yql_client/yql_service_proxy.h>

namespace NYT::NApi::NNative {

using namespace NYqlClient;
using namespace NConcurrency;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TStartYqlQueryResult TClient::DoStartYqlQuery(const TString& query, const TStartYqlQueryOptions& /*options*/)
{
    TYqlServiceProxy proxy(Connection_->GetYqlAgentChannelOrThrow());

    auto req = proxy.StartQuery();
    auto* yqlRequest = req->mutable_yql_request();
    yqlRequest->set_query(query);

    auto rsp = WaitFor(req->Invoke())
        .ValueOrThrow();
    if (rsp->has_error()) {
        const auto& error = FromProto<TError>(rsp->error());
        THROW_ERROR error;
    } else if (rsp->has_yql_response()) {
        const auto& yqlResponse = rsp->yql_response();

        TStartYqlQueryResult result;
        if (yqlResponse.has_result()) {
            result.Result = TYsonString(yqlResponse.result());
        }
        if (yqlResponse.has_plan()) {
            result.Plan = TYsonString(yqlResponse.plan());
        }
        if (yqlResponse.has_statistics()) {
            result.Statistics = TYsonString(yqlResponse.statistics());
        }
        if (yqlResponse.has_task_info()) {
            result.TaskInfo = TYsonString(yqlResponse.task_info());
        }
        return result;
    } else {
        THROW_ERROR_EXCEPTION("Malformed YQL agent response");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
