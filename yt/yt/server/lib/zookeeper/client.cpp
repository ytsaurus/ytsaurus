#include "client.h"

#include "private.h"
#include "session.h"

#include <yt/yt/client/api/client.h>

namespace NYT::NZookeeper {

using namespace NConcurrency;
using namespace NYTree;

const static auto& Logger = ZookeeperLogger;

////////////////////////////////////////////////////////////////////////////////

class TClient
    : public IClient
{
public:
    explicit TClient(NApi::IClientPtr ytClient)
        : YtClient_(std::move(ytClient))
    { }

    TRspPingPtr Ping(TRequestContext<TReqPingPtr> context) override
    {
        const auto& session = context.Session;
        YT_LOG_DEBUG("Pinging session (RequestId: %v, SessionId: %v)",
            context.RequestId,
            session->GetId());

        auto rsp = New<TRspPing>();
        return rsp;
    }

    TRspGetChildren2Ptr GetChildren2(TRequestContext<TReqGetChildren2Ptr> context) override
    {
        const auto& request = context.Request;

        YT_LOG_DEBUG("Getting list of children (RequestId: %v, SessionId: %v, Path: %v)",
            context.RequestId,
            context.Session->GetId(),
            request->Path);

        auto listResult = WaitFor(YtClient_->ListNode(FromZookeeperPath(request->Path)))
            .ValueOrThrow();
        auto children = ConvertTo<std::vector<TString>>(std::move(listResult));

        auto rsp = New<TRspGetChildren2>();
        rsp->Children = std::move(children);

        return rsp;
    }

private:
    const NApi::IClientPtr YtClient_;

    static TYPath FromZookeeperPath(const TString& path)
    {
        if (path == "/") {
            return path;
        } else {
            return "/" + path;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IClientPtr CreateClient(NApi::IClientPtr ytClient)
{
    return New<TClient>(std::move(ytClient));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NZookeeper
