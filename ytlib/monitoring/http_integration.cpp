#include "http_integration.h"

#include <yt/core/json/config.h>
#include <yt/core/json/json_writer.h>

#include <yt/core/misc/url.h>

#include <yt/core/yson/parser.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/ytree/helpers.h>
#include <yt/core/ytree/virtual.h>
#include <yt/core/ytree/ypath_detail.h>
#include <yt/core/ytree/ypath_proxy.h>

#include <yt/core/http/http.h>
#include <yt/core/http/helpers.h>

#include <util/string/vector.h>
#include <util/string/cgiparam.h>

namespace NYT {
namespace NMonitoring {

using namespace NYTree;
using namespace NYson;
using namespace NHttp;
using namespace NConcurrency;
using namespace NJson;

////////////////////////////////////////////////////////////////////////////////

class TYPathHttpHandler
    : public IHttpHandler
{
public:
    TYPathHttpHandler(const IYPathServicePtr& service)
        : Service_(service)
    { }

    virtual void HandleRequest(
        const IRequestPtr& req,
        const IResponseWriterPtr& rsp) override
    {
        TString path{req->GetUrl().Path};
        YCHECK(path.size() >= STRINGBUF("/orchid").size());
        path = path.substr(STRINGBUF("/orchid").size(), TString::npos);

        TCgiParameters params(req->GetUrl().RawQuery);

        auto ypathReq = TYPathProxy::Get(path);
        if (params.size() != 0) {
            auto options = CreateEphemeralAttributes();
            for (const auto& param : params) {
                // Just a check, IAttributeDictionary takes raw YSON anyway.
                try {
                    TYsonString(param.second).Validate();
                } catch (const std::exception& ex) {
                    THROW_ERROR_EXCEPTION("Error parsing value of query parameter %Qv", param.first)
                        << ex;
                }

                options->SetYson(param.first, TYsonString(param.second));
                ToProto(ypathReq->mutable_options(), *options);
            }
        }

        auto ypathRsp = WaitFor(ExecuteVerb(Service_, ypathReq))
            .ValueOrThrow();

        rsp->SetStatus(EStatusCode::Ok);

        auto syncOutput = CreateBufferedSyncAdapter(rsp);
        auto writer = CreateJsonConsumer(syncOutput.get());

        Serialize(TYsonString(ypathRsp->value()), writer.get());
        
        writer->Flush();
        syncOutput->Flush();

        WaitFor(rsp->Close())
            .ThrowOnError();
    }    

private:
    IYPathServicePtr Service_;
};

IHttpHandlerPtr GetOrchidYPathHttpHandler(const IYPathServicePtr& service)
{
    return WrapYTException(New<TYPathHttpHandler>(service));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMonitoring
} // namespace NYT
