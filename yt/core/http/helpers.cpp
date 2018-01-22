#include "helpers.h"

#include "http.h"
#include "private.h"

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/yson/consumer.h>

#include <yt/core/json/json_writer.h>
#include <yt/core/json/config.h>

namespace NYT {
namespace NHttp {

static const auto& Logger = HttpLogger;

using namespace NJson;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

void FillYTErrorHeaders(const IResponseWriterPtr& rsp, const TError& error)
{
    TString errorJson;
    TStringOutput errorJsonOutput(errorJson);
    auto jsonWriter = CreateJsonConsumer(&errorJsonOutput);
    Serialize(error, jsonWriter.get());
    jsonWriter->Flush();
            
    rsp->GetHeaders()->Add("X-YT-Error", errorJson);
    rsp->GetHeaders()->Add("X-YT-Response-Code",
        ToString(static_cast<i64>(error.GetCode())));
    rsp->GetHeaders()->Add("X-YT-Response-Message", error.GetMessage());
}

class TErrorWrappingHttpHandler
    : public virtual IHttpHandler
{
public:
    TErrorWrappingHttpHandler(const IHttpHandlerPtr& underlying)
        : Underlying_(underlying)
    { }

    virtual void HandleRequest(
        const IRequestPtr& req,
        const IResponseWriterPtr& rsp) override
    {
        try {
            Underlying_->HandleRequest(req, rsp);
        } catch(const std::exception& ex) {
            LOG_ERROR(ex, "Error in %v", req->GetUrl().Path);

            TError error(ex);
            FillYTErrorHeaders(rsp, error);
            rsp->WriteHeaders(EStatusCode::InternalServerError);

            WaitFor(rsp->Close())
                .ThrowOnError();
        }
    }

private:
    IHttpHandlerPtr Underlying_;
};

IHttpHandlerPtr WrapYTException(const IHttpHandlerPtr& underlying)
{
    return New<TErrorWrappingHttpHandler>(underlying);
}

bool MaybeHandleCors(const IRequestPtr& req, const IResponseWriterPtr& rsp)
{
    auto headersWhitelist = "Content-Type, Accept, X-YT-Error, X-YT-Response-Code, X-YT-Response-Message";

    auto origin = req->GetHeaders()->Find("Origin");
    if (origin) {
        auto url = ParseUrl(*origin);
        bool allow = url.Host == "localhost"
            || url.Host.EndsWith(".yandex.net")
            || url.Host.EndsWith(".yandex-team.ru");

        if (allow) {
            rsp->GetHeaders()->Add("Access-Control-Allow-Origin", *origin);
            rsp->GetHeaders()->Add("Access-Control-Allow-Methods", "POST, OPTIONS");
            rsp->GetHeaders()->Add("Access-Control-Max-Age", "3600");

            if (req->GetMethod() == EMethod::Options) {
                rsp->GetHeaders()->Add("Access-Control-Allow-Headers", headersWhitelist);
                rsp->WriteHeaders(EStatusCode::Ok);
                WaitFor(rsp->Close())
                    .ThrowOnError();
                return true;
            } else {
                rsp->GetHeaders()->Add("Access-Control-Expose-Headers", headersWhitelist);
            }
        }
    }

    return false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHttp
} // namespace NYT
