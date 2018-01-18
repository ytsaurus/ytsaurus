#include "http.h"

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/http/http.h>
#include <yt/core/http/private.h>

#include <yt/core/yson/consumer.h>

#include <yt/ytlib/formats/json_writer.h>
#include <yt/ytlib/formats/config.h>

namespace NYT {
namespace NHttp {

static const auto& Logger = HttpLogger;

using namespace NFormats;
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NHttp
} // namespace NYT
