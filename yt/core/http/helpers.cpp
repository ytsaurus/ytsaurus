#include "helpers.h"

#include "http.h"
#include "private.h"

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/yson/consumer.h>

#include <yt/core/json/json_writer.h>
#include <yt/core/json/json_parser.h>
#include <yt/core/json/config.h>

namespace NYT {
namespace NHttp {

static const auto& Logger = HttpLogger;

using namespace NJson;
using namespace NYson;
using namespace NYTree;
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

TError ParseYTError(const IResponsePtr& rsp)
{
    TString errorJson;
    TString source;
    auto* errorHeader = rsp->GetHeaders()->Find("X-YT-Error");
    if (errorHeader) {
        source = "header";
        errorJson = *errorHeader;
    } else {
        source = "body";
        errorJson = ToString(rsp->ReadBody());
    }

    TStringInput errorJsonInput(errorJson);
    std::unique_ptr<IBuildingYsonConsumer<TError>> buildingConsumer;
    CreateBuildingYsonConsumer(&buildingConsumer, EYsonType::Node);
    try {
        ParseJson(&errorJsonInput, buildingConsumer.get());
    } catch (const TErrorException& ex) {
        return TError("Failed to parse error from response")
            << TErrorAttribute("source", source)
            << ex;
    }
    return buildingConsumer->Finish();
}

class TErrorWrappingHttpHandler
    : public virtual IHttpHandler
{
public:
    explicit TErrorWrappingHttpHandler(const IHttpHandlerPtr& underlying)
        : Underlying_(underlying)
    { }

    virtual void HandleRequest(
        const IRequestPtr& req,
        const IResponseWriterPtr& rsp) override
    {
        try {
            Underlying_->HandleRequest(req, rsp);
        } catch(const std::exception& ex) {
            TError error(ex);

            LOG_DEBUG(error, "Error handling HTTP request (Path: %v)",
                req->GetUrl().Path);

            FillYTErrorHeaders(rsp, error);
            rsp->SetStatus(EStatusCode::InternalServerError);

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
    static const auto HeadersWhitelist = "Content-Type, Accept, X-YT-Error, X-YT-Response-Code, X-YT-Response-Message";

    auto origin = req->GetHeaders()->Find("Origin");
    if (origin) {
        auto url = ParseUrl(*origin);
        bool allow = url.Host == "localhost" || url.Host.EndsWith(".yandex-team.ru");
        if (allow) {
            rsp->GetHeaders()->Add("Access-Control-Allow-Origin", *origin);
            rsp->GetHeaders()->Add("Access-Control-Allow-Methods", "POST, OPTIONS");
            rsp->GetHeaders()->Add("Access-Control-Max-Age", "3600");

            if (req->GetMethod() == EMethod::Options) {
                rsp->GetHeaders()->Add("Access-Control-Allow-Headers", HeadersWhitelist);
                rsp->SetStatus(EStatusCode::OK);
                WaitFor(rsp->Close())
                    .ThrowOnError();
                return true;
            } else {
                rsp->GetHeaders()->Add("Access-Control-Expose-Headers", HeadersWhitelist);
            }
        }
    }

    return false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHttp
} // namespace NYT
