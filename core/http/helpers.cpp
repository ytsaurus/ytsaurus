#include "helpers.h"

#include "http.h"
#include "private.h"

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/yson/consumer.h>

#include <yt/core/json/json_writer.h>
#include <yt/core/json/json_parser.h>
#include <yt/core/json/config.h>

#include <util/stream/buffer.h>

#include <util/generic/buffer.h>

#include <util/string/strip.h>
#include <util/string/join.h>

namespace NYT {
namespace NHttp {

static const auto& Logger = HttpLogger;

using namespace NJson;
using namespace NYson;
using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

void FillYTError(const THeadersPtr& headers, const TError& error)
{
    TString errorJson;
    TStringOutput errorJsonOutput(errorJson);
    auto jsonWriter = CreateJsonConsumer(&errorJsonOutput);
    Serialize(error, jsonWriter.get());
    jsonWriter->Flush();

    headers->Add("X-YT-Error", errorJson);
    headers->Add("X-YT-Response-Code",
        ToString(static_cast<i64>(error.GetCode())));
    headers->Add("X-YT-Response-Message", error.GetMessage());
}

void FillYTErrorHeaders(const IResponseWriterPtr& rsp, const TError& error)
{
    FillYTError(rsp->GetHeaders(), error);
}

void FillYTErrorTrailers(const IResponseWriterPtr& rsp, const TError& error)
{
    FillYTError(rsp->GetTrailers(), error);
}

TError ParseYTError(const IResponsePtr& rsp, bool fromTrailers)
{
    TString source;

    const TString* errorHeader;
    if (!fromTrailers) {
        source = "header";
        errorHeader = rsp->GetHeaders()->Find("X-YT-Error");
    } else {
        source = "trailer";
        errorHeader = rsp->GetTrailers()->Find("X-YT-Error");
    }

    TString errorJson;
    if (!errorHeader) {
        source = "body";
        errorJson = ToString(rsp->ReadBody());
    } else {
        errorJson = *errorHeader;
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

static const auto HeadersWhitelist = JoinSeq(", ", std::vector<TString>{
    "Authorization",
    "Origin",
    "Content-Type",
    "Accept",
    "X-Csrf-Token",
    "X-YT-Parameters",
    "X-YT-Parameters0",
    "X-YT-Parameters-0",
    "X-YT-Parameters1",
    "X-YT-Parameters-1",
    "X-YT-Input-Format",
    "X-YT-Input-Format0",
    "X-YT-Input-Format-0",
    "X-YT-Output-Format",
    "X-YT-Output-Format0",
    "X-YT-Output-Format-0",
    "X-YT-Header-Format",
    "X-YT-Suppress-Redirect",
    "X-YT-Omit-Trailers",
});

bool MaybeHandleCors(const IRequestPtr& req, const IResponseWriterPtr& rsp)
{
    auto origin = req->GetHeaders()->Find("Origin");
    if (origin) {
        auto url = ParseUrl(*origin);
        bool allow = url.Host == "localhost" || url.Host.EndsWith(".yandex-team.ru");
        if (allow) {
            rsp->GetHeaders()->Add("Access-Control-Allow-Credentials", "true");
            rsp->GetHeaders()->Add("Access-Control-Allow-Origin", *origin);
            rsp->GetHeaders()->Add("Access-Control-Allow-Methods", "POST, PUT, GET, OPTIONS");
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

THashMap<TString, TString> ParseCookies(TStringBuf cookies)
{
    THashMap<TString, TString> map;
    size_t index = 0;
    while (index < cookies.size()) {
        auto nameStartIndex = index;
        auto nameEndIndex = cookies.find('=', index);
        if (nameEndIndex == TString::npos) {
            THROW_ERROR_EXCEPTION("Malformed cookies");
        }
        auto name = StripString(cookies.substr(nameStartIndex, nameEndIndex - nameStartIndex));

        auto valueStartIndex = nameEndIndex + 1;
        auto valueEndIndex = cookies.find(';', valueStartIndex);
        if (valueEndIndex == TString::npos) {
            valueEndIndex = cookies.size();
        }
        auto value = StripString(cookies.substr(valueStartIndex, valueEndIndex - valueStartIndex));

        map.emplace(TString(name), TString(value));

        index = valueEndIndex + 1;
    }
    return map;
}

void ProtectCsrfToken(const IResponseWriterPtr& rsp)
{
    auto headers = rsp->GetHeaders();

    headers->Set("Pragma", "nocache");
    headers->Set("Expires", "Thu, 01 Jan 1970 00:00:01 GMT");
    headers->Set("Cache-Control", "max-age=0, must-revalidate, proxy-revalidate, no-cache, no-store, private");
    headers->Set("X-Content-Type-Options", "nosniff");
    headers->Set("X-Frame-Options", "SAMEORIGIN");
    headers->Set("X-DNS-Prefetch-Control", "off");
}

TNullable<TString> GetBalancerRequestId(const IRequestPtr& req)
{
    auto header = req->GetHeaders()->Find("X-Req-Id");
    if (header) {
        return *header;
    }

    return {};
}

TNullable<TString> GetBalancerRealIP(const IRequestPtr& req)
{
    auto headers = req->GetHeaders();

    auto forwardedFor = headers->Find("X-Forwarded-For-Y");
    auto sourcePort = headers->Find("X-Source-Port-Y");

    if (forwardedFor && sourcePort) {
        return Format("[%v]:%v", *forwardedFor, *sourcePort);
    }

    return {};
}

TNullable<TString> GetUserAgent(const IRequestPtr& req)
{
    auto headers = req->GetHeaders();
    auto userAgent = headers->Find("User-Agent");
    if (userAgent) {
        return *userAgent;
    }
    return {};
}

void ReplyJson(const IResponseWriterPtr& rsp, std::function<void(NYson::IYsonConsumer*)> producer)
{
    rsp->GetHeaders()->Set("Content-Type", "application/json");

    TBufferOutput out;

    auto json = NJson::CreateJsonConsumer(&out);
    producer(json.get());
    json->Flush();

    TString body;
    out.Buffer().AsString(body);
    WaitFor(rsp->WriteBody(TSharedRef::FromString(body)))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHttp
} // namespace NYT
