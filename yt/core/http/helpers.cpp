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
#include <util/string/cast.h>

namespace NYT {
namespace NHttp {

static const auto& Logger = HttpLogger;

using namespace NJson;
using namespace NYson;
using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const TString XYTErrorHeaderName("X-YT-Error");
static const TString XYTResponseCodeHeaderName("X-YT-Response-Code");
static const TString XYTResponseMessageHeaderName("X-YT-Response-Message");
static const TString AccessControlAllowCredentialsHeaderName("Access-Control-Allow-Credentials");
static const TString AccessControlAllowOriginHeaderName("Access-Control-Allow-Origin");
static const TString AccessControlAllowMethodsHeaderName("Access-Control-Allow-Methods");
static const TString AccessControlMaxAgeHeaderName("Access-Control-Max-Age");
static const TString AccessControlAllowHeadersHeaderName("Access-Control-Allow-Headers");
static const TString AccessControlExposeHeadersHeaderName("Access-Control-Expose-Headers");
static const TString XSourcePortYHeaderName("X-Source-Port-Y");
static const TString XForwardedForYHeaderName("X-Forwarded-For-Y");
static const TString ContentTypeHeaderName("Content-Type");
static const TString PragmaHeaderName("Pragma");
static const TString ExpiresHeaderName("Expires");
static const TString CacheControlHeaderName("Cache-Control");
static const TString XContentTypeOptionsHeaderName("X-Content-Type-Options");
static const TString XFrameOptionsHeaderName("X-Frame-Options");
static const TString XDnsPrefetchControlHeaderName("X-DNS-Prefetch-Control");
static const TString XYTTraceIdHeaderName("X-YT-Trace-Id");
static const TString XYTSpanIdHeaderName("X-YT-Span-Id");
static const TString XYTParentSpanIdHeaderName("X-YT-Parent-Span-Id");

////////////////////////////////////////////////////////////////////////////////

void FillYTError(const THeadersPtr& headers, const TError& error)
{
    TString errorJson;
    TStringOutput errorJsonOutput(errorJson);
    auto jsonWriter = CreateJsonConsumer(&errorJsonOutput);
    Serialize(error, jsonWriter.get());
    jsonWriter->Flush();

    headers->Add(XYTErrorHeaderName, errorJson);
    headers->Add(XYTResponseCodeHeaderName, ToString(static_cast<int>(error.GetCode())));
    headers->Add(XYTResponseMessageHeaderName, error.GetMessage());
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
    if (fromTrailers) {
        static const TString TrailerSource("trailer");
        source = TrailerSource;
        errorHeader = rsp->GetTrailers()->Find(XYTErrorHeaderName);
    } else {
        static const TString HeaderSource("header");
        source = HeaderSource;
        errorHeader = rsp->GetHeaders()->Find(XYTErrorHeaderName);
    }

    TString errorJson;
    if (errorHeader) {
        errorJson = *errorHeader;
    } else {
        static const TString BodySource("body");
        source = BodySource;
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
    explicit TErrorWrappingHttpHandler(IHttpHandlerPtr underlying)
        : Underlying_(std::move(underlying))
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
    const IHttpHandlerPtr Underlying_;
};

IHttpHandlerPtr WrapYTException(IHttpHandlerPtr underlying)
{
    return New<TErrorWrappingHttpHandler>(std::move(underlying));
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

bool MaybeHandleCors(const IRequestPtr& req, const IResponseWriterPtr& rsp, bool disableOriginCheck)
{
    auto origin = req->GetHeaders()->Find("Origin");
    if (origin) {
        auto url = ParseUrl(*origin);
        bool allow = disableOriginCheck || url.Host == "localhost" || url.Host.EndsWith(".yandex-team.ru");
        if (allow) {
            rsp->GetHeaders()->Add(AccessControlAllowCredentialsHeaderName, "true");
            rsp->GetHeaders()->Add(AccessControlAllowOriginHeaderName, *origin);
            rsp->GetHeaders()->Add(AccessControlAllowMethodsHeaderName, "POST, PUT, GET, OPTIONS");
            rsp->GetHeaders()->Add(AccessControlMaxAgeHeaderName, "3600");

            if (req->GetMethod() == EMethod::Options) {
                rsp->GetHeaders()->Add(AccessControlAllowHeadersHeaderName, HeadersWhitelist);
                rsp->SetStatus(EStatusCode::OK);
                WaitFor(rsp->Close())
                    .ThrowOnError();
                return true;
            } else {
                rsp->GetHeaders()->Add(AccessControlExposeHeadersHeaderName, HeadersWhitelist);
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
    const auto& headers = rsp->GetHeaders();

    headers->Set(PragmaHeaderName, "nocache");
    headers->Set(ExpiresHeaderName, "Thu, 01 Jan 1970 00:00:01 GMT");
    headers->Set(CacheControlHeaderName, "max-age=0, must-revalidate, proxy-revalidate, no-cache, no-store, private");
    headers->Set(XContentTypeOptionsHeaderName, "nosniff");
    headers->Set(XFrameOptionsHeaderName, "SAMEORIGIN");
    headers->Set(XDnsPrefetchControlHeaderName, "off");
}

TNullable<TString> GetBalancerRequestId(const IRequestPtr& req)
{
    static const TString XReqIdHeaderName("X-Req-Id");
    auto header = req->GetHeaders()->Find(XReqIdHeaderName);
    if (header) {
        return *header;
    }

    return {};
}

TNullable<TString> GetBalancerRealIP(const IRequestPtr& req)
{
    const auto& headers = req->GetHeaders();

    auto forwardedFor = headers->Find(XForwardedForYHeaderName);
    auto sourcePort = headers->Find(XSourcePortYHeaderName);

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
    rsp->GetHeaders()->Set(ContentTypeHeaderName, "application/json");

    TBufferOutput out;

    auto json = NJson::CreateJsonConsumer(&out);
    producer(json.get());
    json->Flush();

    TString body;
    out.Buffer().AsString(body);
    WaitFor(rsp->WriteBody(TSharedRef::FromString(body)))
        .ThrowOnError();
}

namespace {

template <class T, T NullValue>
T GetTracingId(const IRequestPtr& req, const TString& headerName)
{
    const auto& headers = req->GetHeaders();
    auto id = headers->Find(XYTTraceIdHeaderName);
    if (!id) {
        return NullValue;
    }
    return IntFromString<T, 16>(*id);
}

template <class T, T NullValue>
void SetTracingId(const IResponseWriterPtr& rsp, T id, const TString& headerName)
{
    if (id == NullValue) {
        return;
    }
    const auto& headers = rsp->GetHeaders();
    headers->Set(headerName, IntToString<16>(id));
}

} // namespace

NTracing::TTraceId GetTraceId(const IRequestPtr& req)
{
    return GetTracingId<NTracing::TTraceId, NTracing::InvalidTraceId>(req, XYTTraceIdHeaderName);
}

void SetTraceId(const IResponseWriterPtr& rsp, NTracing::TTraceId traceId)
{
    SetTracingId<NTracing::TTraceId, NTracing::InvalidTraceId>(rsp, traceId, XYTTraceIdHeaderName);
}

NTracing::TSpanId GetSpanId(const IRequestPtr& req)
{
    return GetTracingId<NTracing::TSpanId, NTracing::InvalidSpanId>(req, XYTSpanIdHeaderName);
}

void SetSpanId(const IResponseWriterPtr& rsp, NTracing::TSpanId spanId)
{
    SetTracingId<NTracing::TSpanId , NTracing::InvalidSpanId>(rsp, spanId, XYTSpanIdHeaderName);
}

NTracing::TSpanId GetParentSpanId(const IRequestPtr& req)
{
    return GetTracingId<NTracing::TSpanId, NTracing::InvalidSpanId>(req, XYTParentSpanIdHeaderName);
}

void SetParentSpanId(const IResponseWriterPtr& rsp, NTracing::TSpanId parentSpanId)
{
    SetTracingId<NTracing::TSpanId , NTracing::InvalidSpanId>(rsp, parentSpanId, XYTParentSpanIdHeaderName);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHttp
} // namespace NYT
