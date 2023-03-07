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
#include <util/string/split.h>

namespace NYT::NHttp {

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
static const TString ContentRangeHeaderName("Content-Range");
static const TString PragmaHeaderName("Pragma");
static const TString RangeHeaderName("Range");
static const TString ExpiresHeaderName("Expires");
static const TString CacheControlHeaderName("Cache-Control");
static const TString XContentTypeOptionsHeaderName("X-Content-Type-Options");
static const TString XFrameOptionsHeaderName("X-Frame-Options");
static const TString XDnsPrefetchControlHeaderName("X-DNS-Prefetch-Control");
static const TString XYTTraceIdHeaderName("X-YT-Trace-Id");
static const TString XYTSpanIdHeaderName("X-YT-Span-Id");

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
    headers->Add(XYTResponseMessageHeaderName, EscapeHeaderValue(error.GetMessage()));
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
        errorJson = ToString(rsp->ReadAll());
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

            YT_LOG_DEBUG(error, "Error handling HTTP request (Path: %v)",
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
    "Cache-Control",
    "X-Csrf-Token",
    "X-YT-Parameters",
    "X-YT-Parameters0",
    "X-YT-Parameters-0",
    "X-YT-Parameters1",
    "X-YT-Parameters-1",
    "X-YT-Response-Parameters",
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
        bool allow = disableOriginCheck ||
            url.Host == "localhost" ||
            url.Host.EndsWith(".yandex-team.ru");
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

std::optional<TString> GetBalancerRequestId(const IRequestPtr& req)
{
    static const TString XReqIdHeaderName("X-Req-Id");
    auto header = req->GetHeaders()->Find(XReqIdHeaderName);
    if (header) {
        return *header;
    }

    return {};
}

std::optional<TString> GetBalancerRealIP(const IRequestPtr& req)
{
    const auto& headers = req->GetHeaders();

    auto forwardedFor = headers->Find(XForwardedForYHeaderName);
    auto sourcePort = headers->Find(XSourcePortYHeaderName);

    if (forwardedFor && sourcePort) {
        return Format("[%v]:%v", *forwardedFor, *sourcePort);
    }

    return {};
}

std::optional<TString> GetUserAgent(const IRequestPtr& req)
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

NTracing::TTraceId GetTraceId(const IRequestPtr& req)
{
    const auto& headers = req->GetHeaders();
    auto id = headers->Find(XYTTraceIdHeaderName);
    if (!id) {
        return NTracing::InvalidTraceId;
    }

    NTracing::TTraceId traceId;
    if (!NTracing::TTraceId::FromString(*id, &traceId)) {
        return NTracing::InvalidTraceId;
    }
    return traceId;
}

void SetTraceId(const IResponseWriterPtr& rsp, NTracing::TTraceId traceId)
{
    if (traceId != NTracing::InvalidTraceId) {
        rsp->GetHeaders()->Set(XYTTraceIdHeaderName, ToString(traceId));
    }
}

NTracing::TSpanId GetSpanId(const IRequestPtr& req)
{
    const auto& headers = req->GetHeaders();
    auto id = headers->Find(XYTSpanIdHeaderName);
    if (!id) {
        return NTracing::InvalidSpanId;
    }
    return IntFromString<NTracing::TSpanId, 16>(*id);
}

bool TryParseTraceParent(const TString& traceParent, NTracing::TSpanContext& spanContext)
{
    // An adaptation of https://github.com/census-instrumentation/opencensus-go/blob/ae11cd04b/plugin/ochttp/propagation/tracecontext/propagation.go#L49-L106

    auto parts = StringSplitter(traceParent).Split('-').ToList<TString>();
    if (parts.size() < 3 || parts.size() > 4) {
        return false;
    }

    // NB: we support three-part form in which version is assumed to be zero.
    ui8 version = 0;
    if (parts.size() == 4) {
        if (parts[0].size() != 2) {
            return false;
        }
        if (!TryIntFromString<10>(parts[0], version)) {
            return false;
        }
        parts.erase(parts.begin());
    }

    // Now we have exactly three parts: traceId-spanId-options.

    // Parse trace context. Hex string 11111111222222223333333344444444
    // is interpreted as YT GUID 11111111-22222222-33333333-44444444.
    if (!TGuid::FromStringHex32(parts[0], &spanContext.TraceId)) {
        return false;
    }

    if (parts[1].size() != 16) {
        return false;
    }
    if (!TryIntFromString<16>(parts[1], spanContext.SpanId)) {
        return false;
    }

    ui8 options = 0;
    if (!TryIntFromString<16>(parts[2], options)) {
        return false;
    }
    spanContext.Sampled = static_cast<bool>(options & 1u);
    spanContext.Debug = static_cast<bool>(options & 2u);

    return true;
}

NTracing::TTraceContextPtr GetOrCreateTraceContext(const IRequestPtr& req)
{
    const auto& headers = req->GetHeaders();
    NTracing::TTraceContextPtr trace;
    if (auto* traceParent = headers->Find("traceparent")) {
        NTracing::TSpanContext parentSpan;
        if (TryParseTraceParent(*traceParent, parentSpan)) {
            trace = New<NTracing::TTraceContext>(parentSpan, "HttpServer");
        }
    }
    if (!trace) {
        // Generate new trace context from scratch.
        trace = NTracing::CreateRootTraceContext("HttpServer");
    }

    trace->AddTag("path", TString(req->GetUrl().Path));
    return trace;
}

std::optional<std::pair<int64_t, int64_t>> GetRange(const THeadersPtr& headers)
{
    auto range = headers->Find(RangeHeaderName);
    if (!range) {
        return {};
    }

    const TString bytesPrefix = "bytes=";
    if (!range->StartsWith(bytesPrefix)) {
        THROW_ERROR_EXCEPTION("Invalid range header format")
            << TErrorAttribute("range", *range);
    }

    auto indices = range->substr(bytesPrefix.size());
    std::pair<int64_t, int64_t> rangeValue;
    StringSplitter(indices).Split('-').CollectInto(&rangeValue.first, &rangeValue.second);
    return rangeValue;
}

void SetRange(const THeadersPtr& headers, std::pair<int64_t, int64_t> range, int64_t total)
{
    headers->Set(ContentRangeHeaderName, Format("bytes %v-%v/%v", range.first, range.second, total));
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttp
