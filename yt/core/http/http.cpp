#include "http.h"

#include <yt/contrib/http-parser/http_parser.h>

namespace NYT {
namespace NHttp {

using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TStringBuf ToHttpString(EMethod method)
{
    switch(method) {
#define XX(num, name, string) case EMethod::name: return AsStringBuf(#string);
    YT_HTTP_METHOD_MAP(XX)
#undef XX
    default: THROW_ERROR_EXCEPTION("Invalid method %v", method);
    }
}

TStringBuf ToHttpString(EStatusCode code)
{
    switch(code) {
#define XX(num, name, string) case EStatusCode::name: return AsStringBuf(#string);
    YT_HTTP_STATUS_MAP(XX)
#undef XX
    default: THROW_ERROR_EXCEPTION("Invalid status code %d", code);
    }
}

////////////////////////////////////////////////////////////////////////////////

TUrlRef ParseUrl(TStringBuf url)
{
    TUrlRef urlRef;

    http_parser_url parsed;
    if (0 != yt_http_parser_parse_url(url.data(), url.size(), false, &parsed)) {
        THROW_ERROR_EXCEPTION("Invalid URL")
            << TErrorAttribute("url", url);
    }

    auto convertField = [&] (int flag) -> TStringBuf {
        if (parsed.field_set & (1 << flag)) {
            const auto& data = parsed.field_data[flag];
            return url.SubString(data.off, data.len);
        }

        return TStringBuf();
    };
    
    urlRef.Protocol = convertField(UF_SCHEMA);
    urlRef.User = convertField(UF_USERINFO);
    urlRef.Host = convertField(UF_HOST);
    urlRef.PortStr = convertField(UF_PORT);
    urlRef.Path = convertField(UF_PATH);
    urlRef.RawQuery = convertField(UF_QUERY);

    if (parsed.field_set & (1 << UF_PORT)) {
        urlRef.Port = parsed.port;
    }
    
    return urlRef;
}

////////////////////////////////////////////////////////////////////////////////

void THeaders::Add(const TString& header, TString value)
{
    ValidateValue(header, value);

    auto lower = to_lower(header);

    auto& entry = Raw_[lower];
    entry.OriginalHeaderName = header;
    entry.Values.push_back(std::move(value));
}

void THeaders::Remove(const TString& header)
{
    auto lower = to_lower(header);
    Raw_.erase(lower);
}

void THeaders::Set(const TString& header, TString value)
{
    ValidateValue(header, value);

    auto lower = to_lower(header);

    Raw_[lower] = {header, {std::move(value)}};
}

const TString* THeaders::Find(const TString& header) const
{
    auto lower = to_lower(header);

    auto it = Raw_.find(lower);
    if (it == Raw_.end()) {
        return nullptr;
    }

    // Actually impossible, but just in case.
    if (it->second.Values.empty()) {
        return nullptr;
    }

    return &it->second.Values[0];
}

TString THeaders::GetOrThrow(const TString& header) const
{
    auto value = Find(header);
    if (!value) {
        THROW_ERROR_EXCEPTION("Header %Qv not found", header);
    }
    return *value;
}

const SmallVector<TString, 1>& THeaders::GetAll(const TString& header) const
{
    auto lower = to_lower(header);

    auto it = Raw_.find(lower);
    if (it == Raw_.end()) {
        THROW_ERROR_EXCEPTION("Header %Qv not found", header);
    }

    return it->second.Values;
}

void THeaders::WriteTo(IOutputStream* out, const THashSet<TString>* filtered) const
{
    for (const auto& pair : Raw_) {
        // TODO(prime): sanitize headers
        const auto& header = pair.second.OriginalHeaderName;
        const auto& values = pair.second.Values;

        if (filtered && filtered->find(header) != filtered->end()) {
            continue;
        }

        for (const auto& value : values) {
            *out << header << ": " << value << "\r\n";
        }
    }
}

void THeaders::ValidateValue(TStringBuf header, TStringBuf value)
{
    if (value.find('\n') != TString::npos) {
        THROW_ERROR_EXCEPTION("Header value should not contain newline symbol")
            << TErrorAttribute("header", header)
            << TErrorAttribute("value", value);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHttp
} // namespace NYT
