#include "config.h"
#include "formats.h"

#include <yt/yt/server/lib/misc/format_manager.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/client/security_client/public.h>

#include <yt/yt/core/http/helpers.h>
#include <yt/yt/core/http/http.h>

#include <yt/yt/core/ytree/helpers.h>
#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NHttpProxy {

using namespace NFormats;
using namespace NHttp;
using namespace NHttp::NHeaders;
using namespace NYson;
using namespace NYTree;
using namespace NServer;

////////////////////////////////////////////////////////////////////////////////

static INodePtr MimeTypeToFormatNode(TStringBuf mimeType)
{
    static const THashMap<TStringBuf, TStringBuf> MimeTypeToFormatTable = {
        {"application/json",                    "json"},
        {"application/x-yamr-delimited",        "<lenval=%false; has_subkey=%false>yamr"},
        {"application/x-yamr-lenval",           "<lenval=%true; has_subkey=%false>yamr"},
        {"application/x-yamr-subkey-delimited", "<lenval=%false; has_subkey=%true>yamr"},
        {"application/x-yamr-subkey-lenval",    "<lenval=%true; has_subkey=%true>yamr"},
        {"application/x-yt-yson-binary",        "<format=binary>yson"},
        {"application/x-yt-yson-pretty",        "<format=pretty>yson"},
        {"application/x-yt-yson-text",          "<format=text>yson"},
        {"text/csv",                            "<record_separator=\",\"; key_value_separator=\":\">dsv"},
        {"text/tab-separated-values",           "dsv"},
        {"text/x-tskv",                         "<line_prefix=tskv>dsv"},
    };

    auto format = MimeTypeToFormatTable.find(mimeType);
    if (format == MimeTypeToFormatTable.end()) {
        return {};
    }

    return ConvertToNode(TYsonString(format->second));
}

////////////////////////////////////////////////////////////////////////////////

static INodePtr GetDefaultFormatNodeForDataType(EDataType dataType)
{
    if (dataType == EDataType::Structured) {
        return ConvertToNode("json");
    } else if (dataType == EDataType::Tabular) {
        return ConvertToNode(TYsonString(TStringBuf("<format=text>yson")));
    } else {
        return ConvertToNode("yson");
    }
}

////////////////////////////////////////////////////////////////////////////////

TFormat InferFormat(
    const TFormatManager& formatManager,
    const std::string& ytHeaderName,
    const TFormat& ytHeaderFormat,
    const std::optional<std::string>& ytHeader,
    const std::string& mimeHeaderName,
    const std::string* mimeHeader,
    EFormatTarget target,
    EDataType dataType)
{
    if (target == EFormatTarget::Output && (
        dataType == EDataType::Null ||
        dataType == EDataType::Binary))
    {
        auto origin = Format("default format for %Qlv and %Qlv output data type", EDataType::Null, EDataType::Binary);
        return formatManager.ConvertToFormat(ConvertToNode(EFormatType::Yson), origin);
    }

    if (ytHeader) {
        INodePtr formatNode;
        try {
            formatNode = ConvertBytesToNode(*ytHeader, ytHeaderFormat);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Unable to parse %v header",
                ytHeaderName)
                << ex;
        }
        return formatManager.ConvertToFormat(formatNode, Format("format from %Qv header", ytHeaderName));
    }
    INodePtr formatNode;
    if (mimeHeader) {
        auto contentType = StripString(*mimeHeader);
        formatNode = MimeTypeToFormatNode(contentType);
        if (formatNode) {
            return formatManager.ConvertToFormat(formatNode, Format("format inferred from %Qv header", mimeHeaderName));
        }
    }
    formatNode = GetDefaultFormatNodeForDataType(dataType);
    return formatManager.ConvertToFormat(formatNode, Format("%Qlv format inferred from data type %Qlv", target, dataType));
}

TFormat InferHeaderFormat(const TFormatManager& formatManager, const std::string* ytHeader)
{
    if (!ytHeader) {
        return formatManager.ConvertToFormat(ConvertToNode(EFormatType::Json), "default header format");
    }

    INodePtr formatNode;
    try {
        TYsonString header(StripString(TString(*ytHeader)));
        formatNode = ConvertTo<INodePtr>(header);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Unable to parse X-YT-Header-Format header")
            << ex;
    }
    return formatManager.ConvertToFormat(formatNode, "header format from X-YT-Header-Format header");
}

TString FormatToMime(const NFormats::TFormat& format)
{
    switch (format.GetType()) {
        case EFormatType::SchemafulDsv:
            return "text/tab-separated-values";
        case EFormatType::YamredDsv:
            return "text/tab-separated-values";
        case EFormatType::Yamr: {
            auto lenval = format.Attributes().Find<bool>("lenval");
            auto hasSubkey = format.Attributes().Find<bool>("has_subkey");
            if (lenval && *lenval == true) {
                if (hasSubkey && *hasSubkey == true) {
                    return "application/x-yamr-subkey-lenval";
                } else {
                    return "application/x-yamr-lenval";
                }
            } else {
                if (hasSubkey && *hasSubkey == true) {
                    return "application/x-yamr-subkey-delimited";
                } else {
                    return "application/x-yamr-delimited";
                }
            }
        }
        case EFormatType::Dsv: {
            auto recordSeparator = format.Attributes().Find<std::string>("record_separator");
            auto keyValueSeparator = format.Attributes().Find<std::string>("key_value_separator");
            auto linePrefix = format.Attributes().Find<std::string>("line_prefix");

            if (std::string{","} == recordSeparator && std::string{":"} == keyValueSeparator) {
                return "text/csv";
            } else if (std::string{"tskv"} == linePrefix) {
                return "text/x-tskv";
            } else {
                return "text/tab-separated-values";
            }
        }
        case EFormatType::Json:
            return "application/json";
        case EFormatType::Yson: {
            auto ysonFormat = format.Attributes().Find<EYsonFormat>("format");
            if (EYsonFormat::Text == ysonFormat) {
                return "application/x-yt-yson-text";
            } else if (EYsonFormat::Pretty == ysonFormat) {
                return "application/x-yt-yson-pretty";
            } else {
                return "application/x-yt-yson-binary";
            }
        }
        case EFormatType::Skiff:
            return "application/octet-stream";
        case EFormatType::Protobuf:
            return "application/octet-stream";
        case EFormatType::WebJson:
            return "application/json";
        case EFormatType::Arrow:
            return "application/vnd.apache.arrow.stream";
        case EFormatType::Yaml:
            return "application/x-yaml";
        default:
            THROW_ERROR_EXCEPTION("Cannot determine mime-type for format")
                << TErrorAttribute("format", format);
    }
}

NYTree::INodePtr ConvertBytesToNode(
    TStringBuf bytes,
    const NFormats::TFormat& format)
{
    TMemoryInput stream{bytes.data(), bytes.size()};
    return ConvertToNode(CreateProducerForFormat(
        format,
        EDataType::Structured,
        &stream));
}

////////////////////////////////////////////////////////////////////////////////

void FillFormattedYTError(
    const THeadersPtr& headers,
    const TError& error,
    const TFormat& format)
{
    TString errorString;
    TStringOutput errorStringOutput(errorString);

    auto consumer = CreateConsumerForFormat(
        format,
        EDataType::Structured,
        &errorStringOutput);

    Serialize(error, consumer.get());
    consumer->Flush();

    headers->Add(XYTErrorHeaderName, errorString);
    headers->Add(XYTErrorContentTypeHeaderName, FormatToMime(format));
}

void FillFormattedYTErrorHeaders(
    const IResponseWriterPtr& rsp,
    const TError& error,
    const TFormat& format)
{
    FillFormattedYTError(rsp->GetHeaders(), error, format);
    FillYTErrorResponseHeaders(rsp, error);
}

void FillFormattedYTErrorTrailers(
    const IResponseWriterPtr& rsp,
    const TError& error,
    const TFormat& format)
{
    FillFormattedYTError(rsp->GetTrailers(), error, format);
    FillYTErrorResponseTrailers(rsp, error);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
