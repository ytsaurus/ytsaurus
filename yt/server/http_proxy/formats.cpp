#include "formats.h"

#include <yt/core/ytree/helpers.h>
#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NHttpProxy {

using namespace NFormats;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const THashMap<TString, TString> MimeTypeToFormatTable = {
    {"application/json", "json"},
    {"application/x-yamr-delimited",        "<lenval=%false; has_subkey=%false>yamr"},
    {"application/x-yamr-lenval",           "<lenval=%true; has_subkey=%false>yamr"},
    {"application/x-yamr-subkey-delimited", "<lenval=%false; has_subkey=%true>yamr"},
    {"application/x-yamr-subkey-lenval",    "<lenval=%true; has_subkey=%true>yamr"},
    {"application/x-yt-yson-binary", "<format=binary>yson"},
    {"application/x-yt-yson-pretty", "<format=pretty>yson"},
    {"application/x-yt-yson-text",   "<format=text>yson"},
    {"text/csv",                  "<record_separator=\",\"; key_value_separator=\":\">dsv"},
    {"text/tab-separated-values", "dsv"},
    {"text/x-tskv",               "<line_prefix=tskv>dsv"},
};

static const std::vector<TString> OutputMimeTypePriorityForStructuredType = {
    "application/json",
    "application/x-yt-yson-pretty",
    "application/x-yt-yson-text",
    "application/x-yt-yson-binary",
};

static const std::vector<TString> OutputMimeTypePriorityForTabularType = {
    "application/json",
    "application/x-yamr-delimited",
    "application/x-yamr-lenval",
    "application/x-yamr-subkey-delimited",
    "application/x-yamr-subkey-lenval",
    "application/x-yt-yson-binary",
    "application/x-yt-yson-text",
    "application/x-yt-yson-pretty",
    "text/csv",
    "text/tab-separated-values",
    "text/x-tskv",
};

////////////////////////////////////////////////////////////////////////////////

TNullable<TFormat> MimeTypeToFormat(const TString& mimeType)
{
    auto format = MimeTypeToFormatTable.find(mimeType);
    if (format == MimeTypeToFormatTable.end()) {
        return {};
    }

    return ConvertTo<TFormat>(TYsonString(format->second));
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
            auto recordSeparator = format.Attributes().Find<TString>("record_separator");
            auto keyValueSeparator = format.Attributes().Find<TString>("key_value_separator");
            auto linePrefix = format.Attributes().Find<TString>("line_prefix");

            if (TString{","} == recordSeparator && TString{":"} == keyValueSeparator) {
                return "text/csv";
            } else if (TString{"tskv"} == linePrefix) {
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
        case EFormatType::WebJson:
            return "application/json";
        default:
            THROW_ERROR_EXCEPTION("Cannot determine mime-type for format")
                << TErrorAttribute("format", format);
    }
}

NFormats::TFormat GetDefaultFormatForDataType(EDataType dataType)
{
    if (dataType == EDataType::Structured) {
        return NFormats::TFormat(EFormatType::Json);
    } else if (dataType == EDataType::Tabular) {
        auto attributes = CreateEphemeralAttributes();
        attributes->Set("format", "text");
        return NFormats::TFormat(EFormatType::Yson, attributes.get());
    } else {
        return NFormats::TFormat(EFormatType::Yson);
    }
}

NYTree::INodePtr ConvertBytesToNode(
    const TString& bytes,
    const NFormats::TFormat& format)
{
    TMemoryInput stream{bytes.Data(), bytes.Size()};
    return ConvertToNode(CreateProducerForFormat(
        format,
        EDataType::Structured,
        &stream));
}

TNullable<TString> GetBestAcceptedType(
    NFormats::EDataType outputType,
    const TString& clientAcceptHeader)
{
    if (clientAcceptHeader.Contains(";q=")) {
        THROW_ERROR_EXCEPTION("Quality value inside \"Accept\" header is not supported");
    }

    if (clientAcceptHeader.Contains(",")) {
        THROW_ERROR_EXCEPTION("Multiple MIME types inside \"Accept\" header are not supported");
    }

    if (outputType == EDataType::Structured) {
        if (clientAcceptHeader == "*/*") {
            return OutputMimeTypePriorityForStructuredType[0];
        }

        for (const auto& mimeType : OutputMimeTypePriorityForStructuredType) {
            if (mimeType == clientAcceptHeader) {
                return clientAcceptHeader;
            }
        }
    } else if (outputType == EDataType::Tabular) {
        if (clientAcceptHeader == "*/*") {
            return OutputMimeTypePriorityForTabularType[0];
        }

        for (const auto& mimeType : OutputMimeTypePriorityForTabularType) {
            if (mimeType == clientAcceptHeader) {
                return clientAcceptHeader;
            }
        }
    }

    return {};
}

////////////////////////////////////////////////////////////////////////////////

static const std::vector<std::pair<TString, EContentEncoding>> AllEncodings = {
    { "gzip", EContentEncoding::Gzip },
    { "identity", EContentEncoding::None },
    { "br", EContentEncoding::Brotli },
    { "x-lzop", EContentEncoding::Lzop },
    { "y-lzo", EContentEncoding::Lzo },
    { "y-lzf", EContentEncoding::Lzf },
    { "y-snappy", EContentEncoding::Snappy },
    { "deflate", EContentEncoding::Deflate },
};

TNullable<TString> GetBestAcceptedEncoding(const TString& clientAcceptEncodingHeader)
{
    // TODO(prime@): Implement spec.
    for (const auto& encoding : AllEncodings) {
        if (clientAcceptEncodingHeader.Contains(encoding.first)) {
            return encoding.first;
        }
    }

    return {};
}

TNullable<EContentEncoding> EncodingToCompression(const TString& encoding)
{
    for (const auto& supportedEncoding : AllEncodings) {
        if (encoding == supportedEncoding.first) {
            return supportedEncoding.second;
        }
    }

    return {};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHttpProxy
} // namespace NYT

