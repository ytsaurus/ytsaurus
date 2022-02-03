#include "config.h"

namespace NYT::NJson {

////////////////////////////////////////////////////////////////////////////////

TJsonFormatConfig::TJsonFormatConfig()
{
    RegisterParameter("format", Format)
        .Default(EJsonFormat::Text);
    RegisterParameter("attributes_mode", AttributesMode)
        .Default(EJsonAttributesMode::OnDemand);
    RegisterParameter("plain", Plain)
        .Default(false);
    RegisterParameter("encode_utf8", EncodeUtf8)
        .Default(true);
    RegisterParameter("string_length_limit", StringLengthLimit)
        .Default();
    RegisterParameter("stringify", Stringify)
        .Default(false);
    RegisterParameter("annotate_with_types", AnnotateWithTypes)
        .Default(false);
    RegisterParameter("support_infinity", SupportInfinity)
        .Default(false);
    RegisterParameter("stringify_nan_and_infinity", StringifyNanAndInfinity)
        .Default(false);
    RegisterParameter("buffer_size", BufferSize)
        .Default(16 * 1024);
    RegisterParameter("skip_null_values", SkipNullValues)
        .Default(false);

    MemoryLimit = 256_MB;

    RegisterPostprocessor([&] () {
        if (SupportInfinity && StringifyNanAndInfinity) {
            THROW_ERROR_EXCEPTION("\"support_infinity\" and \"stringify_nan_and_infinity\" "
                "cannot be specified simultaneously");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJson
