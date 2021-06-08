#include "config.h"

namespace NYT::NFormats {

// using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TProtobufColumnConfig::Postprocess()
{
    if (Packed && !Repeated) {
        THROW_ERROR_EXCEPTION("Field %Qv is marked \"packed\" but is not marked \"repeated\"",
            Name);
    }

    if (!Type) {
        Type = New<TProtobufTypeConfig>();
        if (!ProtoType) {
            THROW_ERROR_EXCEPTION("One of \"type\" and \"proto_type\" must be specified");
        }
        Type->ProtoType = *ProtoType;
        Type->Fields = std::move(Fields);
        Type->EnumerationName = EnumerationName;
    }

    if (!FieldNumber && Type->ProtoType != EProtobufType::Oneof) {
        THROW_ERROR_EXCEPTION("\"field_number\" is required for type %Qlv",
            Type->ProtoType);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
