#ifndef HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include helpers.h"
// For the sake of sane code completion.
#include "helpers.h"
#endif

#include "request.h"

namespace NYT::NOrm::NClient::NNative {

////////////////////////////////////////////////////////////////////////////////

template <bool ForceYsonPayloadFormat, class T>
TSetUpdate BuildSetAttributeUpdate(NYPath::TYPath attributePath, const T& targetValue)
{
    if constexpr (ForceYsonPayloadFormat ||
        std::same_as<T, NYTree::NProto::TAttributeDictionary> ||
        !std::derived_from<T, ::google::protobuf::Message>)
    {
        return TSetUpdate{
            .Path = std::move(attributePath),
            .Payload = TYsonPayload{
                .Yson = NYson::ConvertToYsonString(targetValue),
            },
            .Recursive = true,
        };
    } else {
        return TSetUpdate{
            .Path = std::move(attributePath),
            .Payload = TProtobufPayload{
                .Protobuf = targetValue.SerializeAsString(),
            },
            .Recursive = true,
        };
    }
}

template <class T>
TSetUpdate BuildSetAttributeUpdate(NYPath::TYPath attributePath, const T& targetValue)
{
    return BuildSetAttributeUpdate<false, T>(std::move(attributePath), targetValue);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NNative
