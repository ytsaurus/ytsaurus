#ifndef ATTRIBUTE_INL_H_
#error "Direct inclusion of this file is not allowed, include attribute.h"
// For the sake of sane code completion
#include "attribute.h"
#endif

namespace NYT::NOrm::NClient::NNative {

////////////////////////////////////////////////////////////////////////////////

template <class TAttribute>
void ParseAttribute(
    TAttribute& attribute,
    NYTree::INodePtr node,
    const TParseAttributeOptions& options)
{
    if constexpr (std::derived_from<TAttribute, google::protobuf::Message>) {
        NYson::TProtobufWriterOptions protobufWriterOptions;
        switch (options.UnknownFieldMode) {
            case EUnknownFieldMode::Skip:
                protobufWriterOptions.UnknownYsonFieldModeResolver =
                    NYson::TProtobufWriterOptions::CreateConstantUnknownYsonFieldModeResolver(
                        NYT::NYson::EUnknownYsonFieldsMode::Skip);
                break;
            case EUnknownFieldMode::Fail:
                protobufWriterOptions.UnknownYsonFieldModeResolver =
                    NYson::TProtobufWriterOptions::CreateConstantUnknownYsonFieldModeResolver(
                        NYT::NYson::EUnknownYsonFieldsMode::Fail);
                break;
        }

        NYTree::DeserializeProtobufMessage(
            attribute,
            NYson::ReflectProtobufMessageType<TAttribute>(),
            node,
            protobufWriterOptions);
    } else {
        Deserialize(attribute, node);
    }
}

// std::vector
template <class TAttribute>
void ParseAttribute(
    std::vector<TAttribute>& attribute,
    NYTree::INodePtr node,
    const TParseAttributeOptions& options)
{
    auto listNode = node->AsList();
    auto size = listNode->GetChildCount();
    attribute.resize(size);

    for (int index = 0; index < size; ++index) {
        ParseAttribute(attribute[index], listNode->GetChildOrThrow(index), options);
    }
}

// std::optional
template <class TAttribute>
void ParseAttribute(
    std::optional<TAttribute>& attribute,
    NYTree::INodePtr node,
    const TParseAttributeOptions& options)
{
    if (node->GetType() == NYTree::ENodeType::Entity) {
        attribute = std::nullopt;
    } else {
        if (!attribute) {
            attribute = TAttribute();
        }
        ParseAttribute(*attribute, node, options);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NNative
