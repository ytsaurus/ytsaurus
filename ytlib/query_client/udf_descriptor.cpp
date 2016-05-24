#include "udf_descriptor.h"

#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NQueryClient {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

const Stroka FunctionDescriptorAttribute = "function_descriptor";
const Stroka AggregateDescriptorAttribute = "aggregate_descriptor";

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TDescriptorType& value, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("tag").Value(ETypeCategory(value.Type.Tag()))
            .DoIf(value.Type.TryAs<TTypeArgument>(), [&] (TFluentMap fluent) {
                fluent.Item("value").Value(value.Type.As<TTypeArgument>());
            })
            .DoIf(value.Type.TryAs<TUnionType>(), [&] (TFluentMap fluent) {
                fluent.Item("value").Value(value.Type.As<TUnionType>());
            })
            .DoIf(value.Type.TryAs<EValueType>(), [&] (TFluentMap fluent) {
                fluent.Item("value").Value(value.Type.As<EValueType>());
            })
        .EndMap();
}

void Deserialize(TDescriptorType& value, INodePtr node)
{
    auto mapNode = node->AsMap();

    auto tagNode = mapNode->GetChild("tag");
    ETypeCategory tag;
    Deserialize(tag, tagNode);

    auto valueNode = mapNode->GetChild("value");
    switch (tag) {
        case ETypeCategory::TypeArgument:
            {
                TTypeArgument type;
                Deserialize(type, valueNode);
                value.Type = type;
                break;
            }
        case ETypeCategory::UnionType:
            {
                TUnionType type;
                Deserialize(type, valueNode);
                value.Type = type;
                break;
            }
        case ETypeCategory::ConcreteType:
            {
                EValueType type;
                Deserialize(type, valueNode);
                value.Type = type;
                break;
            }
        default:
            YUNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

