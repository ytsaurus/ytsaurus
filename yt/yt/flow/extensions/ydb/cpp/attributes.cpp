#include "attributes.h"

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

using namespace NYson;
using NYTree::ConvertTo;

////////////////////////////////////////////////////////////////////////////////

namespace {

void ValidateYdbMessageMetaLimits(i64 itemCount, i64 size)
{
    if (itemCount > MaxYdbMessageMetaItemCount) {
        THROW_ERROR_EXCEPTION("Too many topic message metadata items: %v > %v",
            itemCount,
            MaxYdbMessageMetaItemCount);
    }

    if (size > MaxYdbMessageMetaSize) {
        THROW_ERROR_EXCEPTION("Topic message metadata is too large: %v > %v",
            size,
            MaxYdbMessageMetaSize);
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

bool IsReservedYdbTopicAttributeName(const std::string& name)
{
    if (name == KafkaKeyAttributeName) {
        return false;
    }

    // #LegacyFlowQueueMetaName is not under the prefix, so it has to be named explicitly for
    // as long as any source still reads it as the flow queue meta.
    return name.starts_with(ReservedFlowAttributeNamePrefix) ||
        name.starts_with(ReservedYdbAttributeNamePrefix) ||
        name == LegacyFlowQueueMetaName;
}

void ValidateYdbTopicAttributes(const TYdbTopicAttributes& attributes)
{
    i64 size = 0;
    for (const auto& [name, value] : attributes) {
        if (IsReservedYdbTopicAttributeName(name)) {
            THROW_ERROR_EXCEPTION("Topic message attribute %Qv is reserved",
                name);
        }
        size += std::ssize(name) + std::ssize(value);
    }

    ValidateYdbMessageMetaLimits(std::ssize(attributes), size);
}

void ValidateYdbMessageMeta(const TYdbMessageMeta& meta)
{
    i64 size = 0;
    for (const auto& [name, value] : meta) {
        size += std::ssize(name) + std::ssize(value);
    }

    ValidateYdbMessageMetaLimits(std::ssize(meta), size);
}

TYsonString PackYdbTopicAttributes(const TYdbTopicAttributes& attributes)
{
    ValidateYdbTopicAttributes(attributes);
    return ConvertToYsonString(attributes);
}

TYdbTopicAttributes UnpackYdbTopicAttributes(TYsonStringBuf value)
{
    return ConvertTo<TYdbTopicAttributes>(value);
}

TYdbTopicAttributes ParseYdbTopicAttributesColumn(std::optional<TYsonStringBuf> value)
{
    if (!value) {
        return {};
    }

    auto attributes = UnpackYdbTopicAttributes(*value);
    ValidateYdbTopicAttributes(attributes);
    return attributes;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
