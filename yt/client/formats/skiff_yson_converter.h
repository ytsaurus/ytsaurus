#pragma once

#include <yt/client/table_client/public.h>

#include <yt/core/skiff/public.h>
#include <yt/core/yson/public.h>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

using TYsonToSkiffConverter = std::function<void(NYson::TYsonPullParserCursor*, NSkiff::TCheckedInDebugSkiffWriter*)>;
using TSkiffToYsonConverter = std::function<void(NSkiff::TCheckedInDebugSkiffParser*, NYson::IYsonConsumer*)>;

struct TYsonToSkiffConverterConfig
{
    // Usually skiffSchema MUST match descriptor.LogicalType.
    // But when SparseField is set to true and descriptor.LogicalType is Optional<SomeInnerType>
    // skiffSchema CAN match SomeInnerType. In that case returned converter will throw error when it gets empty value.
    //
    // Useful for sparse fields.
    bool ExpectTopLevelOptionalSet = false;
};

TYsonToSkiffConverter CreateYsonToSkiffConverter(
    NTableClient::TComplexTypeFieldDescriptor descriptor,
    const NSkiff::TSkiffSchemaPtr& skiffSchema, const TYsonToSkiffConverterConfig& config = {});

TSkiffToYsonConverter CreateSkiffToYsonConverter(
    NTableClient::TComplexTypeFieldDescriptor descriptor,
    const NSkiff::TSkiffSchemaPtr& skiffSchema);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::Formats