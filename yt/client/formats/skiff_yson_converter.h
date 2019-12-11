#pragma once

#include <yt/client/table_client/public.h>
#include <yt/client/table_client/row_base.h>

#include <yt/library/skiff/public.h>
#include <yt/core/yson/public.h>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

NSkiff::EWireType GetSkiffTypeForSimpleLogicalType(NTableClient::ESimpleLogicalValueType logicalType);

////////////////////////////////////////////////////////////////////////////////

using TYsonToSkiffConverter = std::function<void(NYson::TYsonPullParserCursor*, NSkiff::TCheckedInDebugSkiffWriter*)>;
using TSkiffToYsonConverter = std::function<void(NSkiff::TCheckedInDebugSkiffParser*, NYson::TCheckedInDebugYsonTokenWriter*)>;

struct TYsonToSkiffConverterConfig
{
    // Usually skiffSchema MUST match descriptor.LogicalType.
    // But when AllowOmitTopLevelOptional is set to true and descriptor.LogicalType is Optional<SomeInnerType>
    // skiffSchema CAN match SomeInnerType. In that case returned converter will throw error when it gets empty value.
    //
    // Useful for sparse fields.
    bool AllowOmitTopLevelOptional = false;
};

TYsonToSkiffConverter CreateYsonToSkiffConverter(
    NTableClient::TComplexTypeFieldDescriptor descriptor,
    const NSkiff::TSkiffSchemaPtr& skiffSchema,
    const TYsonToSkiffConverterConfig& config = {});

struct TSkiffToYsonConverterConfig
{
    // Similar to TYsonToSkiffConverterConfig::AllowOmitTopLevelOptional.
    bool AllowOmitTopLevelOptional = false;
};

TSkiffToYsonConverter CreateSkiffToYsonConverter(
    NTableClient::TComplexTypeFieldDescriptor descriptor,
    const NSkiff::TSkiffSchemaPtr& skiffSchema,
    const TSkiffToYsonConverterConfig& config = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::Formats
