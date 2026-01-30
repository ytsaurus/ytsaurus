#pragma once

#include "yson_parser_adapter.h"

#include <library/cpp/yt/error/public.h>

#include <memory>

#include <DataTypes/IDataType.h>
#include <Formats/JSONExtractTree.h>
#include <Formats/FormatSettings.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

template <typename NumberType>
TError TryGetNumericValueFromYsonElement(NumberType& value, const TYsonParserAdapter::Element& element, bool convertBoolToNumber, bool allowTypeConversion);

////////////////////////////////////////////////////////////////////////////////

class IYsonTreeNodeExtractor
{
public:
    virtual ~IYsonTreeNodeExtractor() = default;

    virtual TError ExtractNodeToColumn(
        DB::IColumn& column,
        const TYsonParserAdapter::Element& node,
        const DB::JSONExtractInsertSettings& insertSettings,
        const DB::FormatSettings& formatSettings) const = 0;
};

std::unique_ptr<IYsonTreeNodeExtractor> CreateYsonTreeNodeExtractor(const DB::DataTypePtr& type);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer

#define YSON_EXTRACT_TREE_INL_H_
#include "yson_extract_tree-inl.h"
#undef YSON_EXTRACT_TREE_INL_H_
