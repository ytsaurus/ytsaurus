#pragma once

#include "public.h"
#include "config.h"

#include <yt/yt/client/complex_types/named_structures_yson.h>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

class TUnversionedValueYsonWriter
{
public:
    TUnversionedValueYsonWriter(
        const NTableClient::TNameTablePtr& nameTable,
        const NTableClient::TTableSchemaPtr& tableSchema,
        EComplexTypeMode complexTypeMode,
        bool skipNullValues);

    void WriteValue(const NTableClient::TUnversionedValue& value, NYson::IYsonConsumer* consumer);

private:
    THashMap<int, NComplexTypes::TYsonConverter> ColumnConverters_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats