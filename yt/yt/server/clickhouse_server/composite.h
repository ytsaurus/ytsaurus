#pragma once

#include <yt/yt/client/table_client/logical_type.h>

#include "private.h"

namespace NYT::NClickHouseServer
{

////////////////////////////////////////////////////////////////////////////////

class TCompositeValueToClickHouseColumnConverter
{
public:
    TCompositeValueToClickHouseColumnConverter(
        NTableClient::TComplexTypeFieldDescriptor descriptor,
        TCompositeSettingsPtr settings);

    ~TCompositeValueToClickHouseColumnConverter();

    DB::ColumnPtr FlushColumn();

    DB::DataTypePtr GetDataType() const;

    void ConsumeYson(TStringBuf yson);
    void ConsumeNull();

private:
    class TImpl;
    const std::unique_ptr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

}
