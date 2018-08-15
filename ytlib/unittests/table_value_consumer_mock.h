#pragma once

#include <yt/core/test_framework/framework.h>

#include <yt/client/table_client/name_table.h>
#include <yt/ytlib/table_client/value_consumer.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TMockValueConsumer
    : public TValueConsumerBase
{
public:
    TMockValueConsumer(
        TNameTablePtr nameTable,
        bool allowUnknownColumns,
        const TTableSchema& schema = TTableSchema(),
        const TTypeConversionConfigPtr& typeConversionConfig = New<TTypeConversionConfig>())
        : TValueConsumerBase(schema, typeConversionConfig)
        , NameTable_(nameTable)
        , AllowUnknowsColumns_(allowUnknownColumns)
    {
        InitializeIdToTypeMapping();
    }

    MOCK_METHOD0(OnBeginRow, void());
    MOCK_METHOD1(OnMyValue, void(const TUnversionedValue& value));
    MOCK_METHOD0(OnEndRow, void());

    virtual const TNameTablePtr& GetNameTable() const override
    {
        return NameTable_;
    }

    virtual bool GetAllowUnknownColumns() const override
    {
        return AllowUnknowsColumns_;
    }

private:
    TNameTablePtr NameTable_;
    bool AllowUnknowsColumns_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
