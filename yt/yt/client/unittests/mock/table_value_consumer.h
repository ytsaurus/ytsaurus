#pragma once

#include <library/cpp/testing/gtest_extensions/gtest_extensions.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/value_consumer.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TMockValueConsumer
    : public TValueConsumerBase
{
public:
    TMockValueConsumer(
        TNameTablePtr nameTable,
        bool allowUnknownColumns,
        TTableSchemaPtr schema = New<TTableSchema>(),
        TTypeConversionConfigPtr typeConversionConfig = New<TTypeConversionConfig>())
        : TValueConsumerBase(std::move(schema), std::move(typeConversionConfig))
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

} // namespace NYT::NTableClient
