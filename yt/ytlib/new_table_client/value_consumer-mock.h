#pragma once

#include "value_consumer.h"
#include "name_table.h"

#include <contrib/testing/gmock.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

class TMockValueConsumer
    : public IValueConsumer
{
public:
    TMockValueConsumer(
        TNameTablePtr nameTable,
        bool allowUnknownColumns)
        : NameTable_(nameTable)
        , AllowUnknowsColumns_(allowUnknownColumns)
    { }

    MOCK_METHOD0(OnBeginRow, void());
    MOCK_METHOD1(OnValue, void(const TUnversionedValue& value));
    MOCK_METHOD0(OnEndRow, void());

    virtual TNameTablePtr GetNameTable() const override
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

} // namespace NVersionedTableClient
} // namespace NYT
