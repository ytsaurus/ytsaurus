#pragma once

#include <yt/client/table_client/unversioned_row.h>
#include <yt/client/table_client/name_table.h>
#include <yt/ytlib/table_client/value_consumer.h>

#include <vector>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TCollectingValueConsumer
    : public NTableClient::IValueConsumer
{
public:
    virtual const NTableClient::TNameTablePtr& GetNameTable() const override
    {
        return NameTable_;
    }

    virtual bool GetAllowUnknownColumns() const override
    {
        return true;
    }

    virtual void OnBeginRow() override
    { }

    virtual void OnValue(const NTableClient::TUnversionedValue& value) override
    {
        Builder_.AddValue(value);
    }

    virtual void OnEndRow() override
    {
        RowList_.emplace_back(Builder_.FinishRow());
    }

    NTableClient::TUnversionedRow GetRow(size_t rowIndex)
    {
        return RowList_.at(rowIndex);
    }

    TNullable<NTableClient::TUnversionedValue> FindRowValue(size_t rowIndex, TStringBuf columnName) const
    {
        NTableClient::TUnversionedRow row = RowList_.at(rowIndex);
        auto id = GetNameTable()->GetIdOrThrow(columnName);

        for (const auto& value : row) {
            if (value.Id == id) {
                return value;
            }
        }
        return Null;
    }

    NTableClient::TUnversionedValue GetRowValue(size_t rowIndex, TStringBuf columnName) const
    {
        auto row = FindRowValue(rowIndex, columnName);
        if (!row) {
            THROW_ERROR_EXCEPTION("Cannot find column %Qv", columnName);
        }
        return *row;
    }

    size_t Size() const
    {
        return RowList_.size();
    }

private:
    const NTableClient::TNameTablePtr NameTable_ = New<NTableClient::TNameTable>();
    NTableClient::TUnversionedOwningRowBuilder Builder_;
    std::vector<NTableClient::TUnversionedOwningRow> RowList_;
};

////////////////////////////////////////////////////////////////////////////////

NTableClient::TUnversionedOwningRow MakeRow(const std::vector<NTableClient::TUnversionedValue>& values);

i64 GetInt64(const NTableClient::TUnversionedValue& row);
ui64 GetUint64(const NTableClient::TUnversionedValue& row);
double GetDouble(const NTableClient::TUnversionedValue& row);
bool GetBoolean(const NTableClient::TUnversionedValue& row);
TString GetString(const NTableClient::TUnversionedValue& row);
NYTree::INodePtr GetAny(const NTableClient::TUnversionedValue& row);
bool IsNull(const NTableClient::TUnversionedValue& row);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
