#pragma once

#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/value_consumer.h>

#include <vector>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TCollectingValueConsumer
    : public NTableClient::IValueConsumer
{
public:
    explicit TCollectingValueConsumer(NTableClient::TTableSchemaPtr schema = New<NTableClient::TTableSchema>())
        : Schema_(std::move(schema))
    { }

    explicit TCollectingValueConsumer(NTableClient::TNameTablePtr nameTable, NTableClient::TTableSchemaPtr schema = New<NTableClient::TTableSchema>())
        : Schema_(std::move(schema))
        , NameTable_(std::move(nameTable))
    { }

    virtual const NTableClient::TNameTablePtr& GetNameTable() const override
    {
        return NameTable_;
    }

    virtual const NTableClient::TTableSchemaPtr& GetSchema() const override
    {
        return Schema_;
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

    std::optional<NTableClient::TUnversionedValue> FindRowValue(size_t rowIndex, TStringBuf columnName) const
    {
        NTableClient::TUnversionedRow row = RowList_.at(rowIndex);
        auto id = GetNameTable()->GetIdOrThrow(columnName);

        for (const auto& value : row) {
            if (value.Id == id) {
                return value;
            }
        }
        return std::nullopt;
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

    const std::vector<NTableClient::TUnversionedOwningRow>& GetRowList() const {
        return RowList_;
    }

private:
    const NTableClient::TTableSchemaPtr Schema_;
    const NTableClient::TNameTablePtr NameTable_ = New<NTableClient::TNameTable>();
    NTableClient::TUnversionedOwningRowBuilder Builder_;
    std::vector<NTableClient::TUnversionedOwningRow> RowList_;
};

////////////////////////////////////////////////////////////////////////////////

class TTableField {
public:
    template <typename T>
    TTableField(TString name, T value)
        : Name_(std::move(name))
        , Value_(std::move(value))
    { }

    template <>
    TTableField(const TString name, TStringBuf value)
        : Name_(std::move(name))
        , Value_(TString(value))
    { }

    NTableClient::TUnversionedValue ToUnversionedValue(const NTableClient::TNameTablePtr& nameTable) const;

private:
    TString Name_;
    std::variant<i64, ui64, double, bool, TString, nullptr_t> Value_;
};

////////////////////////////////////////////////////////////////////////////////

NTableClient::TUnversionedOwningRow MakeRow(const std::vector<NTableClient::TUnversionedValue>& values);
NTableClient::TUnversionedOwningRow MakeRow(
    const NTableClient::TNameTablePtr& nameTable,
    const std::initializer_list<TTableField>& values);

i64 GetInt64(const NTableClient::TUnversionedValue& row);
ui64 GetUint64(const NTableClient::TUnversionedValue& row);
double GetDouble(const NTableClient::TUnversionedValue& row);
bool GetBoolean(const NTableClient::TUnversionedValue& row);
TString GetString(const NTableClient::TUnversionedValue& row);
NYTree::INodePtr GetAny(const NTableClient::TUnversionedValue& row);
NYTree::INodePtr GetComposite(const NTableClient::TUnversionedValue& row);
bool IsNull(const NTableClient::TUnversionedValue& row);

////////////////////////////////////////////////////////////////////////////////

namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT