#pragma once

#include "public.h"
#include "unversioned_row.h"

#include <core/misc/blob_output.h>

#include <core/yson/writer.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct IValueConsumer
    : public virtual TRefCounted
{
    virtual TNameTablePtr GetNameTable() const = 0;

    virtual bool GetAllowUnknownColumns() const = 0;

    virtual void OnBeginRow() = 0;
    virtual void OnValue(const TUnversionedValue& value) = 0;
    virtual void OnEndRow() = 0;
};

DEFINE_REFCOUNTED_TYPE(IValueConsumer)

////////////////////////////////////////////////////////////////////////////////

class TBuildingValueConsumer
    : public IValueConsumer
{
public:
    TBuildingValueConsumer(
        const TTableSchema& schema,
        const TKeyColumns& keyColumns);

    const std::vector<TUnversionedOwningRow>& GetOwningRows() const;
    std::vector<TUnversionedRow> GetRows() const;

    virtual TNameTablePtr GetNameTable() const override;

    void SetTreatMissingAsNull(bool value);

private:
    TUnversionedOwningRowBuilder Builder_;
    std::vector<TUnversionedOwningRow> Rows_;

    TTableSchema Schema_;
    TKeyColumns KeyColumns_;
    TNameTablePtr NameTable_;

    std::vector<bool> WrittenFlags_;
    bool TreatMissingAsNull_ = false;

    TBlobOutput ValueBuffer_;
    NYson::TYsonWriter ValueWriter_;

    virtual bool GetAllowUnknownColumns() const override;

    virtual void OnBeginRow() override;
    virtual void OnValue(const TUnversionedValue& value) override;
    virtual void OnEndRow() override;

    TUnversionedValue MakeAnyFromScalar(const TUnversionedValue& value);
};

DEFINE_REFCOUNTED_TYPE(TBuildingValueConsumer)

////////////////////////////////////////////////////////////////////////////////

class TWritingValueConsumer
    : public IValueConsumer
{
public:
    explicit TWritingValueConsumer(
        ISchemalessWriterPtr writer,
        bool flushImmediately = false);

    void Flush();

private:
    ISchemalessWriterPtr Writer_;

    TUnversionedOwningRowBuilder Builder_;
    std::vector<TUnversionedOwningRow> OwningRows_;
    std::vector<TUnversionedRow> Rows_;

    i64 CurrentBufferSize_ = 0;
    bool FlushImmediately_;

    virtual TNameTablePtr GetNameTable() const override;

    virtual bool GetAllowUnknownColumns() const override;

    virtual void OnBeginRow() override;
    virtual void OnValue(const TUnversionedValue& value) override;
    virtual void OnEndRow() override;

};

DEFINE_REFCOUNTED_TYPE(TWritingValueConsumer)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
