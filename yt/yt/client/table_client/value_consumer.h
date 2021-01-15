#pragma once

#include "public.h"
#include "config.h"

#include "schema.h"

#include <yt/client/table_client/unversioned_row.h>

#include <yt/core/misc/blob_output.h>
#include <yt/core/misc/small_vector.h>

#include <yt/core/yson/writer.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct IValueConsumer
{
    virtual ~IValueConsumer() = default;

    virtual const TNameTablePtr& GetNameTable() const = 0;
    virtual const TTableSchemaPtr& GetSchema() const = 0;

    virtual bool GetAllowUnknownColumns() const = 0;

    virtual void OnBeginRow() = 0;
    virtual void OnValue(const TUnversionedValue& value) = 0;
    virtual void OnEndRow() = 0;
};

struct IFlushableValueConsumer
    : public virtual IValueConsumer
{
    virtual TFuture<void> Flush() = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TValueConsumerBase
    : public virtual IValueConsumer
{
public:
    TValueConsumerBase(
        TTableSchemaPtr schema,
        TTypeConversionConfigPtr typeConversionConfig);

    virtual void OnValue(const TUnversionedValue& value) override;
    virtual const TTableSchemaPtr& GetSchema() const override;

protected:
    const TTableSchemaPtr Schema_;

    virtual void OnMyValue(const TUnversionedValue& value) = 0;

    // This should be done in a separate base class method because we can't do
    // it in a constructor (it depends on a derived type GetNameTable() implementation that
    // can't be called from a parent class).
    void InitializeIdToTypeMapping();

private:
    const TTypeConversionConfigPtr TypeConversionConfig_;

    std::vector<EValueType> NameTableIdToType_;

    // This template method is private and only used in value_consumer.cpp with T = i64/ui64,
    // so it is not necessary to implement it in value_consumer-inl.h.
    template <typename T>
    void ProcessIntegralValue(const TUnversionedValue& value, EValueType columnType);

    void ProcessInt64Value(const TUnversionedValue& value, EValueType columnType);
    void ProcessUint64Value(const TUnversionedValue& value, EValueType columnType);
    void ProcessBooleanValue(const TUnversionedValue& value, EValueType columnType);
    void ProcessDoubleValue(const TUnversionedValue& value, EValueType columnType);
    void ProcessStringValue(const TUnversionedValue& value, EValueType columnType);

    void ThrowConversionException(const TUnversionedValue& value, EValueType columnType, const TError& error);
};

////////////////////////////////////////////////////////////////////////////////

class TBuildingValueConsumer
    : public TValueConsumerBase
{
public:
   TBuildingValueConsumer(
        TTableSchemaPtr schema,
        NLogging::TLogger logger,
        TTypeConversionConfigPtr typeConversionConfig = New<TTypeConversionConfig>());

    std::vector<TUnversionedRow> GetRows() const;

    virtual const TNameTablePtr& GetNameTable() const override;

    void SetAggregate(bool value);
    void SetTreatMissingAsNull(bool value);

private:
    const NLogging::TLogger Logger;
    TUnversionedOwningRowBuilder Builder_;
    std::vector<TUnversionedOwningRow> Rows_;

    const TNameTablePtr NameTable_;

    bool Aggregate_ = false;

    std::vector<bool> WrittenFlags_;
    bool TreatMissingAsNull_ = false;

    TBlobOutput ValueBuffer_;

    bool LogNullToEntity_ = true;

    virtual bool GetAllowUnknownColumns() const override;

    virtual void OnBeginRow() override;
    virtual void OnMyValue(const TUnversionedValue& value) override;
    virtual void OnEndRow() override;

    TUnversionedValue MakeAnyFromScalar(const TUnversionedValue& value);
};

////////////////////////////////////////////////////////////////////////////////

class TWritingValueConsumer
    : public TValueConsumerBase
    , public IFlushableValueConsumer
{
public:
    explicit TWritingValueConsumer(
        IUnversionedWriterPtr writer,
        TTypeConversionConfigPtr typeConversionConfig = New<TTypeConversionConfig>(),
        i64 maxRowBufferSize = 1_MB);

    virtual TFuture<void> Flush() override;
    virtual const TNameTablePtr& GetNameTable() const override;

    virtual bool GetAllowUnknownColumns() const override;

    virtual void OnBeginRow() override;
    virtual void OnMyValue(const TUnversionedValue& value) override;
    virtual void OnEndRow() override;

private:
    const IUnversionedWriterPtr Writer_;
    const i64 MaxRowBufferSize_;

    const TRowBufferPtr RowBuffer_;

    std::vector<TUnversionedRow> Rows_;
    SmallVector<TUnversionedValue, TypicalColumnCount> Values_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
