#pragma once

#include "private.h"

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/row_batch.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

// Note: in normal situation you should not use this class directly. Use one of the
// helpers from conversion.h or TYTToCHBlockConverter.

//! This class serves for two purposes.
//!
//! First is obvious: if you have a bunch of data in one of YT internal representations
//! (i.e. unversioned value or arbitrary yson string), you may feed it to the converter
//! and flush result as a ClickHouse column.
//!
//! Second is less obvious: even if you do not care about actual conversion, but rather
//! interested in performing data type conversion (TLogicalTypePtr -> DB::DataTypePtr),
//! you may instantiate this class and access GetDataType().
class TYTToCHColumnConverter
{
public:
    //! `isReadConversion` is used to maintain type compatibility,
    //! since some YT types are not represented in CH,
    //! and some are mapped to a single type at all.
    //!
    //! YT Timestamp and Timestamp64 are mapped to CH DateTime64,
    //! so when writing we need to convert Timestamp -> YtTimestamp,
    //! which is a custom type over DateTime64.
    //!
    //! Example of read-only conversions:
    //! - optional<T> -> T' when Nullable(T') is not available in CH;
    TYTToCHColumnConverter(
        NTableClient::TComplexTypeFieldDescriptor descriptor,
        TCompositeSettingsPtr settings,
        bool isLowCardinality,
        bool isReadConversion = true);

    TYTToCHColumnConverter(TYTToCHColumnConverter&& other);

    TYTToCHColumnConverter(const TYTToCHColumnConverter& other) = delete;

    ~TYTToCHColumnConverter();

    DB::DataTypePtr GetDataType() const;

    //! Init underlying CH-column.
    //! Should be called before any Consume* method.
    void InitColumn();

    //! Returns converted CH column.
    //! No Consume* method may be called after flush till the converter is
    //! reinitialized with InitColumn again.
    DB::ColumnPtr FlushColumn();

    //! Consume a range of values represented by YT unversioned values.
    //! Each unversioned value may be Any/Composite as well as of non-YSON kind or even Null.
    //! This method intentionally deals with value batches to reduce overhead from virtual calls.
    void ConsumeUnversionedValues(NTableClient::TUnversionedValueRange value);

    //! Consume single value represented by YSON string. Similar to previous
    //! in case when YSON value appears not from unversioned value.
    //! It is pretty hard to reduce the number of virtual calls for composite values,
    //! so this method is present in singular form.
    void ConsumeYson(NYson::TYsonStringBuf yson);

    //! Consume nulls. Useful for situations like when value is missing
    //! in the unversioned row or when the whole column in columnar batch is missing.
    void ConsumeNulls(int count);

    //! Consume native YT column taken from columnar row batch.
    //! If this method is called, it should be the only Consume* method ever called to this
    //! instance of converter.
    void ConsumeYtColumn(const NTableClient::IUnversionedColumnarRowBatch::TColumn& column, TRange<DB::UInt8> filter = {});

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
