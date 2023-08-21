#pragma once

#include "private.h"

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/row_batch.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

// Note: in normal situation you should not use this class directly. Use one of the
// helpers from conversion.h.

//! This class serves for two purposes.
//!
//! First is obvious: if you have a bunch of data in one of YT internal representations
//! (i.e. unversioned value or arbitrary yson string), you may feed it to the converter
//! and flush result as a ClickHouse column.
//!
//! Second is less obvious: even if you do not care about actual conversion, but rather
//! interested in performing data type conversion (TLogicalTypePtr -> DB::DataTypePtr),
//! you may instantiate this class and access GetDataType().
class TYTCHConverter
{
public:
    //! `enableReadOnlyConversions` option enables read-only compatibility conversion options:
    //! - optional<T> -> T' when Nullable(T') is not available in CH;
    //! - dict<K,V> -> List(Tuple(K',V'));
    TYTCHConverter(
        NTableClient::TComplexTypeFieldDescriptor descriptor,
        TCompositeSettingsPtr settings,
        bool enableReadOnlyConversions = true);

    TYTCHConverter(TYTCHConverter&& other);

    ~TYTCHConverter();

    DB::ColumnPtr FlushColumn();

    DB::DataTypePtr GetDataType() const;

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
    void ConsumeYtColumn(const NTableClient::IUnversionedColumnarRowBatch::TColumn& column);

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
