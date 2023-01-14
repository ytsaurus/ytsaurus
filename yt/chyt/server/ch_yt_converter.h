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
//! First is obvious: if you have a bunch of data represented as a CH column, you may
//! feed it to the converter and flush result as a range of unversioned values.
//! If resulting type is string-like, string data is owned either by converter or by
//! the original column.
//!
//! Second is less obvious: even if you do not care about actual conversion, but rather
//! interested in performing data type conversion (DB::DataTypePtr -> TLogicalTypePtr),
//! you may instantiate this class and access GetLogicalType().
class TCHYTConverter
{
public:
    TCHYTConverter(
        DB::DataTypePtr dataType,
        TCompositeSettingsPtr settings);

    TCHYTConverter(TCHYTConverter&& other);

    ~TCHYTConverter();

    //! Convert CH column to range of unversioned values. All values will have
    //! id = 0. Values are valid until next call of this method.
    NTableClient::TUnversionedValueRange ConvertColumnToUnversionedValues(
        const DB::ColumnPtr& column);

    NTableClient::TLogicalTypePtr GetLogicalType() const;

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
