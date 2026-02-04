#pragma once

#include "private.h"

#include <yt/yt/client/table_client/public.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class TYTToCHBlockConverter
{
public:
    TYTToCHBlockConverter(
        const std::vector<NTableClient::TColumnSchema>& columnSchemas,
        const std::vector<NYTree::IAttributeDictionaryPtr>& columnAttributes,
        const NTableClient::TNameTablePtr& nameTable,
        const TCompositeSettingsPtr& compositeSettings,
        bool optimizeDistinctRead);

    TYTToCHBlockConverter(TYTToCHBlockConverter&& other);

    TYTToCHBlockConverter(const TYTToCHBlockConverter& other) = delete;

    ~TYTToCHBlockConverter();

    const DB::Block& GetHeaderBlock() const;

    //! Convert required columns from YT batch to a CH block.
    //! A filter hint may be provided to indicate which rows will be filtered out
    //! after the conversion. The size of the filter should match the number of
    //! rows in the batch. Rows that would be filtered out (indicated by 0 in the
    //! filter hint) may be filled with a default value to avoid complex type
    //! conversions or copying large string values.
    DB::Block Convert(
        const NTableClient::IUnversionedRowBatchPtr& batch,
        TRange<DB::UInt8> filterHint = {});

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
