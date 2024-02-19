#pragma once

#include <yt/yt/client/api/dynamic_table_transaction.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

TSharedRange<TRowModification> GetFullSyncIndexModifications(
    const TSharedRange<TRowModification>& modifications,
    const TSharedRange<NTableClient::TUnversionedRow>& lookedUpRows,
    const NTableClient::TNameTableToSchemaIdMapping& writeIdMapping,
    const NTableClient::TNameTableToSchemaIdMapping& deleteIdMapping,
    const NTableClient::TTableSchema& indexSchema,
    const std::optional<NTableClient::TUnversionedValue>& empty);

TSharedRange<TRowModification> GetUnfoldedIndexModifications(
    const TSharedRange<TRowModification>& modifications,
    const TSharedRange<NTableClient::TUnversionedRow>& lookedUpRows,
    const NTableClient::TNameTableToSchemaIdMapping& writeIdMapping,
    const NTableClient::TNameTableToSchemaIdMapping& deleteIdMapping,
    const NTableClient::TTableSchema& indexSchema,
    const std::optional<NTableClient::TUnversionedValue>& empty,
    int unfoldedKeyPosition);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
