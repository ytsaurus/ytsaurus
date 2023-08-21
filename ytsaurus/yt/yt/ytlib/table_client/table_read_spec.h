#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/data_slice_descriptor.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/client/chunk_client/config.h>

#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/client/ypath/rich.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

//! Helper struct for #CreateAppropriateSchemalessMultiChunkReader.
struct TTableReadSpec
{
    NChunkClient::TDataSourceDirectoryPtr DataSourceDirectory;
    // Data source indices (aka table indices) in data slices should match
    // data sources in data source directory (e.g. if data source directory consists
    // of single data source, all data source indices should be zero).
    std::vector<NChunkClient::TDataSliceDescriptor> DataSliceDescriptors;
};

////////////////////////////////////////////////////////////////////////////////

struct TFetchSingleTableReadSpecOptions
{
    // Required arguments.
    NYPath::TRichYPath RichPath;
    NApi::NNative::IClientPtr Client;

    // Optional arguments.
    NTransactionClient::TTransactionId TransactionId;
    NChunkClient::TReadSessionId ReadSessionId = NChunkClient::TReadSessionId::Create();
    NChunkClient::TGetUserObjectBasicAttributesOptions GetUserObjectBasicAttributesOptions;
    NChunkClient::TFetchChunkSpecConfigPtr FetchChunkSpecConfig = New<NChunkClient::TFetchChunkSpecConfig>();
    bool FetchParityReplicas = true;
    EUnavailableChunkStrategy UnavailableChunkStrategy = EUnavailableChunkStrategy::ThrowError;
    NChunkClient::EChunkAvailabilityPolicy ChunkAvailabilityPolicy = NChunkClient::EChunkAvailabilityPolicy::DataPartsAvailable;

    bool FetchFromTablets = false;
};

//! Helper for fetching single table identified by TRichYPath.
//! By the moment this comment is written, it is used in NApi::TTableReader
//! and in CHYT YT-based external dictionaries.
TTableReadSpec FetchSingleTableReadSpec(const TFetchSingleTableReadSpecOptions& options);

////////////////////////////////////////////////////////////////////////////////

//! Join several table read specs together forming single table read spec
//! with proper data source renumeration.
TTableReadSpec JoinTableReadSpecs(std::vector<TTableReadSpec>& tableReadSpecs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
