#pragma once

#include "public.h"

#include <yt/client/table_client/versioned_row.h>
#include <yt/client/table_client/unversioned_row.h>

#include <yt/ytlib/api/native/public.h>

#include <yt/ytlib/formats/format.h>

#include <yt/ytlib/chunk_client/chunk_owner_ypath_proxy.h>
#include <yt/ytlib/chunk_client/chunk_spec.h>

#include <yt/ytlib/cypress_client/public.h>

#include <yt/ytlib/object_client/public.h>

#include <yt/ytlib/scheduler/public.h>

#include <yt/core/yson/lexer.h>
#include <yt/core/yson/public.h>

#include <yt/core/compression/public.h>

#include <yt/core/erasure/public.h>

#include <yt/core/misc/phoenix.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TTableOutput
    : public IOutputStream
{
public:
    TTableOutput(const NFormats::TFormat& format, NYson::IYsonConsumer* consumer);

    explicit TTableOutput(std::unique_ptr<NFormats::IParser> parser);

    ~TTableOutput();

private:
    void DoWrite(const void* buf, size_t len);
    void DoFinish();

    const std::unique_ptr<NFormats::IParser> Parser_;

    bool IsParserValid_ = true;
};

////////////////////////////////////////////////////////////////////////////////

struct TPipeReaderToWriterOptions
{
    int BufferRowCount = 0;
    bool ValidateValues = false;
    NConcurrency::IThroughputThrottlerPtr Throttler = nullptr;
    // Used only for testing.
    TDuration PipeDelay;
};

void PipeReaderToWriter(
    ISchemalessReaderPtr reader,
    ISchemalessWriterPtr writer,
    TPipeReaderToWriterOptions options);

void PipeInputToOutput(
    IInputStream* input,
    IOutputStream* output,
    i64 bufferBlockSize);

void PipeInputToOutput(
    NConcurrency::IAsyncInputStreamPtr input,
    IOutputStream* output,
    i64 bufferBlockSize);


////////////////////////////////////////////////////////////////////////////////

// NB: not using TYsonString here to avoid copying.
TUnversionedValue MakeUnversionedValue(
    TStringBuf ysonString, int id,
    NYson::TStatelessLexer& lexer);

////////////////////////////////////////////////////////////////////////////////

void ValidateKeyColumns(const TKeyColumns& keyColumns, const TKeyColumns& chunkKeyColumns, bool requireUniqueKeys);
TColumnFilter CreateColumnFilter(const TNullable<std::vector<TString>>& columns, TNameTablePtr nameTable);
int GetSystemColumnCount(TChunkReaderOptionsPtr options);

////////////////////////////////////////////////////////////////////////////////

struct TTableUploadOptions
{
    NChunkClient::EUpdateMode UpdateMode;
    NCypressClient::ELockMode LockMode;
    TTableSchema TableSchema;
    ETableSchemaMode SchemaMode;
    EOptimizeFor OptimizeFor;
    NCompression::ECodec CompressionCodec;
    NErasure::ECodec ErasureCodec;

    void Persist(NPhoenix::TPersistenceContext& context);
};

TTableUploadOptions GetTableUploadOptions(
    const NYPath::TRichYPath& path,
    const NYTree::IAttributeDictionary& cypressTableAttributes,
    i64 rowCount);

////////////////////////////////////////////////////////////////////////////////

// Mostly used in unittests and for debugging purposes.
TUnversionedOwningRow YsonToSchemafulRow(
    const TString& yson,
    const TTableSchema& tableSchema,
    bool treatMissingAsNull);
TUnversionedOwningRow YsonToSchemalessRow(
    const TString& yson);
TVersionedRow YsonToVersionedRow(
    const TRowBufferPtr& rowBuffer,
    const TString& keyYson,
    const TString& valueYson,
    const std::vector<TTimestamp>& deleteTimestamps = std::vector<TTimestamp>(),
    const std::vector<TTimestamp>& extraWriteTimestamps = std::vector<TTimestamp>());
TUnversionedOwningRow YsonToKey(const TString& yson);
TString KeyToYson(TUnversionedRow row);

////////////////////////////////////////////////////////////////////////////////

NScheduler::NProto::TOutputResult GetWrittenChunksBoundaryKeys(
    ISchemalessMultiChunkWriterPtr writer);

std::pair<TOwningKey, TOwningKey> GetChunkBoundaryKeys(
    const NChunkClient::NProto::TChunkMeta& chunkMeta,
    int keyColumnCount);

////////////////////////////////////////////////////////////////////////////////

void ValidateDynamicTableTimestamp(
    const NYPath::TRichYPath& path,
    bool dynamic,
    const TTableSchema& schema,
    const NYTree::IAttributeDictionary& attributes);

////////////////////////////////////////////////////////////////////////////////

std::vector<NChunkClient::TInputChunkPtr> CollectTableInputChunks(
    const NYPath::TRichYPath& path,
    const NApi::NNative::IClientPtr& client,
    const NNodeTrackerClient::TNodeDirectoryPtr& nodeDirectory,
    const NChunkClient::TFetchChunkSpecConfigPtr& config,
    const NObjectClient::TTransactionId& transactionId,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

//! Helpers for updating columnar statistics with versioned and unversioned rows.
void UpdateColumnarStatistics(NProto::TColumnarStatisticsExt& columnarStatisticsExt, const TUnversionedRow& row);
void UpdateColumnarStatistics(NProto::TColumnarStatisticsExt& columnarStatisticsExt, const TVersionedRow& row);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

#define HELPERS_INL_H_
#include "helpers-inl.h"
#undef HELPERS_INL_H_
