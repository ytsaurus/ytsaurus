#pragma once

#include "public.h"

#include <yt/ytlib/formats/format.h>
#include <yt/ytlib/chunk_client/chunk_owner_ypath_proxy.h>
#include <yt/ytlib/cypress_client/public.h>

#include <yt/core/yson/lexer.h>
#include <yt/core/yson/public.h>
#include <yt/core/misc/phoenix.h>

namespace NYT {

namespace NScheduler {
namespace NProto {

class TOutputResult;

} // namespace NProto
} // namespace NScheduler

namespace NTableClient {


//////////////////////////////////////////////////////////////////////////////////

class TTableOutput
    : public TOutputStream
{
public:
    TTableOutput(const NFormats::TFormat& format, NYson::IYsonConsumer* consumer);

    explicit TTableOutput(std::unique_ptr<NFormats::IParser> parser);

    ~TTableOutput() throw();

private:
    void DoWrite(const void* buf, size_t len);
    void DoFinish();

    const std::unique_ptr<NFormats::IParser> Parser_;

    bool IsParserValid_ = true;
};

//////////////////////////////////////////////////////////////////////////////////

void PipeReaderToWriter(
    ISchemalessReaderPtr reader,
    ISchemalessWriterPtr writer,
    int bufferRowCount,
    bool validateValues = false,
    NConcurrency::IThroughputThrottlerPtr throttler = nullptr);

void PipeInputToOutput(
    TInputStream* input,
    TOutputStream* output,
    i64 bufferBlockSize);

void PipeInputToOutput(
    NConcurrency::IAsyncInputStreamPtr input,
    TOutputStream* output,
    i64 bufferBlockSize);


//////////////////////////////////////////////////////////////////////////////////

// NB: not using TYsonString here to avoid copying.
TUnversionedValue MakeUnversionedValue(
    const TStringBuf& ysonString, int id,
    NYson::TStatelessLexer& lexer);

//////////////////////////////////////////////////////////////////////////////////

void ValidateKeyColumns(const TKeyColumns& keyColumns, const TKeyColumns& chunkKeyColumns, bool requireUniqueKeys);
TColumnFilter CreateColumnFilter(const NChunkClient::TChannel& protoChannel, TNameTablePtr nameTable);
int GetSystemColumnCount(TChunkReaderOptionsPtr options);

//////////////////////////////////////////////////////////////////////////////////

struct TTableUploadOptions
{
    NChunkClient::EUpdateMode UpdateMode;
    NCypressClient::ELockMode LockMode;
    TTableSchema TableSchema;
    ETableSchemaMode SchemaMode;

    void Persist(NPhoenix::TPersistenceContext& context);
};

TTableUploadOptions GetTableUploadOptions(
    const NYPath::TRichYPath& path,
    const TTableSchema& schema,
    ETableSchemaMode schemaMode,
    i64 rowCount);

//////////////////////////////////////////////////////////////////////////////////

// Mostly used in unittests and for debugging purposes.
TUnversionedOwningRow YsonToRow(
    const Stroka& yson,
    const TTableSchema& tableSchema,
    bool treatMissingAsNull);
TUnversionedOwningRow YsonToKey(const Stroka& yson);
Stroka KeyToYson(TUnversionedRow row);

//////////////////////////////////////////////////////////////////////////////////

NScheduler::NProto::TOutputResult GetWrittenChunksBoundaryKeys(ISchemalessMultiChunkWriterPtr writer);

//////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NTableClient

#define HELPERS_INL_H_
#include "helpers-inl.h"
#undef HELPERS_INL_H_
