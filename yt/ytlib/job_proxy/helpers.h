#pragma once

#include <yt/ytlib/table_client/schemaful_reader_adapter.h>
#include <yt/ytlib/table_client/schemaful_writer_adapter.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NScheduler {
namespace NProto {

class TQuerySpec;

} // namespace NProto
} // namespace NScheduler

////////////////////////////////////////////////////////////////////////////////

namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

void RunQuery(
    const NScheduler::NProto::TQuerySpec& querySpec,
    const NTableClient::TSchemalessReaderFactory& readerFactory,
    const NTableClient::TSchemalessWriterFactory& writerFactory,
    const TNullable<Stroka>& udfDirectory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
