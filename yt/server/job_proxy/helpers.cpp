#include "stdafx.h"

#include "helpers.h"
#include "private.h"

#include <ytlib/new_table_client/schemaless_chunk_reader.h>
#include <ytlib/new_table_client/schemaless_writer.h>
#include <ytlib/new_table_client/name_table.h>

#include <core/yson/consumer.h>

#include <core/ytree/fluent.h>

#include <core/concurrency/scheduler.h>

namespace NYT {
namespace NJobProxy {

using namespace NFormats;
using namespace NVersionedTableClient;
using namespace NConcurrency;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const size_t BufferSize = 1024 * 1024;
static const int BufferRowCount = 1024;

////////////////////////////////////////////////////////////////////////////////

TContextPreservingInput::TContextPreservingInput(
    ISchemalessMultiChunkReaderPtr reader,
    const TFormat& format,
    bool enableTableSwitch,
    bool enableKeySwitch)
    : Reader_(reader)
{
    CurrentBuffer_.Reserve(BufferSize);
    PreviousBuffer_.Reserve(BufferSize);

    auto consumer = CreateConsumerForFormat(format, EDataType::Tabular, &CurrentBuffer_);

    Consumer_ = consumer.get();
    Writer_ = CreateSchemalessWriterAdapter(
        std::move(consumer),
        Reader_->GetNameTable(),
        enableTableSwitch,
        enableKeySwitch,
        Reader_->GetKeyColumns().size());
}

void TContextPreservingInput::PipeReaderToOutput(TOutputStream* outputStream)
{
    std::vector<TUnversionedRow> rows;
    rows.reserve(BufferRowCount);
    while (Reader_->Read(&rows)) {
        if (rows.empty()) {
            WaitFor(Reader_->GetReadyEvent()).ThrowOnError();
            continue;
        }

        Writer_->SetTableIndex(Reader_->GetTableIndex());
        WriteRows(rows, outputStream);
    }

    outputStream->Write(CurrentBuffer_.Begin(), CurrentBuffer_.Size());
    outputStream->Finish();

    auto asyncError = Writer_->Close();
    YCHECK(asyncError.IsSet());
    asyncError.Get().ThrowOnError();
}

void TContextPreservingInput::WriteRows(const std::vector<TUnversionedRow>& rows, TOutputStream* outputStream)
{
    std::vector<TUnversionedRow> oneRow(1, TUnversionedRow());
    for (auto row : rows) {
        oneRow[0] = row;

        if (!Writer_->Write(oneRow)) {
            WaitFor(Writer_->GetReadyEvent())
                .ThrowOnError();
        }

        if (CurrentBuffer_.Size() < BufferSize) {
            continue;
        }

        outputStream->Write(CurrentBuffer_.Begin(), CurrentBuffer_.Size());

        swap(CurrentBuffer_, PreviousBuffer_);
        CurrentBuffer_.Clear();
    }
}

TBlob TContextPreservingInput::GetContext() const
{
    TBlob result;
    result.Append(TRef::FromBlob(PreviousBuffer_.Blob()));
    result.Append(TRef::FromBlob(CurrentBuffer_.Blob()));
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NJobProxy
