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
    , EnableTableSwitch_(enableTableSwitch)
    , TableIndex_(-1)
    , EnableKeySwitch_(enableKeySwitch)
{
    CurrentBuffer_.Reserve(BufferSize);
    PreviousBuffer_.Reserve(BufferSize);

    auto consumer = CreateConsumerForFormat(format, EDataType::Tabular, &CurrentBuffer_);

    Consumer_ = consumer.get();
    Writer_ = CreateSchemalessWriterAdapter(
        std::move(consumer),
        Reader_->GetNameTable());

    if (EnableKeySwitch_) {
        KeyColumnCount_ = Reader_->GetKeyColumns().size();
    }
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

        if (EnableTableSwitch_ && TableIndex_ != Reader_->GetTableIndex()) {
            TableIndex_ = Reader_->GetTableIndex();
            BuildYsonListFluently(Consumer_).Item()
                .BeginAttributes()
                    .Item("table_index").Value(TableIndex_)
                .EndAttributes()
                .Entity();
        }

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

        if (EnableKeySwitch_) {
            if (CurrentKey_ && CompareRows(row, CurrentKey_, KeyColumnCount_)) {
                BuildYsonListFluently(Consumer_).Item()
                    .BeginAttributes()
                        .Item("key_switch").Value(true)
                    .EndAttributes()
                    .Entity();
            }
            CurrentKey_ = row;
        }

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

    if (EnableKeySwitch_ && CurrentKey_) {
        LastKey_ = GetKeyPrefix(CurrentKey_, KeyColumnCount_);
        CurrentKey_ = LastKey_.Get();
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
