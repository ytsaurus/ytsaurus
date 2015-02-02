#include "stdafx.h"

#include "helpers.h"

#include <ytlib/table_client/sync_reader.h>
#include <ytlib/table_client/table_producer.h>

#include <core/yson/consumer.h>

namespace NYT {
namespace NJobProxy {

using namespace NFormats;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

static const int BufferSize = 1024 * 1024;

TContextPreservingInput::TContextPreservingInput(
    ISyncReaderPtr reader, 
    const TFormat& format, 
    bool enableTableSwitch)
    : Reader_(reader)
    , Format_(format)
    , EnableTableSwitch_(enableTableSwitch) 
{
    CurrentBuffer_.Reserve(BufferSize);
    PreviousBuffer_.Reserve(BufferSize);
}

void TContextPreservingInput::PipeReaderToOutput(TOutputStream* outputStream)
{
    auto consumer = CreateConsumerForFormat(
        Format_,
        EDataType::Tabular,
        &CurrentBuffer_);

    TTableProducer producer(Reader_, consumer.get(), EnableTableSwitch_);
    while (producer.ProduceRow()) {
        if (CurrentBuffer_.Size() < BufferSize) {
            continue;
        }

        outputStream->Write(CurrentBuffer_.Begin(), CurrentBuffer_.Size());

        swap(CurrentBuffer_, PreviousBuffer_);
        CurrentBuffer_.Clear();
    }

    outputStream->Write(CurrentBuffer_.Begin(), CurrentBuffer_.Size());
    outputStream->Finish();
}

TBlob TContextPreservingInput::GetContext() const
{
    TBlob result;
    result.Append(TRef::FromBlob(PreviousBuffer_.Blob()));
    result.Append(TRef::FromBlob(CurrentBuffer_.Blob()));
    return result;
}

////////////////////////////////////////////////////////////////////////////////

void PipeInputToOutput(TInputStream* input, TOutputStream* output)
{
    struct TInputToOutputBufferTag {};
    TBlob buffer(TInputToOutputBufferTag(), BufferSize, false);

    while (true) {
        auto len = input->Read(buffer.Begin(), BufferSize);
        if (len == 0) {
            break;
        }

        output->Write(buffer.Begin(), len);
    }

    output->Finish();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NJobProxy
