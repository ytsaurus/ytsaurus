#include "yamr_table_writer.h"

#include "proxy_output.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TYaMRTableWriter::TYaMRTableWriter(THolder<TProxyOutput> output)
    : Output_(std::move(output))
{ }

TYaMRTableWriter::~TYaMRTableWriter()
{ }

size_t TYaMRTableWriter::GetStreamCount() const
{
    return Output_->GetStreamCount();
}

IOutputStream* TYaMRTableWriter::GetStream(size_t tableIndex) const
{
    return Output_->GetStream(tableIndex);
}

void TYaMRTableWriter::AddRow(const TYaMRRow& row, size_t tableIndex)
{
    auto* stream = GetStream(tableIndex);

    auto writeField = [&stream] (const TStringBuf& field) {
        i32 length = static_cast<i32>(field.length());
        stream->Write(&length, sizeof(length));
        stream->Write(field.data(), field.length());
    };

    writeField(row.Key);
    writeField(row.SubKey);
    writeField(row.Value);

    Output_->OnRowFinished(tableIndex);
}

void TYaMRTableWriter::Abort()
{
    Output_->Abort();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
