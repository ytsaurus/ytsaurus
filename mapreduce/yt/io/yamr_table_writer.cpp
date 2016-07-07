#include "yamr_table_writer.h"

#include "proxy_output.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TYaMRTableWriter::TYaMRTableWriter(THolder<TProxyOutput> output)
    : Output_(std::move(output))
    , Locks_(Output_->GetStreamCount())
{ }

TYaMRTableWriter::~TYaMRTableWriter()
{ }

void TYaMRTableWriter::AddRow(const TYaMRRow& row, size_t tableIndex)
{
    TOutputStream* stream = Output_->GetStream(tableIndex);

    auto writeField = [&stream] (const TStringBuf& field) {
        i32 length = static_cast<i32>(field.length());
        stream->Write(&length, sizeof(length));
        stream->Write(field.data(), field.length());
    };

    auto guard = Guard(Locks_[tableIndex]);

    writeField(row.Key);
    writeField(row.SubKey);
    writeField(row.Value);
    Output_->OnRowFinished(tableIndex);
}

void TYaMRTableWriter::Finish()
{
    for (size_t i = 0; i < Output_->GetStreamCount(); ++i) {
        auto guard = Guard(Locks_[i]);
        Output_->GetStream(i)->Finish();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
