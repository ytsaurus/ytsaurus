#include "yamr_table_writer.h"

#include "proxy_output.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TYaMRTableWriter::TYaMRTableWriter(THolder<TProxyOutput> output)
    : Output_(MoveArg(output))
{ }

TYaMRTableWriter::~TYaMRTableWriter()
{ }

void TYaMRTableWriter::AddRow(const TYaMRRow& row, size_t tableIndex)
{
    TOutputStream* stream = Output_->GetStream(tableIndex);
    WriteField(row.Key, stream);
    WriteField(row.SubKey, stream);
    WriteField(row.Value, stream);
    Output_->OnRowFinished(tableIndex);
}

void TYaMRTableWriter::Finish()
{
    for (size_t i = 0; i < Output_->GetStreamCount(); ++i) {
        Output_->GetStream(i)->Finish();
    }
}

void TYaMRTableWriter::WriteField(const Stroka& field, TOutputStream* stream)
{
    i32 length = field.size();
    stream->Write(&length, sizeof(length));
    stream->Write(field.begin(), length);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
