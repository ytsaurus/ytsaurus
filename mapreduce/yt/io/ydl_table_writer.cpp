#include "ydl_table_writer.h"
#include "ydl_helpers.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TNodeYdlTableWriter::TNodeYdlTableWriter(THolder<TProxyOutput> output, TVector<ui64> hashes)
    : NodeWriter_(new TNodeTableWriter(std::move(output)))
    , TypeHashes_(std::move(hashes))
{ }

size_t TNodeYdlTableWriter::GetTableCount() const
{
    return NodeWriter_->GetTableCount();
}

void TNodeYdlTableWriter::FinishTable(size_t tableIndex)
{
    NodeWriter_->FinishTable(tableIndex);
}

void TNodeYdlTableWriter::AddRow(const TNode& row, size_t tableIndex)
{
    NodeWriter_->AddRow(row, tableIndex);
}

void TNodeYdlTableWriter::AddRow(TNode&& row, size_t tableIndex)
{
    NodeWriter_->AddRow(std::move(row), tableIndex);
}

void TNodeYdlTableWriter::VerifyRowType(ui64 rowTypeHash, size_t tableIndex) const
{
    ValidateYdlTypeHash(rowTypeHash, tableIndex, TypeHashes_, false);
}

void TNodeYdlTableWriter::Abort()
{
    NodeWriter_->Abort();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
