#include "ydl_table_reader.h"
#include "ydl_helpers.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TNodeYdlTableReader::TNodeYdlTableReader(
    ::TIntrusivePtr<TRawTableReader> input,
    TVector<ui64> hashes)
    : NodeReader_(new TNodeTableReader(std::move(input)))
    , TypeHashes_(std::move(hashes))
{ }

const TNode& TNodeYdlTableReader::GetRow() const
{
    return NodeReader_->GetRow();
}

void TNodeYdlTableReader::VerifyRowType(ui64 rowTypeHash) const
{
    ValidateYdlTypeHash(rowTypeHash, GetTableIndex(), TypeHashes_, true);
}

bool TNodeYdlTableReader::IsValid() const
{
    return NodeReader_->IsValid();
}

void TNodeYdlTableReader::Next()
{
    NodeReader_->Next();
}

ui32 TNodeYdlTableReader::GetTableIndex() const
{
    return NodeReader_->GetTableIndex();
}

ui32 TNodeYdlTableReader::GetRangeIndex() const
{
    return NodeReader_->GetRangeIndex();
}

ui64 TNodeYdlTableReader::GetRowIndex() const
{
    return NodeReader_->GetRowIndex();
}

void TNodeYdlTableReader::NextKey()
{
    NodeReader_->NextKey();
}

TMaybe<size_t> TNodeYdlTableReader::GetReadByteCount() const
{
    return NodeReader_->GetReadByteCount();
}

bool TNodeYdlTableReader::IsEndOfStream() const {
    return NodeReader_->IsEndOfStream();
}

bool TNodeYdlTableReader::IsRawReaderExhausted() const {
    return NodeReader_->IsRawReaderExhausted();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
