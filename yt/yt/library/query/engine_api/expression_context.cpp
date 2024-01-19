#include "expression_context.h"

using namespace NYT::NWebAssembly;

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

TExpressionContext::TExpressionContext()
    : RowBuffer_(New<TRowBuffer>())
{ }

TExpressionContext::TExpressionContext(TRowBufferPtr rowBuffer)
    : RowBuffer_(std::move(rowBuffer))
{ }

TExpressionContext::TExpressionContext(TExpressionContext&& other)
{
    std::swap(RowBuffer_, other.RowBuffer_);
}

TExpressionContext& TExpressionContext::operator=(TExpressionContext&& other)
{
    std::swap(RowBuffer_, other.RowBuffer_);
    return *this;
}

void TExpressionContext::Clear()
{
    RowBuffer_->Clear();
}

i64 TExpressionContext::GetSize() const
{
    return RowBuffer_->GetSize();
}

i64 TExpressionContext::GetCapacity() const
{
    return RowBuffer_->GetCapacity();
}

char* TExpressionContext::AllocateUnaligned(size_t byteCount)
{
    return RowBuffer_->GetPool()->AllocateUnaligned(byteCount);
}

char* TExpressionContext::AllocateAligned(size_t byteCount)
{
    return RowBuffer_->GetPool()->AllocateAligned(byteCount);
}

NTableClient::TMutableUnversionedRow TExpressionContext::AllocateUnversioned(int valueCount)
{
    return RowBuffer_->AllocateUnversioned(valueCount);
}

TRowBufferPtr TExpressionContext::GetRowBuffer() const
{
    return RowBuffer_;
}

NTableClient::TMutableUnversionedRow TExpressionContext::CaptureRow(
    NTableClient::TUnversionedRow row,
    bool captureValues)
{
    return RowBuffer_->CaptureRow(row, captureValues);
}

NTableClient::TMutableUnversionedRow TExpressionContext::CaptureRow(
    NTableClient::TUnversionedValueRange values,
    bool captureValues)
{
    return RowBuffer_->CaptureRow(values, captureValues);
}

void TExpressionContext::CaptureValues(NTableClient::TMutableUnversionedRow row)
{
    return RowBuffer_->CaptureValues(row);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
