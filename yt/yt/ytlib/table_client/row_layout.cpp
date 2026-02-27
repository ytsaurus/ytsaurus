#include "row_layout.h"

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////


TUnversionedRowLayout::TUnversionedRowLayout(
    const TKeyWideningOptions& keyWideningOptions,
    int totalColumnCount)
{
    if (keyWideningOptions.InsertPosition != -1) {
        KeyWideningStart_ = keyWideningOptions.InsertPosition;
        KeyWideningEnd_ = KeyWideningStart_ + std::ssize(keyWideningOptions.InsertedColumnIds);
    }

    ExtraValuesOffset_ = totalColumnCount;
}

bool TUnversionedRowLayout::IsPrefixSchemaful() const
{
    return KeyWideningStart_ == KeyWideningEnd_;
}

ENameTableAffinity TUnversionedRowLayout::GetNameTableAffinity(int columnIndex) const
{
    bool isInsideWidening = KeyWideningStart_ <= columnIndex && columnIndex < KeyWideningEnd_;
    bool isExtra = columnIndex >= ExtraValuesOffset_;
    return isInsideWidening || isExtra
        ? ENameTableAffinity::Reader
        : ENameTableAffinity::Chunk;
}

std::pair<int, int> TUnversionedRowLayout::GetKeyWideningRange() const
{
    return {KeyWideningStart_, KeyWideningEnd_};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
