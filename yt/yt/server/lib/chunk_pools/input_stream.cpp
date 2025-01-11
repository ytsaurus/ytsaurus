#include "input_stream.h"

#include <library/cpp/iterator/functools.h>

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

TInputStreamDescriptor::TInputStreamDescriptor(bool isTeleportable, bool isPrimary, bool isVersioned)
    : IsTeleportable_(isTeleportable)
    , IsPrimary_(isPrimary)
    , IsVersioned_(isVersioned)
{ }

bool TInputStreamDescriptor::IsTeleportable() const
{
    return IsTeleportable_;
}

bool TInputStreamDescriptor::IsForeign() const
{
    return !IsPrimary_;
}

bool TInputStreamDescriptor::IsPrimary() const
{
    return IsPrimary_;
}

bool TInputStreamDescriptor::IsVersioned() const
{
    return IsVersioned_;
}

bool TInputStreamDescriptor::IsUnversioned() const
{
    return !IsVersioned_;
}

void TInputStreamDescriptor::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, IsTeleportable_);
    PHOENIX_REGISTER_FIELD(2, IsPrimary_);
    PHOENIX_REGISTER_FIELD(3, IsVersioned_);
    PHOENIX_REGISTER_FIELD(4, TableIndex_);
    PHOENIX_REGISTER_FIELD(5, RangeIndex_);
}

void FormatValue(TStringBuilderBase* builder, const TInputStreamDescriptor& descriptor, TStringBuf /*spec*/)
{
    Format(
        builder,
        "{Teleportable: %v, Primary: %v, Versioned: %v, TableIndex: %v, RangeIndex: %v}",
        descriptor.IsTeleportable(),
        descriptor.IsPrimary(),
        descriptor.IsVersioned(),
        descriptor.GetTableIndex(),
        descriptor.GetRangeIndex());
}

PHOENIX_DEFINE_TYPE(TInputStreamDescriptor);

////////////////////////////////////////////////////////////////////////////////

TInputStreamDescriptor IntermediateInputStreamDescriptor(false /*isTeleportable*/, true /*isPrimary*/, false /*isVersioned*/);
TInputStreamDescriptor TeleportableIntermediateInputStreamDescriptor(true /*isTeleportable*/, true /*isPrimary*/, false /*isVersioned*/);

////////////////////////////////////////////////////////////////////////////////

TInputStreamDirectory::TInputStreamDirectory(
    std::vector<TInputStreamDescriptor> descriptors,
    TInputStreamDescriptor defaultDescriptor)
    : Descriptors_(std::move(descriptors))
    , DefaultDescriptor_(defaultDescriptor)
{
    YT_VERIFY(DefaultDescriptor_.IsPrimary());

    for (const auto& [inputStreamIndex, descriptor] : Enumerate(Descriptors_)) {
        if (descriptor.GetTableIndex() && descriptor.GetRangeIndex()) {
            auto [_, inserted] = TableAndRangeIndicesToInputStreamIndex_.insert({{*descriptor.GetTableIndex(), *descriptor.GetRangeIndex()}, inputStreamIndex});
            YT_VERIFY(inserted);
        }
    }
}

const TInputStreamDescriptor& TInputStreamDirectory::GetDescriptor(int inputStreamIndex) const
{
    if (0 <= inputStreamIndex && inputStreamIndex < std::ssize(Descriptors_)) {
        return Descriptors_[inputStreamIndex];
    } else {
        return DefaultDescriptor_;
    }
}

int TInputStreamDirectory::GetDescriptorCount() const
{
    return Descriptors_.size();
}

int TInputStreamDirectory::GetInputStreamIndex(int tableIndex, int rangeIndex) const
{
    auto it = TableAndRangeIndicesToInputStreamIndex_.find(std::pair(tableIndex, rangeIndex));
    YT_VERIFY(it != TableAndRangeIndicesToInputStreamIndex_.end());
    return it->second;
}

void TInputStreamDirectory::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, Descriptors_);
    PHOENIX_REGISTER_FIELD(2, DefaultDescriptor_);
}

PHOENIX_DEFINE_TYPE(TInputStreamDirectory);

////////////////////////////////////////////////////////////////////////////////

TInputStreamDirectory IntermediateInputStreamDirectory({}, IntermediateInputStreamDescriptor);
TInputStreamDirectory TeleportableIntermediateInputStreamDirectory({}, TeleportableIntermediateInputStreamDescriptor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
