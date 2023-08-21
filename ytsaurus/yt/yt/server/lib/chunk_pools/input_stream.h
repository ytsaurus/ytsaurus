#pragma once

#include "private.h"

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

class TInputStreamDescriptor
{
public:
    DEFINE_BYVAL_RW_PROPERTY(std::optional<int>, TableIndex);
    DEFINE_BYVAL_RW_PROPERTY(std::optional<int>, RangeIndex);

public:
    //! Used only for persistence.
    TInputStreamDescriptor() = default;
    TInputStreamDescriptor(bool isTeleportable, bool isPrimary, bool isVersioned);

    bool IsTeleportable() const;
    bool IsPrimary() const;
    bool IsForeign() const;
    bool IsVersioned() const;
    bool IsUnversioned() const;

    void Persist(const TPersistenceContext& context);

private:
    bool IsTeleportable_;
    bool IsPrimary_;
    bool IsVersioned_;
};

TString ToString(const TInputStreamDescriptor& descriptor);

////////////////////////////////////////////////////////////////////////////////

extern TInputStreamDescriptor IntermediateInputStreamDescriptor;
extern TInputStreamDescriptor TeleportableIntermediateInputStreamDescriptor;

////////////////////////////////////////////////////////////////////////////////

class TInputStreamDirectory
{
public:
    //! Used only for persistence
    TInputStreamDirectory() = default;
    explicit TInputStreamDirectory(
        std::vector<TInputStreamDescriptor> descriptors,
        TInputStreamDescriptor defaultDescriptor = IntermediateInputStreamDescriptor);

    const TInputStreamDescriptor& GetDescriptor(int inputStreamIndex) const;

    int GetDescriptorCount() const;

    //! Recover input stream index by the pair of table index and range index.
    int GetInputStreamIndex(int tableIndex, int rangeIndex) const;

    void Persist(const TPersistenceContext& context);

private:
    std::vector<TInputStreamDescriptor> Descriptors_;
    TInputStreamDescriptor DefaultDescriptor_;

    THashMap<std::pair<int, int>, int> TableAndRangeIndicesToInputStreamIndex_;
};

////////////////////////////////////////////////////////////////////////////////

extern TInputStreamDirectory IntermediateInputStreamDirectory;
extern TInputStreamDirectory TeleportableIntermediateInputStreamDirectory;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
