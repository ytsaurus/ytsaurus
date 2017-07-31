#pragma once

#include "private.h"

namespace NYT {
namespace NChunkPools {

////////////////////////////////////////////////////////////////////////////////

class TInputStreamDescriptor
{
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

////////////////////////////////////////////////////////////////////////////////

extern TInputStreamDescriptor IntermediateInputStreamDescriptor;

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

    void Persist(const TPersistenceContext& context);
private:
    std::vector<TInputStreamDescriptor> Descriptors_;
    TInputStreamDescriptor DefaultDescriptor_;
};

////////////////////////////////////////////////////////////////////////////////

extern TInputStreamDirectory IntermediateInputStreamDirectory;

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkPools
} // namespace NYT
