#pragma once

#include "public.h"

#include "medium_descriptor.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TMediumDirectory
    : public TRefCounted
{
public:
    TMediumDescriptorPtr FindByIndex(int index) const;
    TMediumDescriptorPtr GetByIndexOrThrow(int index) const;

    TMediumDescriptorPtr FindByName(const std::string& name) const;
    TMediumDescriptorPtr GetByNameOrThrow(const std::string& name) const;

    std::vector<int> GetMediumIndexes() const;

    void LoadFrom(const NProto::TMediumDirectory& protoDirectory);

    void Clear();

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SpinLock_);
    THashMap<std::string, TMediumDescriptorPtr> NameToDescriptor_;
    // TODO(cherepashka, achulkov2): Once masters start providing medium ids, we should switch to using medium ids as keys.
    // This will be implemented in hand with supporting offshore media without medium indices.
    THashMap<int, TMediumDescriptorPtr> IndexToDescriptor_;
};

DEFINE_REFCOUNTED_TYPE(TMediumDirectory)

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TMediumDirectoryPtr& mediumDirectory, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
