#pragma once

#include "public.h"
// TODO(achulkov2): [PForReview] Elminate this include in favor of direct inclusion.
#include "medium_descriptor.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/library/s3/public.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TMediumDirectory
    : public TRefCounted
{
public:
    TMediumDescriptorPtr FindByIndex(int index) const;
    TMediumDescriptorPtr GetByIndexOrThrow(int index) const;

    TMediumDescriptorPtr FindByName(const TString& name) const;
    TMediumDescriptorPtr GetByNameOrThrow(const TString& name) const;

    std::vector<int> GetMediumIndexes() const;
    //! Returns the name of the medium with the given index, or "unknown" if the medium is not found.
    TString GetMediumName(int index) const;

    void LoadFrom(const NProto::TMediumDirectory& protoDirectory);

    void Clear();

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SpinLock_);
    // TODO(achulkov2): [PLater] Once masters start providing medium ids, we should switch to using medium ids as keys.
    // This will be implemented in hand with supporting offshore media without medium indexes.
    using TDescriptorStorage = THashMap<int, TMediumDescriptorPtr>;
    TDescriptorStorage IndexToDescriptor_;
    THashMap<TString, TDescriptorStorage::const_iterator> NameToDescriptor_;
};

DEFINE_REFCOUNTED_TYPE(TMediumDirectory)

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TMediumDirectoryPtr& mediumDirectory, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
