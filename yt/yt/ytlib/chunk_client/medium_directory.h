#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct TMediumDescriptor
{
    TString Name;
    int Index = GenericMediumIndex;
    int Priority = -1;

    bool operator == (const TMediumDescriptor& other) const = default;
};

class TMediumDirectory
    : public TRefCounted
{
public:
    const TMediumDescriptor* FindByIndex(int index) const;
    const TMediumDescriptor* GetByIndexOrThrow(int index) const;

    const TMediumDescriptor* FindByName(const TString& name) const;
    const TMediumDescriptor* GetByNameOrThrow(const TString& name) const;

    std::vector<int> GetMediumIndexes() const;

    void LoadFrom(const NProto::TMediumDirectory& protoDirectory);

    void Clear();

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SpinLock_);
    THashMap<TString, const TMediumDescriptor*> NameToDescriptor_;
    THashMap<int, const TMediumDescriptor*> IndexToDescriptor_;

    std::vector<std::unique_ptr<TMediumDescriptor>> Descriptors_;

};

DEFINE_REFCOUNTED_TYPE(TMediumDirectory)

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TMediumDirectoryPtr& mediumDirectory, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
