#include "phoenix.h"

#include "collection_helpers.h"

namespace NYT::NPhoenix {

////////////////////////////////////////////////////////////////////////////////

TRegistry::TRegistry()
{ }

TRegistry* TRegistry::Get()
{
    return Singleton<TRegistry>();
}

ui32 TRegistry::GetTag(const std::type_info& typeInfo)
{
    return GetEntry(typeInfo).Tag;
}

const TRegistry::TEntry& TRegistry::GetEntry(ui32 tag)
{
    return GetOrCrash(TagToEntry_, tag);
}

const TRegistry::TEntry& TRegistry::GetEntry(const std::type_info& typeInfo)
{
    return *GetOrCrash(TypeInfoToEntry_, &typeInfo);
}

////////////////////////////////////////////////////////////////////////////////

TSaveContext::TSaveContext()
{
    // Zero id is reserved for nullptr.
    IdGenerator_.Next();
}

ui32 TSaveContext::FindId(void* basePtr, const std::type_info* typeInfo) const
{
    auto it = PtrToEntry_.find(basePtr);
    if (it == PtrToEntry_.end()) {
        return NullObjectId;
    } else {
        const auto& entry = it->second;
        // Failure here means an attempt was made to serialize a polymorphic type
        // not marked with TDynamicTag.
        YT_VERIFY(entry.TypeInfo == typeInfo);
        return entry.Id;
    }
}

ui32 TSaveContext::GenerateId(void* basePtr, const std::type_info* typeInfo)
{
    TEntry entry;
    entry.Id = static_cast<ui32>(IdGenerator_.Next());
    entry.TypeInfo = typeInfo;
    YT_VERIFY(PtrToEntry_.insert(std::make_pair(basePtr, entry)).second);
    return entry.Id;
}

////////////////////////////////////////////////////////////////////////////////

TLoadContext::~TLoadContext()
{
    for (const auto& deletor : Deletors_) {
        deletor();
    }
}

void TLoadContext::RegisterObject(ui32 id, void* basePtr)
{
    YT_VERIFY(IdToPtr_.insert(std::make_pair(id, basePtr)).second);
}

void* TLoadContext::GetObject(ui32 id) const
{
    return GetOrCrash(IdToPtr_, id);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPhoenix
