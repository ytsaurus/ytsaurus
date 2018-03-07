#include "phoenix.h"

namespace NYT {
namespace NPhoenix {

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
    auto it = TagToEntry_.find(tag);
    YCHECK(it != TagToEntry_.end());
    return it->second;
}

const TRegistry::TEntry& TRegistry::GetEntry(const std::type_info& typeInfo)
{
    auto it = TypeInfoToEntry_.find(&typeInfo);
    YCHECK(it != TypeInfoToEntry_.end());
    return *it->second;
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
        YCHECK(entry.TypeInfo == typeInfo);
        return entry.Id;
    }
}

ui32 TSaveContext::GenerateId(void* basePtr, const std::type_info* typeInfo)
{
    TEntry entry;
    entry.Id = static_cast<ui32>(IdGenerator_.Next());
    entry.TypeInfo = typeInfo;
    YCHECK(PtrToEntry_.insert(std::make_pair(basePtr, entry)).second);
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
    YCHECK(IdToPtr_.insert(std::make_pair(id, basePtr)).second);
}

void* TLoadContext::GetObject(ui32 id) const
{
    auto it = IdToPtr_.find(id);
    YCHECK(it != IdToPtr_.end());
    return it->second;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NPhoenix
} // namespace NYT
