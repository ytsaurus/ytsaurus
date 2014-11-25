#include "stdafx.h"
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
    auto it = TagToEntry.find(tag);
    YCHECK(it != TagToEntry.end());
    return *it->second;
}

const TRegistry::TEntry& TRegistry::GetEntry(const std::type_info& typeInfo)
{
    auto it = TypeInfoToEntry.find(&typeInfo);
    YCHECK(it != TypeInfoToEntry.end());
    return *it->second;
}

////////////////////////////////////////////////////////////////////////////////

TSaveContext::TSaveContext()
{
    // Zero id is reserved for nullptr.
    IdGenerator.Next();
}

ui32 TSaveContext::FindId(void* basePtr, const std::type_info* typeInfo) const
{
    auto it = PtrToEntry.find(basePtr);
    if (it == PtrToEntry.end()) {
        return NullObjectId;
    } else {
        const auto& entry = it->second;
        // Failure here means an attempt was made to serialize a polymorphic type
        // not marked with TDynamicPhoenixBase.
        YCHECK(entry.TypeInfo == typeInfo);
        return entry.Id;
    }
}

ui32 TSaveContext::GenerateId(void* basePtr, const std::type_info* typeInfo)
{
    TEntry entry;
    entry.Id = static_cast<ui32>(IdGenerator.Next());
    entry.TypeInfo = typeInfo;
    YCHECK(PtrToEntry.insert(std::make_pair(basePtr, entry)).second);
    return entry.Id;
}

////////////////////////////////////////////////////////////////////////////////

TLoadContext::~TLoadContext()
{
    for (auto* rawPtr : IntrinsicInstantiated) {
        rawPtr->Unref();
    }
    for (auto* rawPtr : ExtrinsicInstantiated) {
        rawPtr->Unref();
    }
}

void TLoadContext::RegisterObject(ui32 id, void* basePtr)
{
    YCHECK(IdToPtr.insert(std::make_pair(id, basePtr)).second);
}

void* TLoadContext::GetObject(ui32 id) const
{
    auto it = IdToPtr.find(id);
    YCHECK(it != IdToPtr.end());
    return it->second;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NPhoenix
} // namespace NYT
