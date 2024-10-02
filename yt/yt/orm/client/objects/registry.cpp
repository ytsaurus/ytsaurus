#include "registry.h"

namespace NYT::NOrm::NClient::NObjects {

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

TRegistriesHolder* GetGlobalRegistriesHolder()
{
    return Singleton<TRegistriesHolder>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

TRegistriesHolder::TRegistriesHolder(
    IConstObjectTypeRegistryPtr objectTypeRegistry,
    IConstTagsRegistryPtr tagsRegistry,
    IConstAccessControlRegistryPtr aclRegistry)
    : ObjectTypeRegistry_(std::move(objectTypeRegistry))
    , TagsRegistry_(std::move(tagsRegistry))
    , AclRegistry_(std::move(aclRegistry))
{ }

void TRegistriesHolder::SetObjectTypeRegistry(IConstObjectTypeRegistryPtr registry)
{
    TAtomicIntrusivePtr<const IObjectTypeRegistry>::TRawPtr expected{nullptr};
    YT_VERIFY(ObjectTypeRegistry_.CompareAndSwap(expected, registry));
}

IConstObjectTypeRegistryPtr TRegistriesHolder::TryGetObjectTypeRegistry() const
{
    return ObjectTypeRegistry_.Acquire();
}

IConstObjectTypeRegistryPtr TRegistriesHolder::GetObjectTypeRegistry() const
{
    auto registry = TryGetObjectTypeRegistry();
    YT_VERIFY(registry);
    return registry;
}

void TRegistriesHolder::SetTagsRegistry(IConstTagsRegistryPtr registry)
{
    TAtomicIntrusivePtr<const ITagsRegistry>::TRawPtr expected{nullptr};
    YT_VERIFY(TagsRegistry_.CompareAndSwap(expected, registry));
}

IConstTagsRegistryPtr TRegistriesHolder::TryGetTagsRegistry() const
{
    return TagsRegistry_.Acquire();
}

IConstTagsRegistryPtr TRegistriesHolder::GetTagsRegistry() const
{
    auto registry = TryGetTagsRegistry();
    YT_VERIFY(registry);
    return registry;
}

void TRegistriesHolder::SetAccessControlRegistry(IConstAccessControlRegistryPtr registry)
{
    TAtomicIntrusivePtr<const IAccessControlRegistry>::TRawPtr expected{nullptr};
    YT_VERIFY(AclRegistry_.CompareAndSwap(expected, registry));
}

IConstAccessControlRegistryPtr TRegistriesHolder::TryGetAccessControlRegistry() const
{
    return AclRegistry_.Acquire();
}

IConstAccessControlRegistryPtr TRegistriesHolder::GetAccessControlRegistry() const
{
    auto registry = TryGetAccessControlRegistry();
    YT_VERIFY(registry);
    return registry;
}

////////////////////////////////////////////////////////////////////////////////

void SetGlobalObjectTypeRegistry(IConstObjectTypeRegistryPtr registry)
{
    auto* holder = GetGlobalRegistriesHolder();
    holder->SetObjectTypeRegistry(std::move(registry));
}

IConstObjectTypeRegistryPtr TryGetGlobalObjectTypeRegistry()
{
    auto* holder = GetGlobalRegistriesHolder();
    return holder->TryGetObjectTypeRegistry();
}

IConstObjectTypeRegistryPtr GetGlobalObjectTypeRegistry()
{
    auto* holder = GetGlobalRegistriesHolder();
    return holder->GetObjectTypeRegistry();
}

////////////////////////////////////////////////////////////////////////////////

void SetGlobalTagsRegistry(IConstTagsRegistryPtr registry)
{
    auto* holder = GetGlobalRegistriesHolder();
    holder->SetTagsRegistry(std::move(registry));
}

IConstTagsRegistryPtr TryGetGlobalTagsRegistry()
{
    auto* holder = GetGlobalRegistriesHolder();
    return holder->TryGetTagsRegistry();
}

IConstTagsRegistryPtr GetGlobalTagsRegistry()
{
    auto* holder = GetGlobalRegistriesHolder();
    return holder->GetTagsRegistry();
}

////////////////////////////////////////////////////////////////////////////////

void SetGlobalAccessControlRegistry(IConstAccessControlRegistryPtr registry)
{
    auto* holder = GetGlobalRegistriesHolder();
    holder->SetAccessControlRegistry(std::move(registry));
}

IConstAccessControlRegistryPtr TryGetGlobalAccessControlRegistry()
{
    auto* holder = GetGlobalRegistriesHolder();
    return holder->TryGetAccessControlRegistry();
}

IConstAccessControlRegistryPtr GetGlobalAccessControlRegistry()
{
    auto* holder = GetGlobalRegistriesHolder();
    return holder->GetAccessControlRegistry();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NObjects
