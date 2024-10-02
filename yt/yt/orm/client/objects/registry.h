#pragma once

#include "acl.h"
#include "tags.h"
#include "type.h"

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

namespace NYT::NOrm::NClient::NObjects {

////////////////////////////////////////////////////////////////////////////////

class TRegistriesHolder
{
public:
    TRegistriesHolder() = default;
    TRegistriesHolder(
        IConstObjectTypeRegistryPtr objectTypeRegistry,
        IConstTagsRegistryPtr tagsRegistry,
        IConstAccessControlRegistryPtr aclRegistry);

    void SetObjectTypeRegistry(IConstObjectTypeRegistryPtr registry);
    IConstObjectTypeRegistryPtr TryGetObjectTypeRegistry() const;
    IConstObjectTypeRegistryPtr GetObjectTypeRegistry() const;

    void SetTagsRegistry(IConstTagsRegistryPtr registry);
    IConstTagsRegistryPtr TryGetTagsRegistry() const;
    IConstTagsRegistryPtr GetTagsRegistry() const;

    void SetAccessControlRegistry(IConstAccessControlRegistryPtr registry);
    IConstAccessControlRegistryPtr TryGetAccessControlRegistry() const;
    IConstAccessControlRegistryPtr GetAccessControlRegistry() const;

private:
    TAtomicIntrusivePtr<const IObjectTypeRegistry> ObjectTypeRegistry_;
    TAtomicIntrusivePtr<const ITagsRegistry> TagsRegistry_;
    TAtomicIntrusivePtr<const IAccessControlRegistry> AclRegistry_;
};

////////////////////////////////////////////////////////////////////////////////

void SetGlobalObjectTypeRegistry(IConstObjectTypeRegistryPtr registry);
IConstObjectTypeRegistryPtr TryGetGlobalObjectTypeRegistry();

//! Global object registry must be set before usage.
IConstObjectTypeRegistryPtr GetGlobalObjectTypeRegistry();

////////////////////////////////////////////////////////////////////////////////

void SetGlobalTagsRegistry(IConstTagsRegistryPtr registry);
IConstTagsRegistryPtr TryGetGlobalTagsRegistry();

//! Global tags registry must be set before usage.
IConstTagsRegistryPtr GetGlobalTagsRegistry();

////////////////////////////////////////////////////////////////////////////////

void SetGlobalAccessControlRegistry(IConstAccessControlRegistryPtr registry);
IConstAccessControlRegistryPtr TryGetGlobalAccessControlRegistry();

//! Global acl registry must be set before usage.
IConstAccessControlRegistryPtr GetGlobalAccessControlRegistry();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NObjects
