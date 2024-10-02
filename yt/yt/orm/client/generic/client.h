#pragma once

#include <yt/yt/orm/client/native/request.h>
#include <yt/yt/orm/client/native/response.h>
#include <yt/yt/orm/client/objects/helpers.h>

namespace NYT::NOrm::NClient::NGeneric {

////////////////////////////////////////////////////////////////////////////////

struct IOrmClient
    : public TRefCounted
{
    virtual TFuture<NNative::TSelectObjectsResult> SelectObjects(
        NObjects::TObjectTypeValue objectType,
        const NNative::TAttributeSelector& selector,
        const NNative::TSelectObjectsOptions& options = NNative::TSelectObjectsOptions()) = 0;

    virtual TFuture<NNative::TGetObjectsResult> GetObjects(
        std::vector<NNative::TObjectIdentity> objectIdentities,
        NObjects::TObjectTypeValue objectType,
        const NNative::TAttributeSelector& selector,
        const NNative::TGetObjectOptions& options = NNative::TGetObjectOptions()) = 0;

    virtual TFuture<NNative::TCreateObjectResult> CreateObject(
        NObjects::TObjectTypeValue objectType,
        NNative::TPayload attributesPayload,
        const NNative::TCreateObjectOptions& options = NNative::TCreateObjectOptions(),
        std::optional<NNative::TUpdateIfExisting> updateIfExisting = std::nullopt) = 0;

    virtual TFuture<NNative::TUpdateObjectResult> UpdateObject(
        NNative::TObjectIdentity objectIdentity,
        NObjects::TObjectTypeValue objectType,
        std::vector<NNative::TUpdate> updates,
        std::vector<NNative::TAttributeTimestampPrerequisite> attributeTimestampPrerequisites,
        const NNative::TUpdateObjectOptions& options = NNative::TUpdateObjectOptions()) = 0;

    virtual TFuture<NNative::TRemoveObjectResult> RemoveObject(
        NNative::TObjectIdentity objectIdentity,
        NObjects::TObjectTypeValue objectType,
        const NNative::TRemoveObjectOptions& options = NNative::TRemoveObjectOptions()) = 0;
};

DEFINE_REFCOUNTED_TYPE(IOrmClient);

////////////////////////////////////////////////////////////////////////////////

} // NYT::NOrm::NClient::NGeneric
