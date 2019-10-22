#pragma once

#include "type_handler.h"
#include "type_handler_detail.h"

#include <yt/ytlib/cypress_client/cypress_ypath_proxy.h>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

template <class TObject>
class TNonversionedMapObjectTypeHandlerBase
    : public TObjectTypeHandlerWithMapBase<TObject>
{
private:
    using TMapType = NHydra::TEntityMap<TObject>;
    using TBase = NObjectServer::TObjectTypeHandlerWithMapBase<TObject>;

public:
    TNonversionedMapObjectTypeHandlerBase(NCellMaster::TBootstrap* bootstrap, TMapType* map);

    virtual ETypeFlags GetFlags() const override;

    virtual std::unique_ptr<NObjectServer::TObject> InstantiateObject(TObjectId id) override;

    virtual NObjectServer::TObject* DoGetParent(TObject* object) override;

    virtual std::optional<TString> TryGetRootPath(const TObject* maybeRootObject) const = 0;

    virtual void RegisterName(const TString& /* name */, TObject* /* object */) noexcept = 0;
    virtual void UnregisterName(const TString& /* name */, TObject* /* object */) noexcept = 0;

    virtual void ValidateObjectName(const TString& name);
    // XXX(kiselyovp) we might want to move this to the proxy
    virtual void ValidateAttachChildDepth(const TObject* parent, const TObject* child);

protected:
    virtual TString DoGetName(const TObject* object) override;
    virtual NSecurityServer::TAccessControlDescriptor* DoFindAcd(TObject* object) override;
    virtual void DoZombifyObject(TObject* object) override;

    NObjectServer::TObject* CreateObjectImpl(
        const TString& name,
        TObject* parent,
        NYTree::IAttributeDictionary* attributes);

    virtual std::optional<int> GetDepthLimit() const;
    // XXX(kiselyovp) These methods have total complexity of O(depth_limit + subtree_size) and get called
    // on each call of Create and Move verbs. Those calls are not expected to be common.
    int GetDepth(const TObject* object) const;
    void ValidateHeightLimit(const TObject* root, int heightLimit) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
