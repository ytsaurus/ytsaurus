#pragma once

#include "public.h"
#include "cell_base.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/object_server/public.h>
#include <yt/yt/server/master/object_server/object_detail.h>

#include <yt/yt/server/lib/misc/public.h>

#include <yt/yt/core/ytree/ypath_detail.h>

namespace NYT::NCellServer {

////////////////////////////////////////////////////////////////////////////////

class TCellProxyBase
    : public NObjectServer::TNonversionedObjectProxyBase<TCellBase>
{
public:
    using TNonversionedObjectProxyBase::TNonversionedObjectProxyBase;

protected:
    using TBase = NObjectServer::TNonversionedObjectProxyBase<TCellBase>;

    void ValidateRemoval() override;
    void RemoveSelf(TReqRemove* request, TRspRemove* response, const TCtxRemovePtr& context) override;
    void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override;
    bool GetBuiltinAttribute(NYTree::TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override;
    bool SetBuiltinAttribute(NYTree::TInternedAttributeKey key, const NYson::TYsonString& value, bool force) override;
    TResolveResult Resolve(const NYPath::TYPath& path, const NYTree::IYPathServiceContextPtr& context) override;
    TResolveResult ResolveSelf(const NYPath::TYPath& path, const NYTree::IYPathServiceContextPtr& context) override;

private:
    TResolveResult PropagateToHydraPersistenceStorage(const NYPath::TYPath& path) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
