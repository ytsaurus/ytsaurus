#include "public.h"

#include <yt/yt/server/master/object_server/object_detail.h>

#include <yt/yt/core/ytree/static_service_dispatcher.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

class TTabletProxyBase
    : public NObjectServer::TNonversionedObjectProxyBase<TTabletBase>
    , public virtual NYTree::TStaticServiceDispatcher
{
public:
    using TNonversionedObjectProxyBase::TNonversionedObjectProxyBase;

protected:
    using TBase = NObjectServer::TNonversionedObjectProxyBase<TTabletBase>;

    TTabletProxyBase(
        NCellMaster::TBootstrap* bootstrap,
        NObjectServer::TObjectTypeMetadata* metadata,
        TTabletBase* tablet);

    void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override;
    bool GetBuiltinAttribute(NYTree::TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override;
    TFuture<NYson::TYsonString> GetBuiltinAttributeAsync(NYTree::TInternedAttributeKey key) override;

    virtual NYPath::TYPath GetOrchidPath(TTabletId tabletId) const = 0;

private:
    NYTree::IYPathServicePtr CreateOrchidService();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
