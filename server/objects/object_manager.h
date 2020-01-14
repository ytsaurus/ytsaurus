#pragma once

#include "public.h"

#include <yp/server/master/public.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/core/actions/future.h>

#include <yt/core/ypath/public.h>

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

class TObjectManager
    : public TRefCounted
{
public:
    TObjectManager(NServer::NMaster::TBootstrap* bootstrap, TObjectManagerConfigPtr config);

    void Initialize();

    IObjectTypeHandler* GetTypeHandler(EObjectType type);
    IObjectTypeHandler* GetTypeHandlerOrThrow(EObjectType type);
    IObjectTypeHandler* FindTypeHandler(EObjectType type);

    bool AreExtensibleAttributesEnabled() const;
    bool IsHistoryEnabled() const;
    bool IsHistoryDisabledForType(EObjectType type) const;

private:
    class TImpl;
    const NYT::TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TObjectManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
