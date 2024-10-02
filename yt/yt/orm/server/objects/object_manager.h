#pragma once

#include "public.h"

#include <yt/yt/orm/server/master/public.h>

#include <yt/yt/core/actions/signal.h>
#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/core/ytree/public.h>

#include <yt/yt/library/query/base/public.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

class TObjectManager
    : public TRefCounted
{
public:
    TObjectManager(
        NMaster::IBootstrap* bootstrap,
        TObjectManagerConfigPtr initialConfig,
        std::vector<const TDBTable*> dataModelTables);
    ~TObjectManager();

    void Initialize();

    TObjectManagerConfigPtr GetConfig() const;

    IObjectTypeHandler* GetTypeHandlerOrCrash(TObjectTypeValue type) const;
    IObjectTypeHandler* GetTypeHandlerOrThrow(TObjectTypeValue type) const;
    IObjectTypeHandler* FindTypeHandler(TObjectTypeValue type) const;

    EAttributesExtensibilityMode GetAttributesExtensibilityMode() const;
    std::vector<NYPath::TYPath> GetAllowedExtensibleAttributePaths(TObjectTypeValue objectType);

    bool IsHistoryEnabled() const;
    virtual bool AreHistoryEnabledAttributePathsEnabled() const = 0;
    bool IsHistoryDisabledForType(TObjectTypeValue type) const;

    bool IsHistoryIndexAttributeStoreEnabled(TObjectTypeValue type, const NYPath::TYPath& attributePath) const;
    bool IsHistoryIndexAttributeQueryEnabled(TObjectTypeValue type, const NYPath::TYPath& attributePath) const;

    NYTree::IYPathServicePtr CreateOrchidService();

    NQueryClient::IColumnEvaluatorCachePtr GetColumnEvaluatorCache() const;

    DECLARE_SIGNAL(void(const TObjectManagerConfigPtr&), ConfigUpdate);

protected:
    virtual void RegisterTypeHandlers() = 0;

    void RegisterTypeHandler(std::unique_ptr<IObjectTypeHandler> handler);

    NMaster::IBootstrap* GetBootstrap() const;

private:
    class TImpl;
    const NYT::TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TObjectManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
