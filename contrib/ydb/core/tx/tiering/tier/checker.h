#pragma once
#include "object.h"

#include <contrib/ydb/core/tx/schemeshard/schemeshard.h>
#include <contrib/ydb/core/tx/tiering/rule/ss_fetcher.h>
#include <contrib/ydb/core/tx/tiering/snapshot.h>

#include <contrib/ydb/services/metadata/abstract/common.h>
#include <contrib/ydb/services/metadata/abstract/kqp_common.h>
#include <contrib/ydb/services/metadata/manager/preparation_controller.h>
#include <contrib/ydb/services/metadata/secret/snapshot.h>

namespace NKikimr::NColumnShard::NTiers {

class TTierPreparationActor: public NActors::TActorBootstrapped<TTierPreparationActor> {
private:
    std::vector<TTierConfig> Objects;
    NMetadata::NModifications::IAlterPreparationController<TTierConfig>::TPtr Controller;
    NMetadata::NModifications::IOperationsManager::TInternalModificationContext Context;
    std::shared_ptr<NMetadata::NSecret::TSnapshot> Secrets;
    std::shared_ptr<TConfigsSnapshot> Tierings;
    std::shared_ptr<TFetcherCheckUserTieringPermissions> SSFetcher;
    std::optional<TFetcherCheckUserTieringPermissions::TResult> SSCheckResult;
    void StartChecker();
protected:
    void Handle(NMetadata::NProvider::TEvRefreshSubscriberData::TPtr& ev);
    void Handle(NSchemeShard::TEvSchemeShard::TEvProcessingResponse::TPtr& ev);
public:
    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NMetadata::NProvider::TEvRefreshSubscriberData, Handle);
            hFunc(NSchemeShard::TEvSchemeShard::TEvProcessingResponse, Handle);
            default:
                break;
        }
    }
    void Bootstrap();

    TTierPreparationActor(std::vector<TTierConfig>&& objects,
        NMetadata::NModifications::IAlterPreparationController<TTierConfig>::TPtr controller,
        const NMetadata::NModifications::IOperationsManager::TInternalModificationContext& context);
};

}
