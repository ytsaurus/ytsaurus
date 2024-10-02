#ifndef SEMAPHORE_TYPE_HANDLER_DETAIL_INL_H_
#error "Direct inclusion of this file is not allowed, include semaphore_type_handler_detail.h"
// For the sake of sane code completion.
#include "semaphore_type_handler_detail.h"
#endif

#include "object.h"
#include "db_schema.h"

#include <yt/yt/orm/server/access_control/access_control_manager.h>

#include <yt/yt/orm/server/master/bootstrap.h>

#include <util/datetime/base.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

template <
    NMpl::DerivedFromSpecializationOf<TSemaphoreMixin> TSemaphore,
    class TProtoSemaphore,
    std::derived_from<IObjectTypeHandler> TGeneratedSemaphoreTypeHandler>
class TSemaphoreTypeHandlerBase
    : public TGeneratedSemaphoreTypeHandler
{
    using TBase = TGeneratedSemaphoreTypeHandler;
    using TProtoSemaphoreAcquire = std::decay_t<decltype(TProtoSemaphore().control().acquire())>;
    using TProtoSemaphorePing = std::decay_t<decltype(TProtoSemaphore().control().ping())>;
    using TProtoSemaphoreRelease = std::decay_t<decltype(TProtoSemaphore().control().release())>;

public:
    using TBase::TBase;

    void Initialize() override
    {
        using std::placeholders::_2;
        using std::placeholders::_3;
        using std::placeholders::_4;

        TBase::Initialize();

        TBase::ControlAttributeSchema_
            ->AddChildren({
                TBase::MakeScalarAttributeSchema("acquire")
                    ->template SetControlWithTransactionCallContext<TSemaphore, TProtoSemaphoreAcquire>(
                        std::bind(&TSemaphoreTypeHandlerBase::Acquire, _2, _3, _4)),

                TBase::MakeScalarAttributeSchema("ping")
                    ->template SetControlWithTransactionCallContext<TSemaphore, TProtoSemaphorePing>(
                        std::bind(&TSemaphoreTypeHandlerBase::Ping, _2, _3, _4)),

                TBase::MakeScalarAttributeSchema("release")
                    ->template SetControl<TSemaphore, TProtoSemaphoreRelease>(
                        std::bind(&TSemaphoreTypeHandlerBase::Release, _2, _3)),
            });
    }

    void PostInitialize() override
    {
        TBase::PostInitialize();

        TBase::StatusAttributeSchema_
            ->FindChild("fresh_leases")
                ->AsScalar()
                ->SetConstantChangedGetter(true)
                ->template SetValueGetter<TSemaphore>(&TSemaphoreTypeHandlerBase::GetFreshLeases)
                ->template SetPreloader<TSemaphore>(&TSemaphoreTypeHandlerBase::PreloadFreshLeases);
    }

    std::unique_ptr<TObject> InstantiateObject(
        const TObjectKey& key,
        const TObjectKey& parentKey,
        ISession* session) override
    {
        TObjectId id = std::get<TString>(key[0]);
        return std::make_unique<TSemaphore>(id, parentKey, this, session);
    }

    void PreloadObjectRemoval(TTransaction* transaction, const TObject* object, IUpdateContext* context) override
    {
        TBase::PreloadObjectRemoval(transaction, object, context);

        object->As<TSemaphore>()->Status().Etc().ScheduleLoad();
    }

    void CheckObjectRemoval(TTransaction* transaction, const TObject* object) override
    {
        TBase::CheckObjectRemoval(transaction, object);

        auto* semaphore = object->As<TSemaphore>();
        if (semaphore->ContainsFreshLease(Now())) {
            transaction->GetBootstrap()->GetAccessControlManager()
                ->ValidateSuperuser(Format("remove semaphore %v", semaphore->GetKey()));
        }
    }

private:
    static void Acquire(
        TSemaphore* semaphore,
        const TProtoSemaphoreAcquire& control,
        const TTransactionCallContext& transactionCallContext)
    {
        auto now = Now();
        semaphore->Acquire(now, control, transactionCallContext);
    }

    static void Ping(
        TSemaphore* semaphore,
        const TProtoSemaphorePing& control,
        const TTransactionCallContext& transactionCallContext)
    {
        auto now = Now();
        semaphore->Ping(now, control, transactionCallContext);
    }

    static void Release(
        TSemaphore* semaphore,
        const TProtoSemaphoreRelease& control)
    {
        auto now = Now();
        semaphore->Release(now, control);
    }

    static void PreloadFreshLeases(const TSemaphore* semaphore)
    {
        semaphore->Status().Etc().ScheduleLoad();
    }

    static void GetFreshLeases(
        TTransaction* /*transaction*/,
        const TSemaphore* semaphore,
        NYT::NYson::IYsonConsumer* consumer)
    {
        auto now = Now();
        NYTree::BuildYsonFluently(consumer)
            .DoMapFor(semaphore->GetFreshLeases(now), [] (auto fluent, const auto& item) {
                auto [leaseUuid, lease] = item;
                fluent.Item(leaseUuid).Value(lease);
            });
    }
};

////////////////////////////////////////////////////////////////////////////////

template <
    NMpl::DerivedFromSpecializationOf<TSemaphoreMixin> TSemaphore,
    class TProtoSemaphore,
    std::derived_from<IObjectTypeHandler> TGeneratedSemaphoreTypeHandler>
std::unique_ptr<IObjectTypeHandler> CreateSemaphoreTypeHandler(
    NMaster::IBootstrap* bootstrap,
    TObjectManagerConfigPtr config)
{
    return std::make_unique<TSemaphoreTypeHandlerBase<
        TSemaphore,
        TProtoSemaphore,
        TGeneratedSemaphoreTypeHandler>>(bootstrap, std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
