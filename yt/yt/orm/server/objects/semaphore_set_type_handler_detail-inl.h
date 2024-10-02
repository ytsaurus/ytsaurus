#ifndef SEMAPHORE_SET_TYPE_HANDLER_DETAIL_INL_H_
#error "Direct inclusion of this file is not allowed, include semaphore_set_type_handler_detail.h"
// For the sake of sane code completion.
#include "semaphore_set_type_handler_detail.h"
#endif

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

template <
    NMpl::DerivedFromSpecializationOf<TSemaphoreSetMixin> TSemaphoreSet,
    class TProtoSemaphoreSet,
    std::derived_from<IObjectTypeHandler> TGeneratedSemaphoreSetTypeHandler>
class TSemaphoreSetTypeHandlerBase
    : public TGeneratedSemaphoreSetTypeHandler
{
    using TBase = TGeneratedSemaphoreSetTypeHandler;
    using TProtoSemaphoreSetAcquire = std::decay_t<decltype(TProtoSemaphoreSet().control().acquire())>;
    using TProtoSemaphoreSetPing = std::decay_t<decltype(TProtoSemaphoreSet().control().ping())>;
    using TProtoSemaphoreSetRelease = std::decay_t<decltype(TProtoSemaphoreSet().control().release())>;

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
                    ->template SetControlWithTransactionCallContext<TSemaphoreSet, TProtoSemaphoreSetAcquire>(
                        std::bind(&TSemaphoreSetTypeHandlerBase::Acquire, _2, _3, _4)),

                TBase::MakeScalarAttributeSchema("ping")
                    -> template SetControlWithTransactionCallContext<TSemaphoreSet, TProtoSemaphoreSetPing>(
                        std::bind(&TSemaphoreSetTypeHandlerBase::Ping, _2, _3, _4)),

                TBase::MakeScalarAttributeSchema("release")
                    ->template SetControl<TSemaphoreSet, TProtoSemaphoreSetRelease>(
                        std::bind(&TSemaphoreSetTypeHandlerBase::Release, _2, _3)),
            });
    }

    void PostInitialize() override
    {
        TBase::PostInitialize();

        TBase::StatusAttributeSchema_
            ->FindChild("fresh_leases_by_semaphore")
                ->AsScalar()
                ->SetConstantChangedGetter(true)
                ->template SetValueGetter<TSemaphoreSet>(&TSemaphoreSetTypeHandlerBase::GetFreshLeasesBySemaphore)
                ->template SetPreloader<TSemaphoreSet>(&TSemaphoreSetTypeHandlerBase::PreloadFreshLeases);
    }

    std::unique_ptr<TObject> InstantiateObject(
        const TObjectKey& key,
        const TObjectKey& parentKey,
        ISession* session) override
    {
        YT_VERIFY(!parentKey);
        TObjectId id = std::get<TString>(key[0]);
        return std::make_unique<TSemaphoreSet>(id, this, session);
    }

private:
    static void Acquire(
        TSemaphoreSet* semaphoreSet,
        const TProtoSemaphoreSetAcquire& control,
        const TTransactionCallContext& transactionCallContext)
    {
        auto now = Now();
        semaphoreSet->Acquire(now, control, transactionCallContext);
    }

    static void Ping(
        TSemaphoreSet* semaphoreSet,
        const TProtoSemaphoreSetPing& control,
        const TTransactionCallContext& transactionCallContext)
    {
        auto now = Now();
        semaphoreSet->Ping(now, control, transactionCallContext);
    }

    static void Release(
        TSemaphoreSet* semaphoreSet,
        const TProtoSemaphoreSetRelease& control)
    {
        auto now = Now();
        semaphoreSet->Release(now, control);
    }

    static void PreloadFreshLeases(const TSemaphoreSet* semaphoreSet)
    {
        semaphoreSet->Semaphores().ScheduleLoad();
    }

    static void GetFreshLeasesBySemaphore(
        NYT::NOrm::NServer::NObjects::TTransaction* /*transaction*/,
        const TSemaphoreSet* semaphoreSet,
        NYT::NYson::IYsonConsumer* consumer)
    {
        auto now = Now();
        auto freshLeasesBySemaphore = semaphoreSet->GetFreshLeasesBySemaphore(now);
        NYTree::BuildYsonFluently(consumer)
            .DoMapFor(freshLeasesBySemaphore, [] (auto fluent, auto semaphore) {
                auto [semaphoreId, freshLeases] = semaphore;
                fluent.Item(semaphoreId)
                    .BeginMap()
                        .Item("values")
                        .DoMapFor(freshLeases, [] (auto fluent, auto item) {
                            auto [leaseUuid, lease] = item;
                            fluent.Item(leaseUuid).Value(lease);
                        })
                    .EndMap();
            });
    }
};

////////////////////////////////////////////////////////////////////////////////

template <
    NMpl::DerivedFromSpecializationOf<TSemaphoreSetMixin> TSemaphoreSet,
    class TProtoSemaphoreSet,
    std::derived_from<IObjectTypeHandler> TGeneratedSemaphoreSetTypeHandler>
std::unique_ptr<IObjectTypeHandler> CreateSemaphoreSetTypeHandler(
    NMaster::IBootstrap* bootstrap,
    TObjectManagerConfigPtr config)
{
    return std::make_unique<TSemaphoreSetTypeHandlerBase<
        TSemaphoreSet,
        TProtoSemaphoreSet,
        TGeneratedSemaphoreSetTypeHandler>>(bootstrap, std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
