#include <yt/yt/server/lib/hydra/mock/simple_hydra_manager_mock.h>

#include <yt/yt/server/lib/hydra/composite_automaton.h>
#include <yt/yt/server/lib/hydra/public.h>
#include <yt/yt/server/lib/hydra/serialize.h>

#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/test_framework/framework.h>


namespace NYT::NHydra {
namespace {

using namespace NHydra::NProto;

////////////////////////////////////////////////////////////////////////////////

class TSimpleHydraManagerMockOverride
    : public TSimpleHydraManagerMock {
public:
    TSimpleHydraManagerMockOverride(
        TCompositeAutomatonPtr automaton,
        IInvokerPtr automatonInvoker,
        TReign reign)
        : TSimpleHydraManagerMock(
            std::move(automaton),
            std::move(automatonInvoker),
            reign)
    { }

    NLogging::ELogLevel GetUnknownAutomatonPartsLogLevel() const override {
        return NLogging::ELogLevel::Info;
    }
};

DECLARE_REFCOUNTED_CLASS(TSimpleHydraManagerMockOverride);
DEFINE_REFCOUNTED_TYPE(TSimpleHydraManagerMockOverride);


class TAutomatonPart
    : public TCompositeAutomatonPart
{
public:
    TAutomatonPart(
        ISimpleHydraManagerPtr hydraManager,
        TCompositeAutomatonPtr automaton,
        IInvokerPtr automatonInvoker,
        std::string saverName,
        std::string loaderName)
    : TCompositeAutomatonPart(
        hydraManager,
        automaton,
        automatonInvoker)
    {
        RegisterSaver(
            ESyncSerializationPriority::Values,
            saverName,
            BIND_NO_PROPAGATE(&TAutomatonPart::Save, Unretained(this)));
        RegisterLoader(
            loaderName,
            BIND_NO_PROPAGATE(&TAutomatonPart::Load, Unretained(this)));
    }

private:
    i64 FirstValue_ = 1234;
    i64 SecondValue_ = 5678;

    void Load(TLoadContext& context) {
        FirstValue_ = NYT::Load<i64>(context);
        SecondValue_ = NYT::Load<i64>(context);
        YT_VERIFY(FirstValue_ == 1234);
        YT_VERIFY(SecondValue_ == 5678);
    }

    void Save(TSaveContext& context) const {
        NYT::Save<i64>(context, FirstValue_);
        NYT::Save<i64>(context, SecondValue_);
    }
};

DECLARE_REFCOUNTED_CLASS(TAutomatonPart);
DEFINE_REFCOUNTED_TYPE(TAutomatonPart);


class TAutomatonPartWithEmptyLoader
    : public TCompositeAutomatonPart
{
public:
    TAutomatonPartWithEmptyLoader(
        ISimpleHydraManagerPtr hydraManager,
        TCompositeAutomatonPtr automaton,
        IInvokerPtr automatonInvoker,
        std::string saverName,
        std::string loaderName)
    : TCompositeAutomatonPart(
        hydraManager,
        automaton,
        automatonInvoker)
    {
        RegisterSaver(
            ESyncSerializationPriority::Values,
            saverName,
            BIND_NO_PROPAGATE(&TAutomatonPartWithEmptyLoader::Save, Unretained(this)));
        RegisterLoader(
        loaderName,
        BIND_NO_PROPAGATE([] (TLoadContext&) {}));
    }

private:
    void Save(TSaveContext& context) const {
        NYT::Save<i64>(context, 0x0246);
        NYT::Save<i64>(context, 0x1357);
    }
};

DECLARE_REFCOUNTED_CLASS(TAutomatonPartWithEmptyLoader);
DEFINE_REFCOUNTED_TYPE(TAutomatonPartWithEmptyLoader);


class TAutomaton
    : public TCompositeAutomaton
{
public:
    TAutomaton()
    : TCompositeAutomaton(nullptr, TCellId())
    { }


    std::unique_ptr<TSaveContext> CreateSaveContext(
        ICheckpointableOutputStream* output,
        NLogging::TLogger logger) override
    {
        return std::make_unique<TSaveContext>(output, std::move(logger), GetCurrentReign());
    }

    std::unique_ptr<TLoadContext> CreateLoadContext(
        ICheckpointableInputStream* input) override
    {
        auto context = std::make_unique<TLoadContext>(input);
        TCompositeAutomaton::SetupLoadContext(context.get());
        return context;
    }

    TReign GetCurrentReign() override
    {
        return 2;
    }

    EFinalRecoveryAction GetActionToRecoverFromReign(TReign) override
    {
        return EFinalRecoveryAction::None;
    }
};

DECLARE_REFCOUNTED_CLASS(TAutomaton);
DEFINE_REFCOUNTED_TYPE(TAutomaton);


void RunSimpleSnapshotLoadTest(
    std::string saverName,
    std::string loaderName,
    bool logUnknownPartsAtInfo = false,
    bool useEmptyLoader = false)
{
    auto threadPool = NConcurrency::CreateThreadPool(1, "AutomatonThread");
    auto automaton =  New<TAutomaton>();
    TReign reign = 2;

    TSimpleHydraManagerMockPtr hydra = nullptr;
    if (!logUnknownPartsAtInfo) {
        hydra = New<TSimpleHydraManagerMock>(automaton, threadPool->GetInvoker(), reign);
    } else {
        hydra = New<TSimpleHydraManagerMockOverride>(automaton, threadPool->GetInvoker(), reign);
    }

    TCompositeAutomatonPartPtr automatonPart = nullptr;
    if (!useEmptyLoader) {
        automatonPart = New<TAutomatonPart>(hydra, automaton, nullptr, saverName, loaderName);
    } else {
        automatonPart = New<TAutomatonPartWithEmptyLoader>(hydra, automaton, nullptr, saverName, loaderName);
    }

    hydra->SaveLoad();
}

TEST(TestSnapshotLoad, ItWorks)
{
    RunSimpleSnapshotLoadTest("Part", "Part");
}

TEST(TestSnapshotLoad, FailsOnUnknownParts)
{
    EXPECT_DEATH(RunSimpleSnapshotLoadTest("Part", "Part_"), /*regex*/ "Started skipping unknown automaton part .*Name: Part");
}

TEST(TestSnapshotLoad, LogLevelConfigWorks)
{
    RunSimpleSnapshotLoadTest("Part", "Part_", /*logUnknownPartsAtInfo*/ true);
}

TEST(TestSnapshotLoad, SkipIntentionally)
{
    // Use empty loader to skip a part on purpose.
    RunSimpleSnapshotLoadTest("Part", "Part", /*logUnknownPartsAtInfo*/ false, /*useEmptyLoader*/ true);
}

} // namespace
} // namespace NYT::NHydra
