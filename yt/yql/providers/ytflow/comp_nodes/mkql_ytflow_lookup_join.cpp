#include "mkql_ytflow_lookup_join.h"

#include <library/cpp/threading/future/future.h>

#include <yt/yql/providers/ytflow/integration/mkql_interface/yql_ytflow_lookup_provider.h>

#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders_codegen.h>
#include <yql/essentials/minikql/defs.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/minikql/mkql_type_builder.h>

#include <util/datetime/base.h>
#include <util/generic/algorithm.h>
#include <util/string/join.h>

#include <deque>


namespace NKikimr::NMiniKQL {

using namespace NYql;
using namespace NYql::NUdf;

namespace {

struct TLookupJoinScope
{
    TString Label;
    bool IsLeftSide;
    TVector<TString> Keys;
    ERowSelectionMode RowSelectionMode;

    const TStructType* RowType = nullptr;
};

THolder<IYtflowLookupProvider> CreateLookupProvider(
    const IYtflowLookupProviderFactory& factory,
    TComputationContext& ctx
) {
    // The wrapper can be shared by pattern clones, so codecs must be built
    // against the target graph context. Create() consumes this builder synchronously.
    TFunctionTypeInfoBuilder functionTypeInfoBuilder(
        ctx.LangVer,
        ctx.RuntimeSettings,
        ctx.TypeEnv,
        ctx.TypeInfoHelper,
        "YtflowLookupJoin",
        ctx.CountersProvider,
        NUdf::TSourcePosition(),
        ctx.SecureParamsProvider,
        ctx.LogProvider);

    return factory.Create(IYtflowLookupProviderFactory::TCreationContext{
        .ValueBuilder = const_cast<NUdf::IValueBuilder&>(*ctx.Builder),
        .FunctionTypeInfoBuilder = functionTypeInfoBuilder,
    });
}


class TYtflowLookupJoinWrapper
    : public TMutableComputationNode<TYtflowLookupJoinWrapper>
{
public:
    using TBase = TMutableComputationNode<TYtflowLookupJoinWrapper>;
    using TSelf = TYtflowLookupJoinWrapper;

    struct TKeyGroup
    {
        TVector<TUnboxedValue> Rows;
        bool LookupStarted = false;
        bool HasNullKeyColumns = false;
    };

    struct TLookupState
    {
        TVector<TUnboxedValue> Keys;
        NThreading::TFuture<IYtflowLookupProvider::ILookupResultPtr> Future;
    };

    class TStreamValue
        : public TComputationValue<TStreamValue>
    {
    public:
        TStreamValue(
            TMemoryUsageInfo* memInfo,
            TUnboxedValue stream,
            const TSelf* self,
            TComputationContext& ctx
        )
            : TComputationValue(memInfo)
            , Stream(std::move(stream))
            , Self(self)
            , Ctx(ctx)
            , YtflowLookupProvider(CreateLookupProvider(
                *Self->YtflowLookupProviderFactory,
                ctx))
            , KeyGroups(
                0,
                TValueHasher(Self->StreamKeyTypes, true, nullptr),
                TValueEqual(Self->StreamKeyTypes, true, nullptr)
            )
        {
        }

        EFetchStatus Fetch(TUnboxedValue& result) override {
            while (true) {
                FetchStreamItems();

                if (TryMakeOutputItem(result)) {
                    return EFetchStatus::Ok;
                }

                if (LoadedKeys || TryFillLoadedData()) {
                    continue;
                }

                switch (InputStatus) {
                case EFetchStatus::Ok:
                    break;

                case EFetchStatus::Yield:
                    InputStatus = EFetchStatus::Ok;
                    if (FlushInProgress) {
                        FlushInProgress = false;
                        EnsureEmptyInflight();
                    }
                    return EFetchStatus::Yield;

                case EFetchStatus::Finish:
                    FlushInProgress = false;
                    EnsureEmptyInflight();
                    return EFetchStatus::Finish;
                }
            }
        }

        void FetchStreamItems() {
            while (ShouldFetchStreamItem()) {
                TUnboxedValue streamItem;
                InputStatus = Stream.Fetch(streamItem);

                switch (InputStatus) {
                case EFetchStatus::Ok:
                    AddStreamItem(std::move(streamItem));
                    continue;

                case EFetchStatus::Yield:
                    if (Ctx.FlushingMode) {
                        StartFlush();
                    }
                    return;

                case EFetchStatus::Finish:
                    StartFlush();
                    return;
                }
            }
        }

        bool ShouldFetchStreamItem() const {
            return InputStatus == EFetchStatus::Ok
                && !FlushInProgress
                && InflightLookups.size() < Self->InflightLookupLimit;
        }

        void AddStreamItem(TUnboxedValue streamItem) {
            auto key = ExtractStreamKey(streamItem);

            auto iterator = KeyGroups.find(key);

            if (iterator != KeyGroups.end()) {
                iterator->second.Rows.push_back(std::move(streamItem));
            } else {
                InflightKeys.push_back(key);

                bool hasNullKeyColumns = HasNullColumns(key, Self->StreamKeyIndices.size());

                iterator = KeyGroups.emplace(
                    std::move(key),
                    TKeyGroup{
                        .Rows = {std::move(streamItem)},
                        .HasNullKeyColumns = hasNullKeyColumns,
                    }).first;
            }

            if (!iterator->second.LookupStarted) {
                ++InflightRowCount;
            }

            if (InflightRowCount >= Self->InflightRowLimit) {
                DoLookup();
            }
        }

        void StartFlush() {
            FlushInProgress = true;

            if (InflightKeys) {
                DoLookup();
            }
        }

        TUnboxedValue ExtractStreamKey(const TUnboxedValue& streamItem) const {
            TUnboxedValue* items = nullptr;
            auto key = Self->StreamKeyStruct.NewArray(
                Ctx,
                Self->StreamKeyIndices.size(),
                items);

            for (ui32 index = 0; index < Self->StreamKeyIndices.size(); ++index) {
                items[index] = streamItem.GetElement(
                    Self->StreamKeyIndices[index]);
            }

            return key;
        }

        bool HasNullColumns(const TUnboxedValue& value, ui32 columnCount) const {
            for (ui32 index = 0; index < columnCount; ++index) {
                if (!value.GetElement(index)) {
                    return true;
                }
            }

            return false;
        }

        void DoLookup() {
            auto keys = std::move(InflightKeys);
            for (const auto& key : keys) {
                KeyGroups[key].LookupStarted = true;
            }

            auto future = YtflowLookupProvider->Lookup(keys);

            auto lookupState = TLookupState{
                .Keys = std::move(keys),
                .Future = std::move(future)
            };

            InflightLookups.push_back(std::move(lookupState));
            InflightRowCount = 0;
        }

        bool TryFillLoadedData() {
            if (InflightLookups.empty()) {
                return false;
            }

            auto& lookupState = InflightLookups.front();
            if (!lookupState.Future.IsReady()) {
                MKQL_ENSURE(
                    lookupState.Future.Wait(Self->LookupTimeout),
                    "Lookup timeout exceeded for table "
                        << YtflowLookupProvider->GetTableName());
            }

            LoadedKeys = std::move(lookupState.Keys);
            LoadedValues = YtflowLookupProvider->Decode(
                lookupState.Future.ExtractValue());

            InflightLookups.pop_front();

            return true;
        }

        bool TryMakeOutputItem(TUnboxedValue& result) {
            if (!LoadedKeys) {
                return false;
            }

            if (CurrentLoadedKeyIndex >= LoadedKeys.size()) {
                LoadedKeys.clear();
                LoadedValues.clear();
                CurrentLoadedKeyIndex = 0;

                return false;
            }

            const auto& currentLoadedKey = LoadedKeys[CurrentLoadedKeyIndex];
            auto advanceCurrentLoadedKeyIndex = [&] {
                KeyGroups.erase(currentLoadedKey);
                ++CurrentLoadedKeyIndex;
            };

            const auto& currentLoadedKeyGroup = KeyGroups[currentLoadedKey];
            const auto& currentLoadedValues = LoadedValues[CurrentLoadedKeyIndex];

            bool hasNullKeyColumns = currentLoadedKeyGroup.HasNullKeyColumns;
            bool emptyLookupSourceSide = currentLoadedValues.empty()
                || hasNullKeyColumns;

            if (CurrentStreamRowIndex == 0 && CurrentLoadedValueIndex == 0) {
                bool satisfiesJoinKind;

                switch (Self->EffectiveJoinKind) {
                case EJoinKind::Left:
                    satisfiesJoinKind = true;
                    break;

                case EJoinKind::LeftOnly:
                    satisfiesJoinKind = emptyLookupSourceSide;
                    break;

                case EJoinKind::LeftSemi:
                case EJoinKind::Inner:
                    satisfiesJoinKind = !emptyLookupSourceSide;
                    break;

                default:
                    MKQL_ENSURE(
                        false,
                        "Unexpected join kind: "
                            << static_cast<ui32>(Self->JoinKind));
                }

                if (!satisfiesJoinKind) {
                    advanceCurrentLoadedKeyIndex();
                    return false;
                }

                if (currentLoadedValues.size() > 1 && Self->RowSelectionMode != ERowSelectionMode::All) {
                    MKQL_ENSURE(
                        false,
                        "Unexpected lookup result count: "
                            << currentLoadedValues.size()
                            << ", row selection mode: " << ToString(Self->RowSelectionMode));
                }
            }

            if (CurrentStreamRowIndex >= currentLoadedKeyGroup.Rows.size()) {
                advanceCurrentLoadedKeyIndex();
                CurrentStreamRowIndex = 0;
                return false;
            }

            if (CurrentLoadedValueIndex >= currentLoadedValues.size()) {
                if (CurrentLoadedValueIndex > 0) {
                    ++CurrentStreamRowIndex;
                    CurrentLoadedValueIndex = 0;
                    return false;
                }
            }

            TUnboxedValue* resultItems = nullptr;

            result = Self->ResultStruct.NewArray(
                Ctx,
                Self->StreamOutputIndices.size() + Self->LookupSourceOutputIndices.size(),
                resultItems);

            const auto& currentStreamRow =
                currentLoadedKeyGroup.Rows[CurrentStreamRowIndex];

            for (
                ui32 position = 0;
                position < Self->StreamOutputIndices.size();
                ++position
            ) {
                ui32 inputIndex = Self->StreamInputIndices[position];
                auto inputItem = currentStreamRow.GetElement(inputIndex);

                ui32 outputIndex = Self->StreamOutputIndices[position];
                resultItems[outputIndex] = inputItem;
            }

            if (emptyLookupSourceSide) {
                for (
                    ui32 position = 0;
                    position < Self->LookupSourceOutputIndices.size();
                    ++position
                ) {
                    ui32 outputIndex = Self->LookupSourceOutputIndices[position];
                    resultItems[outputIndex] = TUnboxedValuePod();
                }

                ++CurrentStreamRowIndex;
                CurrentLoadedValueIndex = 0;
            } else {
                const auto& currentValueRow = currentLoadedValues[CurrentLoadedValueIndex];

                for (
                    ui32 position = 0;
                    position < Self->LookupSourceOutputIndices.size();
                    ++position
                ) {
                    ui32 inputIndex = Self->LookupSourceInputIndices[position];
                    auto inputItem = currentValueRow.GetElement(inputIndex);

                    ui32 outputIndex = Self->LookupSourceOutputIndices[position];
                    resultItems[outputIndex] = inputItem;
                }

                ++CurrentLoadedValueIndex;
            }

            return true;
        }

    private:
        void EnsureEmptyInflight() {
            Y_ABORT_UNLESS(
                KeyGroups.empty() &&
                InflightKeys.empty() &&
                InflightLookups.empty() &&
                LoadedKeys.empty() &&
                LoadedValues.empty() &&
                !FlushInProgress,
                "Unexpected nonzero inflight values during lookup join:"
                    " %ld (key groups),"
                    " %ld (inflight keys),"
                    " %ld (inflight lookups),"
                    " %ld (loaded keys),"
                    " %ld (loaded values),"
                    " %ld (flush in progress)",
                    KeyGroups.size(),
                    InflightKeys.size(),
                    InflightLookups.size(),
                    LoadedKeys.size(),
                    LoadedValues.size(),
                    FlushInProgress);
        }

    private:
        TUnboxedValue Stream;
        const TSelf* Self;
        TComputationContext& Ctx;
        THolder<NYql::IYtflowLookupProvider> YtflowLookupProvider;

        TMKQLHashMap<
            TUnboxedValue, TKeyGroup,
            TValueHasher, TValueEqual
        > KeyGroups;

        TVector<TUnboxedValue> InflightKeys;
        std::deque<TLookupState> InflightLookups;

        ui32 InflightRowCount = 0;

        TVector<TUnboxedValue> LoadedKeys;
        TVector<TVector<TUnboxedValue>> LoadedValues;

        ui32 CurrentLoadedKeyIndex = 0;
        ui32 CurrentStreamRowIndex = 0;
        ui32 CurrentLoadedValueIndex = 0;

        bool FlushInProgress = false;

        EFetchStatus InputStatus = EFetchStatus::Ok;
    };

public:
    TYtflowLookupJoinWrapper(
        TComputationMutables& mutables,
        IComputationNode* stream,
        bool streamFromLeftSide,
        EJoinKind joinKind,
        ERowSelectionMode rowSelectionMode,
        THolder<NYql::IYtflowLookupProviderFactory> ytflowLookupProviderFactory,
        ui64 inflightRowLimit,
        ui64 inflightLookupLimit,
        TDuration lookupTimeout,
        TVector<ui32> streamKeyIndices,
        TKeyTypes streamKeyTypes,
        TVector<ui32> streamInputIndices,
        TVector<ui32> streamOutputIndices,
        TVector<ui32> lookupSourceInputIndices,
        TVector<ui32> lookupSourceOutputIndices
    )
        : TBase(mutables)
        , Stream(stream)
        , StreamFromLeftSide(streamFromLeftSide)
        , JoinKind(joinKind)
        , RowSelectionMode(rowSelectionMode)
        , YtflowLookupProviderFactory(std::move(ytflowLookupProviderFactory))
        , InflightRowLimit(inflightRowLimit)
        , InflightLookupLimit(inflightLookupLimit)
        , LookupTimeout(lookupTimeout)
        , StreamKeyIndices(std::move(streamKeyIndices))
        , StreamKeyTypes(std::move(streamKeyTypes))
        , StreamInputIndices(std::move(streamInputIndices))
        , StreamOutputIndices(std::move(streamOutputIndices))
        , LookupSourceInputIndices(std::move(lookupSourceInputIndices))
        , LookupSourceOutputIndices(std::move(lookupSourceOutputIndices))
        , ResultStruct(mutables)
        , StreamKeyStruct(mutables)
    {
        EffectiveJoinKind = JoinKind;

        if (!StreamFromLeftSide) {
            switch (JoinKind) {
            case EJoinKind::Right:
                EffectiveJoinKind = EJoinKind::Left;
                break;

            case EJoinKind::RightOnly:
                EffectiveJoinKind = EJoinKind::LeftOnly;
                break;

            case EJoinKind::RightSemi:
                EffectiveJoinKind = EJoinKind::LeftSemi;
                break;

            case EJoinKind::Inner:
                EffectiveJoinKind = EJoinKind::Inner;
                break;

            default:
                MKQL_ENSURE(
                    false,
                    "Unexpected join kind: "
                        << static_cast<ui32>(JoinKind));
            }
        }
    }

    void RegisterDependencies() const override {
        DependsOn(Stream);
    }

    NYql::NUdf::TUnboxedValue DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TStreamValue>(Stream->GetValue(ctx), this, ctx);
    }

private:
    IComputationNode* Stream;
    bool StreamFromLeftSide;
    EJoinKind JoinKind;
    EJoinKind EffectiveJoinKind;
    ERowSelectionMode RowSelectionMode;
    const THolder<NYql::IYtflowLookupProviderFactory> YtflowLookupProviderFactory;

    ui64 InflightRowLimit;
    ui64 InflightLookupLimit;
    TDuration LookupTimeout;

    TVector<ui32> StreamKeyIndices;
    TKeyTypes StreamKeyTypes;

    TVector<ui32> StreamInputIndices;
    TVector<ui32> StreamOutputIndices;
    TVector<ui32> LookupSourceInputIndices;
    TVector<ui32> LookupSourceOutputIndices;

    TContainerCacheOnContext ResultStruct;
    TContainerCacheOnContext StreamKeyStruct;
};

} // anonymous namespace

IComputationNode* WrapYtflowLookupJoin(
    TCallable& callable,
    const TComputationNodeFactoryContext& ctx,
    const NYql::IYtflowLookupProviderRegistry& ytflowLookupProviderRegistry
) {
    MKQL_ENSURE(
        callable.GetInputsCount() == 7,
        "Unexpected inputs count: " << callable.GetInputsCount());

    auto parseRowType = [](const TType* type) {
        MKQL_ENSURE(
            type->IsStream() || type->IsList(),
            "Unexpected type: " << type->GetKindAsStr());

        const auto* rowType = type->IsStream()
            ? static_cast<const TStreamType*>(type)->GetItemType()
            : static_cast<const TListType*>(type)->GetItemType();

        MKQL_ENSURE(
            rowType->IsStruct(),
            "Unexpected type: " << rowType->GetKindAsStr());

        const auto* structType = static_cast<const TStructType*>(rowType);

        return structType;
    };

    auto* stream = LocateNode(ctx.NodeLocator, callable, 0);
    const auto* streamRowType = parseRowType(callable.GetInput(0).GetStaticType());

    auto* wrappedLookupSourceType = AS_CALLABLE("Nop", callable.GetInput(1));
    const auto* lookupSourceRowType = parseRowType(
        wrappedLookupSourceType->GetType()->GetReturnType());

    auto* lookupSourceArgs = AS_VALUE(TTupleLiteral, callable.GetInput(2));
    MKQL_ENSURE(
        lookupSourceArgs->GetValuesCount() == 2,
        "Unexpected values count: " << lookupSourceArgs->GetValuesCount());

    auto providerData = lookupSourceArgs->GetValue(0);
    auto provider = AS_VALUE(TDataLiteral, providerData)->AsValue().AsStringRef();

    auto providerLookupSourceArgs = lookupSourceArgs->GetValue(1);

    auto joinKindData = callable.GetInput(3);
    auto joinKind = GetJoinKind(
        AS_VALUE(TDataLiteral, joinKindData)->AsValue().Get<ui32>());

    auto parseScope = [](const TRuntimeNode& node) {
        auto tupleLiteral = AS_VALUE(TTupleLiteral, node);
        MKQL_ENSURE(
            tupleLiteral->GetValuesCount() == 4,
            "Unexpected values count: " << tupleLiteral->GetValuesCount());

        auto labelData = tupleLiteral->GetValue(0);
        auto label = AS_VALUE(TDataLiteral, labelData)->AsValue().AsStringRef();

        auto sideData = tupleLiteral->GetValue(1);
        auto side = AS_VALUE(TDataLiteral, sideData)->AsValue().AsStringRef();

        TVector<TString> keys;

        auto keysData = tupleLiteral->GetValue(2);
        auto* keysTupleLiteral = AS_VALUE(TTupleLiteral, keysData);

        for (size_t index = 0; index < keysTupleLiteral->GetValuesCount(); ++index) {
            auto key = AS_VALUE(TDataLiteral, keysTupleLiteral->GetValue(index))
                ->AsValue()
                .AsStringRef();

            keys.push_back(TString(key.data(), key.size()));
        }

        auto rowSelectionModeData = tupleLiteral->GetValue(3);

        auto rowSelectionMode = static_cast<ERowSelectionMode>(
            AS_VALUE(TDataLiteral, rowSelectionModeData)->AsValue().Get<ui32>());

        return TLookupJoinScope{
            .Label = TString(label.data(), label.size()),
            .IsLeftSide = TStringBuf(side.data(), side.size()) == "left",
            .Keys = std::move(keys),
            .RowSelectionMode = rowSelectionMode,
        };
    };

    auto streamScope = parseScope(callable.GetInput(4));
    streamScope.RowType = streamRowType;

    auto lookupSourceScope = parseScope(callable.GetInput(5));
    lookupSourceScope.RowType = lookupSourceRowType;

    auto settingsData = callable.GetInput(6);
    auto* settingsTupleLiteral = AS_VALUE(TTupleLiteral, settingsData);
    MKQL_ENSURE(
        settingsTupleLiteral->GetValuesCount() == 3,
        "Unexpected values count: " << settingsTupleLiteral->GetValuesCount());

    auto inflightRowLimitData = settingsTupleLiteral->GetValue(0);
    auto inflightRowLimit = AS_VALUE(TDataLiteral, inflightRowLimitData)
        ->AsValue().Get<ui64>();

    auto inflightLookupLimitData = settingsTupleLiteral->GetValue(1);
    auto inflightLookupLimit = AS_VALUE(TDataLiteral, inflightLookupLimitData)
        ->AsValue().Get<ui64>();

    auto lookupTimeoutData = settingsTupleLiteral->GetValue(2);
    auto lookupTimeoutMilliSeconds = AS_VALUE(TDataLiteral, lookupTimeoutData)
        ->AsValue().Get<ui64>();

    auto lookupTimeout = TDuration::MilliSeconds(lookupTimeoutMilliSeconds);

    auto ytflowLookupProviderFactoryCreationContext = NYql::IYtflowLookupProviderRegistry::TFactoryCreationContext{
        .LookupSourceArgs = providerLookupSourceArgs,
        .LookupSourceRowSelectionMode = lookupSourceScope.RowSelectionMode,
        .StreamKeys = streamScope.Keys,
        .StreamRowType = streamRowType,
        .LookupSourceKeys = lookupSourceScope.Keys,
        .LookupSourceRowType = lookupSourceRowType,
        .TypeEnvironment = ctx.Env,
        .SecureParamsProvider = ctx.SecureParamsProvider,
    };

    auto ytflowLookupProviderFactory = ytflowLookupProviderRegistry.CreateFactory(
        TString(provider.data(), provider.size()),
        ytflowLookupProviderFactoryCreationContext);

    TVector<ui32> streamKeyIndices;

    for (ui32 index = 0; index < streamScope.Keys.size(); ++index) {
        const auto& key = streamScope.Keys[index];
        auto keyIndex = streamScope.RowType->GetMemberIndex(key);
        streamKeyIndices.push_back(keyIndex);
    }

    Sort(streamKeyIndices);

    TKeyTypes streamKeyTypes;

    for (const auto& keyIndex : streamKeyIndices) {
        auto* keyType = streamScope.RowType->GetMemberType(keyIndex);
        bool isOptional;
        auto* unpacked = UnpackOptional(keyType, isOptional);
        MKQL_ENSURE(unpacked->IsData(), "Composite key types are not supported yet");

        streamKeyTypes.emplace_back(
            *AS_TYPE(TDataType, unpacked)->GetDataSlot(),
            isOptional
        );
    }

    auto* outputRowType = parseRowType(callable.GetType()->GetReturnType());

    THashSet<TString> outputRowMembers;
    for (
        ui32 memberIndex = 0;
        memberIndex < outputRowType->GetMembersCount();
        ++memberIndex
    ) {
        auto memberName = TString(outputRowType->GetMemberName(memberIndex));
        outputRowMembers.emplace(std::move(memberName));
    }

    auto fillIndices = [&](const TString& label, const auto* inputRowType) {
        TVector<ui32> inputIndices;
        TVector<ui32> outputIndices;

        for (
            ui32 memberIndex = 0;
            memberIndex < inputRowType->GetMembersCount();
            ++memberIndex
        ) {
            auto memberName = TString(inputRowType->GetMemberName(memberIndex));
            TString fullMemberName = label.empty()
                ? std::move(memberName)
                : Join(".", label, memberName);

            if (outputRowMembers.contains(fullMemberName)) {
                auto outputMemberIndex = outputRowType->FindMemberIndex(fullMemberName);
                MKQL_ENSURE(outputMemberIndex, "Unknown member name: " << fullMemberName);

                inputIndices.push_back(memberIndex);
                outputIndices.push_back(outputMemberIndex.GetRef());
            }
        }

        return std::pair(std::move(inputIndices), std::move(outputIndices));
    };

    auto [streamInputIndices, streamOutputIndices] = fillIndices(
        streamScope.Label,
        streamScope.RowType);

    auto [lookupSourceInputIndices, lookupSourceOutputIndices] = fillIndices(
        lookupSourceScope.Label,
        lookupSourceScope.RowType);

    return new TYtflowLookupJoinWrapper(
        ctx.Mutables,
        std::move(stream),
        streamScope.IsLeftSide,
        joinKind,
        lookupSourceScope.RowSelectionMode,
        std::move(ytflowLookupProviderFactory),
        inflightRowLimit,
        inflightLookupLimit,
        lookupTimeout,
        std::move(streamKeyIndices),
        std::move(streamKeyTypes),
        std::move(streamInputIndices),
        std::move(streamOutputIndices),
        std::move(lookupSourceInputIndices),
        std::move(lookupSourceOutputIndices)
    );
}

} // namespace NKikimr::NMiniKQL
