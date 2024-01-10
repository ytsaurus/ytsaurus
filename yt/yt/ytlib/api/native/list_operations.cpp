#include "list_operations.h"

#include <yt/yt/client/security_client/acl.h>
#include <yt/yt/client/security_client/helpers.h>

#include <yt/yt/ytlib/scheduler/helpers.h>

#include <yt/yt/core/yson/pull_parser.h>
#include <yt/yt/core/yson/pull_parser_deserialize.h>
#include <yt/yt/core/yson/token_writer.h>

#include <experimental/functional>

namespace NYT::NApi::NNative {

using namespace NScheduler;
using namespace NSecurityClient;
using namespace NTableClient;
using namespace NYTree;
using namespace NYson;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TListOperationsCountingFilter::TListOperationsCountingFilter(const TListOperationsOptions& options)
    : Options_(&options)
{ }

bool TListOperationsCountingFilter::Filter(
    const TCountingFilterAttributes& countingFilterAttributes,
    i64 count)
{
    YT_VERIFY(Options_);

    TCompactVector<EListOperationsCountingFilterType, TEnumTraits<EListOperationsCountingFilterType>::GetDomainSize()> failedFilters;
    bool poolTreeFailed = [&] {
        if (!Options_->PoolTree) {
            return false;
        }
        if (!countingFilterAttributes.PoolTreeToPool) {
            return true;
        }

        auto it = countingFilterAttributes.PoolTreeToPool->find(*Options_->PoolTree);
        return it == countingFilterAttributes.PoolTreeToPool->end() ||
            (Options_->Pool && it->second != Options_->Pool);
    }();

    if (poolTreeFailed) {
        failedFilters.push_back(EListOperationsCountingFilterType::PoolTree);
    }

    bool poolFailed = [&] {
        if (!Options_->Pool) {
            return false;
        }

        const auto& pools = countingFilterAttributes.Pools;
        return !pools ||
            std::find(pools->begin(), pools->end(), *Options_->Pool) == pools->end();
    }();

    if (poolFailed) {
        failedFilters.push_back(EListOperationsCountingFilterType::Pool);
    }

    if (Options_->UserFilter && *Options_->UserFilter != countingFilterAttributes.User) {
        failedFilters.push_back(EListOperationsCountingFilterType::User);
    }

    if (Options_->StateFilter && *Options_->StateFilter != countingFilterAttributes.State) {
        failedFilters.push_back(EListOperationsCountingFilterType::OperationState);
    }

    if (Options_->TypeFilter && *Options_->TypeFilter != countingFilterAttributes.Type) {
        failedFilters.push_back(EListOperationsCountingFilterType::OperationType);
    }

    if (Options_->WithFailedJobs && *Options_->WithFailedJobs != countingFilterAttributes.HasFailedJobs) {
        failedFilters.push_back(EListOperationsCountingFilterType::WithFailedJobs);
    }

    if (std::ssize(failedFilters) > 1) {
        return false;
    }

    auto shouldIncrementFilterCounter = [&] (EListOperationsCountingFilterType type) {
        return failedFilters.empty() || failedFilters[0] == type;
    };

    if (shouldIncrementFilterCounter(EListOperationsCountingFilterType::PoolTree) && countingFilterAttributes.PoolTreeToPool) {
        for (const auto& [poolTree, pool] : *countingFilterAttributes.PoolTreeToPool) {
            if (!Options_->Pool || pool == *Options_->Pool) {
                PoolTreeCounts_[poolTree] += count;
            }
        }
    }

    if (shouldIncrementFilterCounter(EListOperationsCountingFilterType::Pool) && countingFilterAttributes.Pools) {
        for (const auto& pool : *countingFilterAttributes.Pools) {
            PoolCounts_[pool] += count;
        }
    }

    if (shouldIncrementFilterCounter(EListOperationsCountingFilterType::User)) {
        UserCounts_[countingFilterAttributes.User] += count;
    }

    if (shouldIncrementFilterCounter(EListOperationsCountingFilterType::OperationState)) {
        StateCounts_[countingFilterAttributes.State] += count;
    }

    if (shouldIncrementFilterCounter(EListOperationsCountingFilterType::OperationType)) {
        TypeCounts_[countingFilterAttributes.Type] += count;
    }

    if (shouldIncrementFilterCounter(EListOperationsCountingFilterType::WithFailedJobs) && countingFilterAttributes.HasFailedJobs) {
        WithFailedJobsCount_ += count;
    }

    return failedFilters.empty();
}


void TListOperationsCountingFilter::MergeFrom(const TListOperationsCountingFilter& otherFilter)
{
    for (const auto& [poolTree, count] : otherFilter.PoolTreeCounts()) {
        PoolTreeCounts_[poolTree] += count;
    }
    for (const auto& [pool, count] : otherFilter.PoolCounts()) {
        PoolCounts_[pool] += count;
    }
    for (const auto& [user, count] : otherFilter.UserCounts()) {
        UserCounts_[user] += count;
    }
    for (auto operationState : TEnumTraits<EOperationState>::GetDomainValues()) {
        StateCounts_[operationState] += otherFilter.StateCounts()[operationState];
    }
    for (auto operationType : TEnumTraits<EOperationType>::GetDomainValues()) {
        TypeCounts_[operationType] += otherFilter.TypeCounts()[operationType];
    }
    WithFailedJobsCount_ += otherFilter.WithFailedJobsCount_;
}

////////////////////////////////////////////////////////////////////////////////

class TConstructingOperationConsumer
{
public:
    TConstructingOperationConsumer(TOperation& operation, const THashSet<TString>& attributes)
        : Operation_(operation)
        , Attributes_(attributes)
    { }

    void OnBeginOperation()
    { }

    void OnEndOperation()
    {
        if (!HeavyRuntimeParameters_) {
            return;
        }
        if (!Operation_.RuntimeParameters) {
            Operation_.RuntimeParameters = HeavyRuntimeParameters_;
            return;
        }

        auto heavyRuntimeParametersNode = ConvertTo<IMapNodePtr>(HeavyRuntimeParameters_);
        auto runtimeParametersNode =  ConvertTo<IMapNodePtr>(Operation_.RuntimeParameters);
        Operation_.RuntimeParameters = ConvertToYsonString(PatchNode(
            runtimeParametersNode,
            heavyRuntimeParametersNode));
    }

    void OnId(TOperationId id)
    {
        if (Attributes_.contains("id")) {
            Operation_.Id = id;
        }
    }

    void OnType(NScheduler::EOperationType type)
    {
        if (Attributes_.contains("type")) {
            Operation_.Type = type;
        }
    }

    void OnState(NScheduler::EOperationState state)
    {
        if (Attributes_.contains("state")) {
            Operation_.State = state;
        }
    }

    void OnStartTime(TInstant startTime)
    {
        if (Attributes_.contains("start_time")) {
            Operation_.StartTime = startTime;
        }
    }

    void OnFinishTime(TInstant finishTime)
    {
        if (Attributes_.contains("finish_time")) {
            Operation_.FinishTime = finishTime;
        }
    }

    void OnAuthenticatedUser(TStringBuf authenticatedUser)
    {
        if (Attributes_.contains("authenticated_user")) {
            Operation_.AuthenticatedUser = authenticatedUser;
        }
    }

    void OnBriefSpec(TYsonPullParserCursor* cursor)
    {
        TransferAndGetYson("brief_spec", Operation_.BriefSpec, cursor);
    }

    void OnSpec(TYsonPullParserCursor* cursor)
    {
        TransferAndGetYson("spec", Operation_.Spec, cursor);
    }

    void OnProvidedSpec(TYsonPullParserCursor* cursor)
    {
        TransferAndGetYson("provided_spec", Operation_.ProvidedSpec, cursor);
    }

    void OnFullSpec(TYsonPullParserCursor* cursor)
    {
        TransferAndGetYson("full_spec", Operation_.FullSpec, cursor);
    }

    void OnUnrecognizedSpec(TYsonPullParserCursor* cursor)
    {
        TransferAndGetYson("unrecognized_spec", Operation_.UnrecognizedSpec, cursor);
    }

    void OnBriefProgress(TYsonPullParserCursor* cursor)
    {
        TransferAndGetYson("brief_progress", Operation_.BriefProgress, cursor);
    }

    void OnProgress(TYsonPullParserCursor* cursor)
    {
        TransferAndGetYson("progress", Operation_.Progress, cursor);
    }

    void OnRuntimeParameters(TYsonPullParserCursor* cursor)
    {
        TransferAndGetYson("runtime_parameters", Operation_.RuntimeParameters, cursor);
    }

    void OnHeavyRuntimeParameters(TYsonPullParserCursor* cursor)
    {
        // We don't use heavy_runtime_parameters here intentionally as it can't be contained in Attributes_
        TransferAndGetYson("runtime_parameters", HeavyRuntimeParameters_, cursor);
    }

    void OnSuspended(bool suspended)
    {
        if (Attributes_.contains("suspended")) {
            Operation_.Suspended = suspended;
        }
    }

    void OnEvents(TYsonPullParserCursor* cursor)
    {
        TransferAndGetYson("events", Operation_.Events, cursor);
    }

    void OnResult(TYsonPullParserCursor* cursor)
    {
        TransferAndGetYson("result", Operation_.Result, cursor);
    }

    void OnSlotIndexPerPoolTree(TYsonPullParserCursor* cursor)
    {
        TransferAndGetYson("slot_index_per_pool_tree", Operation_.SlotIndexPerPoolTree, cursor);
    }

    void OnAlerts(TYsonPullParserCursor* cursor)
    {
        TransferAndGetYson("alerts", Operation_.Alerts, cursor);
    }

    void OnTaskNames(TYsonPullParserCursor* cursor)
    {
        TransferAndGetYson("task_names", Operation_.TaskNames, cursor);
    }

    void OnExperimentAssignments(TYsonPullParserCursor* cursor)
    {
        TransferAndGetYson("experiment_assignments", Operation_.ExperimentAssignments, cursor);
    }

    void OnExperimentAssignmentNames(TYsonPullParserCursor* cursor)
    {
        TransferAndGetYson("experiment_assignment_names", Operation_.ExperimentAssignmentNames, cursor);
    }

    void OnControllerFeatures(TYsonPullParserCursor* cursor)
    {
        TransferAndGetYson("controller_features", Operation_.ControllerFeatures, cursor);
    }

    void OnHasFailedJobs(bool /*hasFailedJobs*/)
    { }

private:
    TOperation& Operation_;
    const THashSet<TString>& Attributes_;

    TYsonString Annotations_;
    TYsonString HeavyRuntimeParameters_;

private:
    void TransferAndGetYson(TStringBuf attribute, TYsonString& result, TYsonPullParserCursor* cursor)
    {
        if (!Attributes_.contains(attribute)) {
            cursor->SkipComplexValue();
            return;
        }
        TString data;
        {
            TStringOutput output(data);
            TCheckedInDebugYsonTokenWriter writer(&output);
            cursor->TransferComplexValue(&writer);
        }
        result = TYsonString(std::move(data));
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename TConsumer>
void ParseOperationToConsumer(TYsonPullParserCursor* cursor, TConsumer* consumer)
{
    consumer->OnBeginOperation();
    cursor->ParseAttributes([&] (TYsonPullParserCursor* cursor) {
        YT_VERIFY((*cursor)->GetType() == EYsonItemType::StringValue);
        auto key = (*cursor)->UncheckedAsString();
        if (key == TStringBuf("key")) {
            cursor->Next();
            consumer->OnId(ExtractTo<TOperationId>(cursor));
        } else if (key == TStringBuf("operation_type")) {
            cursor->Next();
            consumer->OnType(ExtractTo<EOperationType>(cursor));
        } else if (key == TStringBuf("state")) {
            cursor->Next();
            consumer->OnState(ExtractTo<EOperationState>(cursor));
        } else if (key == TStringBuf("start_time")) {
            cursor->Next();
            consumer->OnStartTime(ExtractTo<TInstant>(cursor));
        } else if (key == TStringBuf("finish_time")) {
            cursor->Next();
            consumer->OnFinishTime(ExtractTo<TInstant>(cursor));
        } else if (key == TStringBuf("authenticated_user")) {
            cursor->Next();
            EnsureYsonToken("authenticated_user", *cursor, EYsonItemType::StringValue);
            consumer->OnAuthenticatedUser((*cursor)->UncheckedAsString());
            cursor->Next();
        } else if (key == TStringBuf("brief_spec")) {
            cursor->Next();
            consumer->OnBriefSpec(cursor);
        } else if (key == TStringBuf("spec")) {
            cursor->Next();
            consumer->OnSpec(cursor);
        } else if (key == TStringBuf("provided_spec")) {
            cursor->Next();
            consumer->OnProvidedSpec(cursor);
        } else if (key == TStringBuf("experiment_assignments")) {
            cursor->Next();
            consumer->OnExperimentAssignments(cursor);
        } else if (key == TStringBuf("experiment_assignment_names")) {
            cursor->Next();
            consumer->OnExperimentAssignmentNames(cursor);
        } else if (key == TStringBuf("full_spec")) {
            cursor->Next();
            consumer->OnFullSpec(cursor);
        } else if (key == TStringBuf("unrecognized_spec")) {
            cursor->Next();
            consumer->OnUnrecognizedSpec(cursor);
        } else if (key == TStringBuf("progress")) {
            cursor->Next();
            consumer->OnProgress(cursor);
        } else if (key == TStringBuf("runtime_parameters")) {
            cursor->Next();
            consumer->OnRuntimeParameters(cursor);
        } else if (key == TStringBuf("heavy_runtime_parameters")) {
            cursor->Next();
            consumer->OnHeavyRuntimeParameters(cursor);
        } else if (key == TStringBuf("suspended")) {
            cursor->Next();
            consumer->OnSuspended(ExtractTo<bool>(cursor));
        } else if (key == TStringBuf("events")) {
            cursor->Next();
            consumer->OnEvents(cursor);
        } else if (key == TStringBuf("result")) {
            cursor->Next();
            consumer->OnResult(cursor);
        } else if (key == TStringBuf("slot_index_per_pool_tree")) {
            cursor->Next();
            consumer->OnSlotIndexPerPoolTree(cursor);
        } else if (key == TStringBuf("alerts")) {
            cursor->Next();
            consumer->OnAlerts(cursor);
        } else if (key == TStringBuf("task_names")) {
            cursor->Next();
            consumer->OnTaskNames(cursor);
        } else if (key == TStringBuf("controller_features")) {
            cursor->Next();
            consumer->OnControllerFeatures(cursor);
        } else if (key == "has_failed_jobs") {
            cursor->Next();
            consumer->OnHasFailedJobs(ExtractTo<bool>(cursor));
        } else {
            cursor->Next();
            cursor->SkipComplexValue();
        }
    });
    cursor->SkipComplexValue();
    consumer->OnEndOperation();
}

template <typename TFunction, typename ...TArgs>
auto RunYsonPullParser(TStringBuf yson, TFunction function, TArgs&&... args)
{
    TMemoryInput input(yson);
    TYsonPullParser parser(&input, EYsonType::Node);
    TYsonPullParserCursor cursor(&parser);
    return function(&cursor, std::forward<TArgs>(args)...);
}

////////////////////////////////////////////////////////////////////////////////

class TFilteringConsumer
{
public:
    TFilteringConsumer(
        TListOperationsCountingFilter* countingFilter,
        const TListOperationsOptions& options)
        : CountingFilter_(countingFilter)
        , Options_(options)
    { }

    TListOperationsFilter::TLightOperation ExtractCurrent()
    {
        return std::move(CurrentOperation_);
    }

    void OnBeginOperation()
    {
        CurrentOperation_ = {};
        CurrentOperation_.FilterAttributes.PoolTreeToPool.emplace();
        CurrentOperation_.FilterAttributes.Pools.emplace();
        HasAcl_ = false;
        SubstringFound_ = false;
    }

    void OnEndOperation()
    {
        CurrentOperation_.FilterPassed = Filter();
    }

    void OnHasFailedJobs(bool operationHasFailedJobs)
    {
        CurrentOperation_.FilterAttributes.HasFailedJobs = operationHasFailedJobs;
    }

    void OnId(TOperationId id)
    {
        CurrentOperation_.Id = id;
        if (Options_.SubstrFilter) {
            TextFactorsBuilder_.Reset();
            FormatValue(&TextFactorsBuilder_, id, "%v");
            SearchSubstring(TextFactorsBuilder_.GetBuffer());
        }
    }

    void OnType(EOperationType type)
    {
        CurrentOperation_.FilterAttributes.Type = type;
        if (Options_.SubstrFilter) {
            TextFactorsBuilder_.Reset();
            FormatValue(&TextFactorsBuilder_, type, "%lv");
            SearchSubstring(TextFactorsBuilder_.GetBuffer());
        }
    }

    void OnState(EOperationState state)
    {
        CurrentOperation_.FilterAttributes.State = state;
        if (Options_.SubstrFilter) {
            TextFactorsBuilder_.Reset();
            FormatValue(&TextFactorsBuilder_, state, "%lv");
            SearchSubstring(TextFactorsBuilder_.GetBuffer());
        }
    }

    void OnStartTime(TInstant startTime)
    {
        CurrentOperation_.StartTime = startTime;
    }

    void OnFinishTime(TInstant /*finishTime*/)
    { }

    void OnAuthenticatedUser(TStringBuf authenticatedUser)
    {
        CurrentOperation_.FilterAttributes.User = authenticatedUser;
        if (Options_.SubstrFilter) {
            SearchSubstring(authenticatedUser);
        }
    }

    void OnBriefSpec(TYsonPullParserCursor* cursor)
    {
        if (!Options_.SubstrFilter) {
            cursor->SkipComplexValue();
            return;
        }
        cursor->ParseMap([this] (TYsonPullParserCursor* cursor) {
            YT_VERIFY((*cursor)->GetType() == EYsonItemType::StringValue);
            auto key = (*cursor)->UncheckedAsString();
            if (key == TStringBuf("title")) {
                cursor->Next();
                EnsureYsonToken("title", *cursor, EYsonItemType::StringValue);
                SearchSubstring((*cursor)->UncheckedAsString());
                cursor->Next();
            } else if (key == TStringBuf("input_table_paths") || key == TStringBuf("output_table_paths")) {
                cursor->Next();
                if ((*cursor)->GetType() == EYsonItemType::BeginAttributes) {
                    cursor->SkipAttributes();
                }
                bool isFirst = true;
                cursor->ParseList([&] (TYsonPullParserCursor* cursor) {
                    if (isFirst) {
                        isFirst = false;
                        EnsureYsonToken(
                            R"("input_table_paths" or "output_table_paths")",
                            *cursor,
                            EYsonItemType::StringValue);
                        SearchSubstring((*cursor)->UncheckedAsString());
                    }
                    cursor->Next();
                });
            } else {
                cursor->Next();
                cursor->SkipComplexValue();
            }
        });
    }

    void OnSpec(TYsonPullParserCursor* cursor)
    {
        cursor->SkipComplexValue();
    }

    void OnProvidedSpec(TYsonPullParserCursor* cursor)
    {
        cursor->SkipComplexValue();
    }

    void OnFullSpec(TYsonPullParserCursor* cursor)
    {
        cursor->SkipComplexValue();
    }

    void OnUnrecognizedSpec(TYsonPullParserCursor* cursor)
    {
        cursor->SkipComplexValue();
    }

    void OnProgress(TYsonPullParserCursor* cursor)
    {
        cursor->SkipComplexValue();
    }

    void OnRuntimeParameters(TYsonPullParserCursor* cursor)
    {
        cursor->ParseMap([&] (TYsonPullParserCursor* cursor) {
            YT_VERIFY((*cursor)->GetType() == EYsonItemType::StringValue);
            auto key = (*cursor)->UncheckedAsString();
            if (Options_.AccessFilter && key == TStringBuf("acl")) {
                cursor->Next();
                HasAcl_ = true;
                Deserialize(Acl_, cursor);
            } else if (key == TStringBuf("scheduling_options_per_pool_tree")) {
                cursor->Next();
                cursor->ParseMap([&] (TYsonPullParserCursor* cursor) {
                    YT_VERIFY((*cursor)->GetType() == EYsonItemType::StringValue);
                    auto poolTree = ExtractTo<TString>(cursor);
                    cursor->ParseMap([&] (TYsonPullParserCursor* cursor) {
                        YT_VERIFY((*cursor)->GetType() == EYsonItemType::StringValue);
                        auto innerKey = (*cursor)->UncheckedAsString();
                        if (innerKey == TStringBuf("pool")) {
                            cursor->Next();
                            auto pool = ExtractTo<TString>(cursor);
                            CurrentOperation_.FilterAttributes.PoolTreeToPool->emplace(poolTree, pool);
                            SearchSubstring(pool);
                            CurrentOperation_.FilterAttributes.Pools->push_back(std::move(pool));
                        } else {
                            cursor->Next();
                            cursor->SkipComplexValue();
                        }
                    });
                });
            // COMPAT(egor-gutrov)
            } else if (key == TStringBuf("annotations")) {
                cursor->Next();
                OnAnnotations(cursor);
            } else {
                cursor->Next();
                cursor->SkipComplexValue();
            }
        });
    }

    void OnHeavyRuntimeParameters(TYsonPullParserCursor* cursor)
    {
        cursor->ParseMap([&] (TYsonPullParserCursor* cursor) {
            YT_VERIFY((*cursor)->GetType() == EYsonItemType::StringValue);
            auto key = (*cursor)->UncheckedAsString();
            if (key == TStringBuf("annotations")) {
                cursor->Next();
                OnAnnotations(cursor);
            } else {
                cursor->Next();
                cursor->SkipComplexValue();
            }
        });
    }

    void OnSuspended(bool /*suspended*/)
    { }

    void OnEvents(TYsonPullParserCursor* cursor)
    {
        cursor->SkipComplexValue();
    }

    void OnResult(TYsonPullParserCursor* cursor)
    {
        cursor->SkipComplexValue();
    }

    void OnSlotIndexPerPoolTree(TYsonPullParserCursor* cursor)
    {
        cursor->SkipComplexValue();
    }

    void OnAlerts(TYsonPullParserCursor* cursor)
    {
        cursor->SkipComplexValue();
    }

    void OnTaskNames(TYsonPullParserCursor* cursor)
    {
        cursor->SkipComplexValue();
    }

    void OnExperimentAssignments(TYsonPullParserCursor* cursor)
    {
        cursor->SkipComplexValue();
    }

    void OnExperimentAssignmentNames(TYsonPullParserCursor* cursor)
    {
        if (!Options_.SubstrFilter) {
            cursor->SkipComplexValue();
            return;
        }
        cursor->ParseList([&] (TYsonPullParserCursor* cursor) {
            SearchSubstring((*cursor)->UncheckedAsString());
            cursor->Next();
        });
    }

    void OnControllerFeatures(TYsonPullParserCursor* cursor)
    {
        cursor->SkipComplexValue();
    }

private:
    TListOperationsCountingFilter* CountingFilter_;
    const TListOperationsOptions Options_;

    TListOperationsFilter::TLightOperation CurrentOperation_ = {};
    bool HasAcl_ = false;
    TSerializableAccessControlList Acl_;
    TString Annotations_;
    bool SubstringFound_ = false;
    TStringBuilder TextFactorsBuilder_;

private:
    void SearchSubstring(TStringBuf haystack)
    {
        if (!Options_.SubstrFilter || SubstringFound_) {
            return;
        }
        auto it = std::search(
            haystack.begin(),
            haystack.end(),
            std::experimental::boyer_moore_horspool_searcher(
                Options_.SubstrFilter->begin(),
                Options_.SubstrFilter->end(),
                [] (char ch) {
                    return std::hash<char>()(std::tolower(ch));
                },
                [] (char left, char right) {
                    return std::tolower(left) == std::tolower(right);
                }));
        SubstringFound_ = (it != haystack.end());
    }

    bool Filter()
    {
        if ((Options_.FromTime && CurrentOperation_.StartTime < *Options_.FromTime) ||
            (Options_.ToTime && CurrentOperation_.StartTime >= *Options_.ToTime))
        {
            return false;
        }

        if (Options_.AccessFilter) {
            if (!HasAcl_) {
                return false;
            }
            auto action = CheckPermissionsByAclAndSubjectClosure(
                Acl_,
                Options_.AccessFilter->SubjectTransitiveClosure,
                Options_.AccessFilter->Permissions);
            if (action != ESecurityAction::Allow) {
                return false;
            }
        }

        if (Options_.SubstrFilter && !SubstringFound_) {
            return false;
        }

        auto state = CurrentOperation_.FilterAttributes.State;
        if (state != EOperationState::Pending && IsOperationInProgress(state)) {
            state = EOperationState::Running;
        }

        return CountingFilter_->Filter(CurrentOperation_.FilterAttributes, /*count*/ 1);
    }

    void OnAnnotations(TYsonPullParserCursor* cursor)
    {
        if (!Options_.SubstrFilter || SubstringFound_) {
            cursor->SkipComplexValue();
        } else {
            {
                Annotations_.clear();
                TStringOutput output(Annotations_);
                TYsonWriter writer(&output, EYsonFormat::Text); // TODO(egor-gutrov): write binary yson here
                cursor->TransferComplexValue(&writer);
            }
            SearchSubstring(Annotations_);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TListOperationsFilter::TListOperationsFilter(
    const TListOperationsOptions& options,
    const IInvokerPtr& invoker,
    const NLogging::TLogger& logger)
    : Options_(options)
    , CountingFilter_(Options_)
    , Invoker_(invoker)
    , Logger(logger)
{ }

std::vector<TOperation> TListOperationsFilter::BuildOperations(const THashSet<TString>& attributes) const
{
    YT_LOG_DEBUG("Building final operations result");

    std::vector<TOperation> operations;
    operations.reserve(LightOperations_.size());
    for (const auto& lightOperation : LightOperations_) {
        TConstructingOperationConsumer consumer(operations.emplace_back(), attributes);
        RunYsonPullParser(lightOperation.Yson, ParseOperationToConsumer<TConstructingOperationConsumer>, &consumer);
    }

    YT_LOG_DEBUG("Operations result built (OperationCount: %v)", operations.size());

    return operations;
}

i64 TListOperationsFilter::GetCount() const
{
    return static_cast<i64>(LightOperations_.size());
}

void TListOperationsFilter::ParseResponses(std::vector<TYsonString> operationsResponses)
{
    YT_LOG_DEBUG("Parsing Cypress responses (ResponseCount: %v)", operationsResponses.size());

    std::vector<TFuture<TParseResult>> asyncResults;

    for (auto& operationsYson : operationsResponses) {
        asyncResults.push_back(
            BIND(&TListOperationsFilter::ParseOperationsYson, MakeStrong(this), Passed(std::move(operationsYson)))
            .AsyncVia(Invoker_)
            .Run());
    }

    std::vector<TParseResult> parseResults = WaitFor(AllSucceeded(asyncResults))
        .ValueOrThrow();

    i64 operationCount = 0;
    for (const auto& result : parseResults) {
        operationCount += result.Operations.size();
    }
    LightOperations_.reserve(operationCount);

    for (auto& result : parseResults) {
        for (auto& operation : result.Operations) {
            if (Options_.CursorTime &&
                ((Options_.CursorDirection == EOperationSortDirection::Past && operation.StartTime >= *Options_.CursorTime) ||
                (Options_.CursorDirection == EOperationSortDirection::Future && operation.StartTime <= *Options_.CursorTime)))
            {
                continue;
            }
            LightOperations_.emplace_back(std::move(operation));
        }
        CountingFilter_.MergeFrom(result.CountingFilter);
    }

    auto operationsToRetain = static_cast<i64>(Options_.Limit) + 1;
    if (std::ssize(LightOperations_) > operationsToRetain) {
        // Leave only |operationsToRetain| operations:
        // either oldest (|cursor_direction == "future"|) or newest (|cursor_direction == "past"|).
        std::nth_element(
            LightOperations_.begin(),
            LightOperations_.begin() + operationsToRetain,
            LightOperations_.end(),
            [&] (const TLightOperation& lhs, const TLightOperation& rhs) {
                return
                    (Options_.CursorDirection == EOperationSortDirection::Future && lhs.StartTime < rhs.StartTime) ||
                    (Options_.CursorDirection == EOperationSortDirection::Past && lhs.StartTime > rhs.StartTime);
            });
        LightOperations_.resize(operationsToRetain);
    }

    YT_LOG_DEBUG("Cypress responses parsed (OperationCount: %v)", LightOperations_.size());
}

TListOperationsFilter::TParseResult TListOperationsFilter::ParseOperationsYson(TYsonString operationsYson) const
{
    VERIFY_INVOKER_AFFINITY(Invoker_);

    std::vector<TLightOperation> operations;

    TListOperationsCountingFilter countingFilter(Options_);
    TFilteringConsumer filteringConsumer(&countingFilter, Options_);

    TString singleOperationYson;

    RunYsonPullParser(operationsYson.AsStringBuf(), [&operations, &filteringConsumer, &singleOperationYson] (TYsonPullParserCursor* cursor) {
        cursor->ParseList([&] (TYsonPullParserCursor* cursor) {
            singleOperationYson.clear();
            {
                TStringOutput output(singleOperationYson);
                TCheckedInDebugYsonTokenWriter writer(&output);
                cursor->TransferComplexValue(&writer);
                writer.Finish();
            }
            RunYsonPullParser(
                singleOperationYson,
                ParseOperationToConsumer<TFilteringConsumer>,
                &filteringConsumer);
            auto operation = filteringConsumer.ExtractCurrent();
            if (operation.FilterPassed) {
                // Copy without COW (it is faster: otherwise on the next iteration
                // |singleOperationYson| will be incrementally reallocated during |TransferComplexValue}).
                operation.Yson = singleOperationYson.copy();
                operations.push_back(std::move(operation));
            }
        });
    });

    return TParseResult{std::move(operations), std::move(countingFilter)};
}

const TListOperationsCountingFilter& TListOperationsFilter::GetCountingFilter() const
{
    return CountingFilter_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
