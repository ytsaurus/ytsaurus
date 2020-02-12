#include "list_operations.h"

#include <yt/client/security_client/acl.h>
#include <yt/client/security_client/helpers.h>

#include <yt/ytlib/scheduler/helpers.h>

#include <yt/core/yson/pull_parser.h>
#include <yt/core/yson/pull_parser_deserialize.h>
#include <yt/core/yson/token_writer.h>

namespace NYT::NApi::NNative {

using namespace NScheduler;
using namespace NSecurityClient;
using namespace NTableClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

bool TListOperationsCountingFilter::Filter(
    const std::optional<std::vector<TString>>& pools,
    TStringBuf user,
    EOperationState state,
    EOperationType type,
    i64 count)
{
    UserCounts[user] += count;

    if (Options_.UserFilter && *Options_.UserFilter != user) {
        return false;
    }

    if (pools) {
        for (const auto& pool : *pools) {
            PoolCounts[pool] += count;
        }
    }

    if (Options_.Pool && (!pools || std::find(pools->begin(), pools->end(), *Options_.Pool) == pools->end())) {
        return false;
    }

    StateCounts[state] += count;

    if (Options_.StateFilter && *Options_.StateFilter != state) {
        return false;
    }

    TypeCounts[type] += count;

    if (Options_.TypeFilter && *Options_.TypeFilter != type) {
        return false;
    }

    return true;
}

bool TListOperationsCountingFilter::FilterByFailedJobs(bool hasFailedJobs, i64 count)
{
    if (hasFailedJobs) {
        FailedJobsCount += count;
    }
    return !Options_.WithFailedJobs || (*Options_.WithFailedJobs == hasFailedJobs);
}

TListOperationsCountingFilter::TListOperationsCountingFilter(const TListOperationsOptions& options)
    : Options_(options)
{ }

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
        // COMPAT(gritukan)
        if (Annotations_) {
            IMapNodePtr RuntimeParametersMapNode;
            if (Operation_.RuntimeParameters) {
                RuntimeParametersMapNode = ConvertToNode(Operation_.RuntimeParameters)->AsMap();
            } else {
                RuntimeParametersMapNode = GetEphemeralNodeFactory()->CreateMap();
            }
            RuntimeParametersMapNode->AddChild("annotations", ConvertToNode(Annotations_));

            Operation_.RuntimeParameters = ConvertToYsonString(RuntimeParametersMapNode);
        }
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

    // COMPAT(gritukan)
    void OnAnnotations(TYsonPullParserCursor* cursor)
    {
        TransferAndGetYson("annotations", Annotations_, cursor);
    }

private:
    TOperation& Operation_;
    const THashSet<TString>& Attributes_;

    TYsonString Annotations_;

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
        if (key == AsStringBuf("key")) {
            cursor->Next();
            consumer->OnId(ExtractTo<TOperationId>(cursor));
        } else if (key == AsStringBuf("operation_type")) {
            cursor->Next();
            consumer->OnType(ExtractTo<EOperationType>(cursor));
        } else if (key == AsStringBuf("state")) {
            cursor->Next();
            consumer->OnState(ExtractTo<EOperationState>(cursor));
        } else if (key == AsStringBuf("start_time")) {
            cursor->Next();
            consumer->OnStartTime(ExtractTo<TInstant>(cursor));
        } else if (key == AsStringBuf("finish_time")) {
            cursor->Next();
            consumer->OnFinishTime(ExtractTo<TInstant>(cursor));
        } else if (key == AsStringBuf("authenticated_user")) {
            cursor->Next();
            EnsureYsonToken("authenticated_user", *cursor, EYsonItemType::StringValue);
            consumer->OnAuthenticatedUser((*cursor)->UncheckedAsString());
            cursor->Next();
        } else if (key == AsStringBuf("brief_spec")) {
            cursor->Next();
            consumer->OnBriefSpec(cursor);
        } else if (key == AsStringBuf("spec")) {
            cursor->Next();
            consumer->OnSpec(cursor);
        } else if (key == AsStringBuf("full_spec")) {
            cursor->Next();
            consumer->OnFullSpec(cursor);
        } else if (key == AsStringBuf("unrecognized_spec")) {
            cursor->Next();
            consumer->OnUnrecognizedSpec(cursor);
        } else if (key == AsStringBuf("brief_progress")) {
            cursor->Next();
            consumer->OnBriefProgress(cursor);
        } else if (key == AsStringBuf("progress")) {
            cursor->Next();
            consumer->OnProgress(cursor);
        } else if (key == AsStringBuf("runtime_parameters")) {
            cursor->Next();
            consumer->OnRuntimeParameters(cursor);
        } else if (key == AsStringBuf("suspended")) {
            cursor->Next();
            consumer->OnSuspended(ExtractTo<bool>(cursor));
       } else if (key == AsStringBuf("events")) {
            cursor->Next();
            consumer->OnEvents(cursor);
        } else if (key == AsStringBuf("result")) {
            cursor->Next();
            consumer->OnResult(cursor);
        } else if (key == AsStringBuf("slot_index_per_pool_tree")) {
            cursor->Next();
            consumer->OnSlotIndexPerPoolTree(cursor);
        } else if (key == AsStringBuf("alerts")) {
            cursor->Next();
            consumer->OnAlerts(cursor);
        } else if (key == AsStringBuf("annotations")) {
            cursor->Next();
            consumer->OnAnnotations(cursor);
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

static TListOperationsFilter::TBriefProgress ParseBriefProgress(TYsonPullParserCursor* cursor)
{
    TListOperationsFilter::TBriefProgress result = {};
    cursor->ParseMap([&] (TYsonPullParserCursor* cursor) {
        YT_VERIFY((*cursor)->GetType() == EYsonItemType::StringValue);
        auto key = (*cursor)->UncheckedAsString();
        if (key == AsStringBuf("build_time")) {
            cursor->Next();
            result.BuildTime = ExtractTo<TInstant>(cursor);
        } else if (key == AsStringBuf("jobs")) {
            cursor->Next();
            cursor->ParseMap([&] (TYsonPullParserCursor* cursor) {
                auto innerKey = (*cursor)->UncheckedAsString();
                if (innerKey == AsStringBuf("failed")) {
                    cursor->Next();
                    result.HasFailedJobs = ExtractTo<i64>(cursor) > 0;
                } else {
                    cursor->Next();
                    cursor->SkipComplexValue();
                }
            });
        } else {
            cursor->Next();
            cursor->SkipComplexValue();
        }
    });
    return result;
}

class TFilteringConsumer
{
public:
    TFilteringConsumer(
        TListOperationsCountingFilter& countingFilter,
        const TListOperationsOptions& options)
        : CountingFilter_(countingFilter)
        , Options_(options)
    { }

    std::optional<TListOperationsFilter::TLightOperation> ExtractCurrent()
    {
        if (PassedFilter_) {
            return std::move(CurrentOperation_);
        } else {
            return {};
        }
    }

    void OnBeginOperation()
    {
        Pools_.clear();
        HasAcl_ = false;
        SubstringFound_ = false;
        CurrentOperation_ = {};
    }

    void OnEndOperation()
    {
        PassedFilter_ = Filter();
    }

    void OnId(TOperationId id)
    {
        CurrentOperation_.Id_ = id;
        if (Options_.SubstrFilter) {
            TextFactorsBuilder_.Reset();
            FormatValue(&TextFactorsBuilder_, id, "%v");
            SearchSubstring(TextFactorsBuilder_.GetBuffer());
        }
    }

    void OnType(EOperationType type)
    {
        Type_ = type;
        if (Options_.SubstrFilter) {
            TextFactorsBuilder_.Reset();
            FormatValue(&TextFactorsBuilder_, type, "%lv");
            SearchSubstring(TextFactorsBuilder_.GetBuffer());
        }
    }

    void OnState(EOperationState state)
    {
        State_ = state;
        if (Options_.SubstrFilter) {
            TextFactorsBuilder_.Reset();
            FormatValue(&TextFactorsBuilder_, state, "%lv");
            SearchSubstring(TextFactorsBuilder_.GetBuffer());
        }
    }

    void OnStartTime(TInstant startTime)
    {
        CurrentOperation_.StartTime_ = startTime;
    }

    void OnFinishTime(TInstant finishTime)
    { }

    void OnAuthenticatedUser(TStringBuf authenticatedUser)
    {
        AuthenticatedUser_ = authenticatedUser;
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
            if (key == AsStringBuf("title")) {
                cursor->Next();
                EnsureYsonToken("title", *cursor, EYsonItemType::StringValue);
                SearchSubstring((*cursor)->UncheckedAsString());
                cursor->Next();
            } else if (key == AsStringBuf("input_table_paths") || key == AsStringBuf("output_table_paths")) {
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

    void OnFullSpec(TYsonPullParserCursor* cursor)
    {
        cursor->SkipComplexValue();
    }

    void OnUnrecognizedSpec(TYsonPullParserCursor* cursor)
    {
        cursor->SkipComplexValue();
    }

    void OnBriefProgress(TYsonPullParserCursor* cursor)
    {
        CurrentOperation_.BriefProgress_ = ParseBriefProgress(cursor);
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
            if (Options_.AccessFilter && key == AsStringBuf("acl")) {
                cursor->Next();
                HasAcl_ = true;
                Deserialize(Acl_, cursor);
            } else if (key == AsStringBuf("scheduling_options_per_pool_tree")) {
                cursor->Next();
                cursor->ParseMap([&] (TYsonPullParserCursor* cursor) {
                    YT_VERIFY((*cursor)->GetType() == EYsonItemType::StringValue);
                    cursor->Next();
                    cursor->ParseMap([&] (TYsonPullParserCursor* cursor) {
                        YT_VERIFY((*cursor)->GetType() == EYsonItemType::StringValue);
                        auto innerKey = (*cursor)->UncheckedAsString();
                        if (innerKey == AsStringBuf("pool")) {
                            cursor->Next();
                            Pools_.push_back(ExtractTo<TString>(cursor));
                            SearchSubstring(Pools_.back());
                        } else {
                            cursor->Next();
                            cursor->SkipComplexValue();
                        }
                    });
                });
            } else if (key == AsStringBuf("annotations")) {
                cursor->Next();
                OnAnnotations(cursor);
            } else {
                cursor->Next();
                cursor->SkipComplexValue();
            }
        });
    }

    void OnSuspended(bool suspended)
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

    // COMPAT(gritukan): Move it to `OnRuntimeParameters'.
    void OnAnnotations(TYsonPullParserCursor* cursor)
    {
        if (!Options_.SubstrFilter || SubstringFound_) {
            cursor->SkipComplexValue();
            return;
        }
        {
            Annotations_.clear();
            TStringOutput output(Annotations_);
            TYsonWriter writer(&output, EYsonFormat::Text);
            cursor->TransferComplexValue(&writer);
        }
        SearchSubstring(Annotations_);
    }

private:
    TListOperationsCountingFilter& CountingFilter_;
    const TListOperationsOptions& Options_;

    bool PassedFilter_ = false;
    TListOperationsFilter::TLightOperation CurrentOperation_ = {};
    NScheduler::EOperationState State_ = {};
    NScheduler::EOperationType Type_ = {};
    TString AuthenticatedUser_;
    std::vector<TString> Pools_;
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
            Options_.SubstrFilter->begin(),
            Options_.SubstrFilter->end(),
            [] (char left, char right) {
                return std::tolower(left) == std::tolower(right);
            });
        SubstringFound_ = (it != haystack.end());
    }

    bool Filter()
    {
        if ((Options_.FromTime && CurrentOperation_.StartTime_ < *Options_.FromTime) ||
            (Options_.ToTime && CurrentOperation_.StartTime_ >= *Options_.ToTime))
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

        auto state = State_;
        if (state != EOperationState::Pending && IsOperationInProgress(state)) {
            state = EOperationState::Running;
        }

        return CountingFilter_.Filter(Pools_, AuthenticatedUser_, state, Type_, /* count = */ 1);
    }
};

////////////////////////////////////////////////////////////////////////////////

TListOperationsFilter::TListOperationsFilter(
    const std::vector<TYsonString>& operations,
    TListOperationsCountingFilter& countingFilter,
    const TListOperationsOptions& options)
    : CountingFilter_(countingFilter)
    , Options_(options)
{
    TFilteringConsumer filteringConsumer(CountingFilter_, Options_);

    TString singleOperationYson;
    for (const auto& operationsYson : operations) {
        RunYsonPullParser(operationsYson.GetData(), [&] (TYsonPullParserCursor* cursor) {
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
                if (auto lightOperation = filteringConsumer.ExtractCurrent()) {
                    // Copy without COW (it is faster: otherwise on the next iteration
                    // |singleOperationYson| will be incrementally reallocated during |TransferComplexValue}).
                    lightOperation->Yson_ = singleOperationYson.copy();
                    LightOperations_.push_back(std::move(*lightOperation));
                }
            });
        });
    }
}

void TListOperationsFilter::OnBriefProgressFinished()
{
    std::vector<TLightOperation> filtered;
    for (const auto& operation : LightOperations_) {
        const auto& [hasFailedJobs, buildTime] = operation.BriefProgress_;
        if (!CountingFilter_.FilterByFailedJobs(hasFailedJobs, /* count = */ 1)) {
            continue;
        }
        if (Options_.CursorTime &&
            ((Options_.CursorDirection == EOperationSortDirection::Past && operation.StartTime_ >= *Options_.CursorTime) ||
            (Options_.CursorDirection == EOperationSortDirection::Future && operation.StartTime_ <= *Options_.CursorTime)))
        {
            continue;
        }
        filtered.push_back(operation);
    }

    auto operationsToRetain = static_cast<i64>(Options_.Limit) + 1;
    if (filtered.size() > operationsToRetain) {
        // Leave only |operationsToRetain| operations:
        // either oldest (|cursor_direction == "future"|) or newest (|cursor_direction == "past"|).
        std::nth_element(
            filtered.begin(),
            filtered.begin() + operationsToRetain,
            filtered.end(),
            [&] (const TLightOperation& lhs, const TLightOperation& rhs) {
                return
                    (Options_.CursorDirection == EOperationSortDirection::Future && lhs.StartTime_ < rhs.StartTime_) ||
                    (Options_.CursorDirection == EOperationSortDirection::Past && lhs.StartTime_ > rhs.StartTime_);
            });
        filtered.resize(operationsToRetain);
    }

    LightOperations_.swap(filtered);
}

std::vector<TOperation> TListOperationsFilter::BuildOperations(const THashSet<TString>& attributes) const
{
    std::vector<TOperation> operations;
    operations.reserve(LightOperations_.size());
    for (const auto& lightOperation : LightOperations_) {
        TConstructingOperationConsumer consumer(operations.emplace_back(), attributes);
        RunYsonPullParser(lightOperation.Yson_, ParseOperationToConsumer<TConstructingOperationConsumer>, &consumer);
    }
    return operations;
}

i64 TListOperationsFilter::GetCount() const
{
    return static_cast<i64>(LightOperations_.size());
}

////////////////////////////////////////////////////////////////////////////////

NObjectClient::TOperationId TListOperationsFilter::TLightOperation::GetId() const
{
    return Id_;
}

void TListOperationsFilter::TLightOperation::UpdateBriefProgress(TStringBuf briefProgressYson)
{
    auto newBriefProgress = RunYsonPullParser(briefProgressYson, ParseBriefProgress);
    if (newBriefProgress.BuildTime >= BriefProgress_.BuildTime) {
        BriefProgress_ = newBriefProgress;
    }
}

void TListOperationsFilter::TLightOperation::SetYson(TString yson)
{
    Yson_ = std::move(yson);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
