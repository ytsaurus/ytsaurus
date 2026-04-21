#include "private.h"

#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/yt/string/guid.h>

#include <util/digest/multi.h>

namespace NYT::NControllerAgent {

using namespace NTracing;

////////////////////////////////////////////////////////////////////////////////

void TJobMonitoringDescriptor::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, Guid);
    PHOENIX_REGISTER_FIELD(2, Index);
}

PHOENIX_DEFINE_TYPE(TJobMonitoringDescriptor);

void FormatValue(TStringBuilderBase* builder, const TJobMonitoringDescriptor& descriptor, TStringBuf /*spec*/)
{
    builder->AppendFormat(
        "%v/%v",
        descriptor.Guid,
        descriptor.Index);
}

////////////////////////////////////////////////////////////////////////////////

TTraceContextGuard CreateOperationTraceContextGuard(
    TString spanName,
    TOperationId operationId)
{
    auto traceContext = CreateTraceContextFromCurrent(std::move(spanName));
    traceContext->SetAllocationTags({{OperationIdTag, ToString(operationId)}});
    return TTraceContextGuard(std::move(traceContext));
}

////////////////////////////////////////////////////////////////////////////////

bool TCompositePendingJobCount::IsZero() const
{
    if (DefaultCount != 0) {
        return false;
    }

    for (const auto& [_, count] : CountByPoolTree) {
        if (count != 0) {
            return false;
        }
    }

    return true;
}

int TCompositePendingJobCount::GetJobCountFor(const TString& tree) const
{
    auto it = CountByPoolTree.find(tree);
    return it != CountByPoolTree.end()
        ? it->second
        : DefaultCount;
}

void TCompositePendingJobCount::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, DefaultCount);
    PHOENIX_REGISTER_FIELD(2, CountByPoolTree);
}

PHOENIX_DEFINE_TYPE(TCompositePendingJobCount);

void Serialize(const TCompositePendingJobCount& allocationCount, NYson::IYsonConsumer* consumer)
{
    NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            .Item("absolute").Value(allocationCount.DefaultCount)
            .Item("count_by_pool_tree").Value(allocationCount.CountByPoolTree)
        .EndMap();
}

void FormatValue(TStringBuilderBase* builder, const TCompositePendingJobCount& allocationCount, TStringBuf /*format*/)
{
    if (allocationCount.CountByPoolTree.empty()) {
        builder->AppendFormat("%v", allocationCount.DefaultCount);
    } else {
        builder->AppendFormat(
            "{DefaultCount: %v, CountByPoolTree: %v}",
            allocationCount.DefaultCount,
            allocationCount.CountByPoolTree);
    }
}

bool operator==(const TCompositePendingJobCount& lhs, const TCompositePendingJobCount& rhs)
{
    if (lhs.DefaultCount != rhs.DefaultCount) {
        return false;
    }

    if (lhs.CountByPoolTree.size() != rhs.CountByPoolTree.size()) {
        return false;
    }

    for (const auto& [tree, lhsCount] : lhs.CountByPoolTree) {
        auto rhsIt = rhs.CountByPoolTree.find(tree);
        if (rhsIt == rhs.CountByPoolTree.end()) {
            return false;
        }
        if (lhsCount != rhsIt->second) {
            return false;
        }
    }
    return true;
}

TCompositePendingJobCount operator+(const TCompositePendingJobCount& lhs, const TCompositePendingJobCount& rhs)
{
    TCompositePendingJobCount result;
    result.DefaultCount = lhs.DefaultCount + rhs.DefaultCount;
    for (const auto& [tree, lhsCount] : lhs.CountByPoolTree) {
        auto rhsIt = rhs.CountByPoolTree.find(tree);
        if (rhsIt == rhs.CountByPoolTree.end()) {
            result.CountByPoolTree[tree] = lhsCount;
        } else {
            result.CountByPoolTree[tree] = lhsCount + rhsIt->second;
        }
    }

    for (const auto& [tree, rhsCount] : rhs.CountByPoolTree) {
        if (result.CountByPoolTree.find(tree) == result.CountByPoolTree.end()) {
            result.CountByPoolTree[tree] = rhsCount;
        }
    }

    return result;
}

TCompositePendingJobCount operator-(const TCompositePendingJobCount& count)
{
    TCompositePendingJobCount result;
    result.DefaultCount = -count.DefaultCount;
    for (const auto& [tree, countPerTree] : count.CountByPoolTree) {
        result.CountByPoolTree[tree] = -countPerTree;
    }

    return result;
}

TCompositePendingJobCount operator-(const TCompositePendingJobCount& lhs, const TCompositePendingJobCount& rhs)
{
    return lhs + (-rhs);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent

size_t THash<NYT::NControllerAgent::TJobMonitoringDescriptor>::operator()(const NYT::NControllerAgent::TJobMonitoringDescriptor& descriptor) const
{
    return MultiHash(descriptor.Guid, descriptor.Index);
}
