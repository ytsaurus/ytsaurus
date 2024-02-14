#pragma once

#include <library/cpp/yson/node/node.h>

#include <yt/cpp/mapreduce/interface/errors.h>

#include <yt/systest/table.h>

namespace NYT::NTest {

using TColumnIndex = std::unordered_map<TString, int>;

TColumnIndex BuildColumnIndex(const std::vector<TDataColumn>& dataColumns);

std::vector<TNode> ArrangeValuesToIndex(
    const std::unordered_map<TString, int>& index,
    const TNode::TMapType& mapRow);

ssize_t ComputeNodeByteSize(const TNode& node);

std::vector<TNode> ExtractInputValues(TRange<TNode> values, TRange<int> input);

int CompareRowPrefix(int prefixLength, TRange<TNode> lhs, TRange<TNode> rhs);

TString DebugString(const TNode& node);
TString DebugString(TRange<TNode> row);

bool IsRetriableError(const TErrorResponse& ex);

size_t RowHash(TRange<TNode> row);

}  // namespace NYT::NTest
