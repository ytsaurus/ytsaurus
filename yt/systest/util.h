#pragma once

#include <library/cpp/yson/node/node.h>
#include <yt/systest/table.h>

namespace NYT::NTest {

typedef std::unordered_map<TString, int> TColumnIndex;

TColumnIndex BuildColumnIndex(const std::vector<TDataColumn>& dataColumns);

std::vector<TNode> ArrangeValuesToIndex(
    const std::unordered_map<TString, int>& index,
    const TNode::TMapType& mapRow);

size_t ComputeNodeByteSize(const TNode& node);

std::vector<TNode> ExtractInputValues(TRange<TNode> values, TRange<int> input);

}  // namespace NYT::NTest
