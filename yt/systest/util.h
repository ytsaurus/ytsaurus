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

}  // namespace NYT::NTest
