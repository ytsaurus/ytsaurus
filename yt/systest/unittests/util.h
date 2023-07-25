#pragma once

#include <library/cpp/yson/node/node.h>
#include <yt/systest/dataset.h>

namespace NYT::NTest {

bool equals(TRange<TNode> lhs, TRange<TNode> rhs);

TString ToString(const TNode& node);
TString ToString(TRange<TNode> nodes);

void ExpectEqual(const std::vector<std::vector<TNode>>& expected,
                 IDatasetIterator* iterator);

}  // namespace NYT::NTest
