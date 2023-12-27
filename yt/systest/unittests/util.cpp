
#include <yt/systest/util.h>
#include <yt/systest/unittests/util.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NTest {

bool equals(TRange<TNode> lhs, TRange<TNode> rhs)
{
    if (lhs.size() != rhs.size()) {
        return false;
    }
    for (int i = 0; i < std::ssize(lhs); i++) {
        if (lhs[i] != rhs[i]) {
            return false;
        }
    }
    return true;
}

TString DebugString(const TNode& node)
{
    TTempBufOutput outputStream;
    node.Save(&outputStream);
    return TString(outputStream.Data(), outputStream.Filled());
}

TString ToString(TRange<TNode> nodes)
{
    TString ret;
    for (int i = 0; i < std::ssize(nodes); i++) {
        if (i > 0) {
            ret += ", ";
        }
        ret += DebugString(nodes[i]);
    }
    return ret;
}

void ExpectEqual(const std::vector<std::vector<TNode>>& expected,
                 IDatasetIterator* iterator)
{
    int pos = 0;
    for (; !iterator->Done() && pos < std::ssize(expected); ++pos, iterator->Next()) {
        if (!equals(TRange<TNode>(expected[pos]), iterator->Values())) {
            Cerr << "Difference in position " << pos << ": "
                 << "expected " << ToString(TRange<TNode>(expected[pos])) << "\n"
                 << "actual " << ToString(iterator->Values()) << "\n";
            GTEST_FAIL();
        }
    }
    if (pos < std::ssize(expected)) {
        Cerr << "Expected more rows:\n";
        for (int i = pos; i < std::ssize(expected); i++) {
            Cerr << ToString(TRange<TNode>(expected[i])) << "\n";
        }
        GTEST_FAIL();
    }
}

}  // namespace NYT::NTest
