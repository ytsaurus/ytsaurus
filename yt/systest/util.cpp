
#include <yt/systest/util.h>

namespace NYT::NTest {

using namespace NNodeCmp;

namespace {

class TByteCountOutputStream : public IOutputStream
{
public:
    size_t GetCount() const
    {
        return Count_;
    }

protected:
    virtual void DoWrite(const void* /*buf*/, size_t len) override
    {
        Count_ += len;
    }

private:
    size_t Count_ = 0;
};

}  // namespace

////////////////////////////////////////////////////////////////////////////////

TColumnIndex BuildColumnIndex(const std::vector<TDataColumn>& dataColumns)
{
    std::unordered_map<TString, int> columnIndex;
    for (int i = 0; i < std::ssize(dataColumns); i++) {
        const auto& columnName = dataColumns[i].Name;
        columnIndex[columnName] = i;
    }
    return columnIndex;
}

std::vector<TNode> ArrangeValuesToIndex(
    const std::unordered_map<TString, int>& index,
    const TNode::TMapType& mapRow)
{
    std::vector<TNode> result(index.size());
    std::vector<bool> resultPresent(index.size(), false);
    for (const auto& entry : mapRow) {
        auto iteratorColumnPos = index.find(entry.first);
        if (iteratorColumnPos == index.end()) {
            THROW_ERROR_EXCEPTION("Validation failed, unexpected column (ColumnName: %v)", entry.first);
        }
        if (resultPresent[iteratorColumnPos->second]) {
            THROW_ERROR_EXCEPTION("Validation failed, duplicate column (ColumnName: %v)", entry.first);
        }
        resultPresent[iteratorColumnPos->second] = true;
        result[iteratorColumnPos->second] = entry.second;
    }
    return result;
}

ssize_t ComputeNodeByteSize(const TNode& node)
{
    TByteCountOutputStream byteCountStream;
    node.Save(&byteCountStream);
    return byteCountStream.GetCount();
}

std::vector<TNode> ExtractInputValues(TRange<TNode> values, TRange<int> input)
{
    std::vector<TNode> result;
    for (int position : input) {
        result.push_back(values[position]);
    }
    return result;
}

TString DebugString(const TNode& node)
{
    TTempBufOutput outputStream;
    node.Save(&outputStream);
    return TString(outputStream.Data(), outputStream.Filled());
}

int CompareRowPrefix(int prefixLength, TRange<TNode> lhs, TRange<TNode> rhs)
{
    for (int i = 0; i < prefixLength; ++i) {
        if (lhs[i] != rhs[i]) {
            if (lhs[i] < rhs[i]) {
                return -1;
            } else {
                return 1;
            }
        }
    }
    return 0;
}

}  // namespace NYT::NTest
