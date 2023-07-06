
#include <yt/systest/util.h>

namespace NYT::NTest {

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

size_t ComputeNodeByteSize(const TNode& node)
{
    TByteCountOutputStream byteCountStream;
    node.Save(&byteCountStream);
    return byteCountStream.GetCount();
}

}  // namespace NYT::NTest
