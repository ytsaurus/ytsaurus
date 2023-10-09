#include "transforms.h"

#include "roren.h"

#include "private/co_group_by_key.h"

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

TGroupByKeyTransform GroupByKey()
{
    return {};
}

////////////////////////////////////////////////////////////////////////////////

TString TCoGroupByKeyTransform::GetName() const
{
    return "CoGroupByKey";
}

TPCollection<TCoGbkResult> TCoGroupByKeyTransform::ApplyTo(const TMultiPCollection& multiPCollection) const
{
    // 1. Нужно убедиться, что все ключи имеют один тип.
    TString keyTypeName;
    std::vector<NPrivate::TPCollectionNode*> inputs;
    std::vector<TDynamicTypeTag> tags;
    for (const auto& [tag, node] : NPrivate::GetTaggedNodeList(multiPCollection)) {
        const auto& rowVtable = node->GetRowVtable();
        if (!IsKv(rowVtable)) {
            ythrow yexception() << "MultiPCollection contains non KV: " << rowVtable.TypeName;
        }
        auto keyVtable = rowVtable.KeyVtableFactory();
        Y_ABORT_UNLESS(!keyVtable.TypeName.empty());
        if (keyTypeName.empty()) {
            keyTypeName = keyVtable.TypeName;
        } else if (keyTypeName != keyVtable.TypeName) {
            ythrow yexception() << "MultiPCollection contains TKVs with different keys: " << keyTypeName << " vs " << keyVtable.TypeName;
        }
        tags.emplace_back(tag);
        inputs.emplace_back(node.Get());
    }

    // 2. Добавить в граф трансформ.
    const auto& rawPipeline = NPrivate::GetRawPipeline(multiPCollection);
    auto transformNode = rawPipeline->AddTransform(NPrivate::MakeRawCoGroupByKey(tags), inputs);

    auto sinkNodeList = transformNode->GetTaggedSinkNodeList();
    Y_ABORT_UNLESS(sinkNodeList.size() == 1);

    return NPrivate::MakePCollection<TCoGbkResult>(sinkNodeList[0].second, rawPipeline);
}

////////////////////////////////////////////////////////////////////////////////

TCoGroupByKeyTransform CoGroupByKey()
{
    return TCoGroupByKeyTransform{};
}

////////////////////////////////////////////////////////////////////////////////

TFlattenTransform Flatten()
{
    return {};
}

////////////////////////////////////////////////////////////////////////////////

TNullWriteTransform NullWrite()
{
    return {};
}

} // namespace NRoren
