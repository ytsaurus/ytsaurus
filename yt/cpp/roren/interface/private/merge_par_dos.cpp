#include "merge_par_dos.h"

#include "par_do_tree.h"
#include "raw_pipeline.h"
#include "raw_transform.h"

#include <util/generic/hash_set.h>


namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

template <class TContainer, class... TArgs>
auto EmplaceOrCrash(TContainer& container, TArgs&&... args)
{
    auto [it, emplaced] = container.emplace(std::forward<TArgs>(args)...);
    Y_VERIFY(emplaced);
    return it;
}

template <class TMap, class TKey>
auto& GetOrCrash(TMap& map, const TKey& key)
{
    auto it = map.find(key);
    Y_VERIFY(it != map.end());
    return it->second;
}

////////////////////////////////////////////////////////////////////////////////

struct TUnifiedParDo
{
    IParDoTreePtr ParDoTree;
    std::vector<TPCollectionNodePtr> SinkList;
};

////////////////////////////////////////////////////////////////////////////////

class TUnifiedParDoBuilder
{
public:
    TUnifiedParDoBuilder(TPCollectionNodePtr root)
        : Root_(std::move(root))
    { }

    void Add(TTransformNode* transform)
    {
        Transforms_.push_back(transform);
    }

    TUnifiedParDo Build()
    {
        THashSet<TTransformNode*> transformSet(Transforms_.begin(), Transforms_.end());

        TParDoTreeBuilder builder;
        std::vector<TPCollectionNodePtr> sinkList;

        THashMap<TPCollectionNodePtr, TParDoTreeBuilder::TPCollectionNodeId> pCollectionNodeToId;
        EmplaceOrCrash(pCollectionNodeToId, Root_, builder.RootNodeId);

        for (const auto* transform : Transforms_) {
            auto rawParDo = transform->GetRawTransform()->AsRawParDo();

            auto sourceNode = transform->GetSource(0);
            auto sourceNodeIdIterator = pCollectionNodeToId.find(sourceNode);
            Y_VERIFY(sourceNodeIdIterator != pCollectionNodeToId.end());
            auto sourceNodeId = sourceNodeIdIterator->second;

            auto sinkNodeIds = builder.AddParDo(rawParDo, sourceNodeId);
            Y_VERIFY(std::ssize(sinkNodeIds) == transform->GetSinkCount());
            for (int i = 0; i < transform->GetSinkCount(); ++i) {
                const auto& sink = transform->GetSink(i);
                auto sinkNodeId = sinkNodeIds[i];
                EmplaceOrCrash(pCollectionNodeToId, sink, sinkNodeId);
                if (IsOutputPCollection(sink, transformSet)) {
                    builder.MarkAsOutput(sinkNodeId);
                    sinkList.push_back(sink);
                }
            }
        }

        return TUnifiedParDo{
            .ParDoTree =  builder.Build(),
            .SinkList = std::move(sinkList)
        };
    }

private:
    static bool IsOutputPCollection(
        const TPCollectionNodePtr& node,
        const THashSet<TTransformNode*>& transforms)
    {
        const auto& sources = node->GetSourceFor();
        return std::any_of(sources.begin(), sources.end(), [&] (TTransformNode* transform) {
            return !transforms.contains(transform);
        });
    }

private:
    TPCollectionNodePtr Root_;
    std::vector<TTransformNode*> Transforms_;
};

////////////////////////////////////////////////////////////////////////////////

class TCollectingVisitor
    : public IRawPipelineVisitor
{
public:
    void OnTransform(TTransformNode* transform) override
    {
        if (transform->GetRawTransform()->GetType() != ERawTransformType::ParDo) {
            return;
        }
        auto rawParDo = transform->GetRawTransform()->AsRawParDo();

        Y_VERIFY(transform->GetSourceCount() == 1);
        auto* source = transform->GetSource(0).Get();

        if (auto it = RootToUnifiedParDoBuilders_.find(source);
            it != RootToUnifiedParDoBuilders_.end())
        {
            RegisterTransform(transform, source, it->second);
            return;
        }

        auto* previousTransform = source->GetSinkOf();
        if (auto it = TransformNodeToRoot_.find(previousTransform);
            it != TransformNodeToRoot_.end())
        {
            auto* root = it->second;
            auto builderIt = RootToUnifiedParDoBuilders_.find(root);
            Y_VERIFY(builderIt != RootToUnifiedParDoBuilders_.end());
            RegisterTransform(transform, root, builderIt->second);
            return;
        }

        auto it = EmplaceOrCrash(RootToUnifiedParDoBuilders_, source, TUnifiedParDoBuilder(source));
        RegisterTransform(transform, source, it->second);
    }

private:
    void RegisterTransform(TTransformNode* transform, TPCollectionNode* root, TUnifiedParDoBuilder& builder)
    {
        EmplaceOrCrash(TransformNodeToRoot_, transform, root);
        builder.Add(transform);
    }

private:
    THashMap<TPCollectionNode*, TUnifiedParDoBuilder> RootToUnifiedParDoBuilders_;
    THashMap<TTransformNode*, TPCollectionNode*> TransformNodeToRoot_;

    friend class TBuildingVisitor;
};

////////////////////////////////////////////////////////////////////////////////

class TBuildingVisitor
    : public IRawPipelineVisitor
{
public:
    explicit TBuildingVisitor(TCollectingVisitor&& collectingVisitor)
        : TransformNodeToRoot_(std::move(collectingVisitor.TransformNodeToRoot_))
    {
        for (auto& [root, builder] : collectingVisitor.RootToUnifiedParDoBuilders_) {
            EmplaceOrCrash(RootToUnifiedParDos_, root, builder.Build());
        }
    }

    void OnPCollection(TPCollectionNode* pCollection) override
    {
        auto it = RootToUnifiedParDos_.find(pCollection);
        if (it == RootToUnifiedParDos_.end()) {
            return;
        }
        const auto& unifiedParDo = it->second;
        auto newTransform = RawPipeline_->AddTransform(
            unifiedParDo.ParDoTree,
            MapPCollectionNodes({pCollection})
        );
        AddPCollectionNodeMapping(unifiedParDo.SinkList, newTransform->GetSinkList());
    }

    void OnTransform(TTransformNode* transform) override
    {
        auto it = TransformNodeToRoot_.find(transform);
        if (it != TransformNodeToRoot_.end()) {
            return;
        }
        auto newTransform = RawPipeline_->AddTransform(
            transform->GetRawTransform(),
            MapPCollectionNodes(transform->GetSourceList())
        );
        AddPCollectionNodeMapping(transform->GetSinkList(), newTransform->GetSinkList());
    }

    TRawPipelinePtr Finish()
    {
        return std::move(RawPipeline_);
    }

private:
    std::vector<TPCollectionNode*> MapPCollectionNodes(const std::vector<TPCollectionNodePtr>& nodes)
    {
        std::vector<TPCollectionNode*> result;
        result.reserve(nodes.size());
        for (const auto& node : nodes) {
            result.push_back(GetOrCrash(PCollectionNodeMapping_, node.Get()));
        }
        return result;
    }

    void AddPCollectionNodeMapping(
        const std::vector<TPCollectionNodePtr>& src,
        const std::vector<TPCollectionNodePtr>& dst)
    {
        Y_VERIFY(src.size() == dst.size());
        for (int i = 0; i < std::ssize(src); ++i) {
            Y_VERIFY(!PCollectionNodeMapping_.contains(src[i].Get()));
            PCollectionNodeMapping_.emplace(src[i].Get(), dst[i].Get());
        }
    }

private:
    const THashMap<TTransformNode*, TPCollectionNode*> TransformNodeToRoot_;
    THashMap<TPCollectionNode*, TUnifiedParDo> RootToUnifiedParDos_;
    TRawPipelinePtr RawPipeline_ = ::MakeIntrusive<TRawPipeline>();
    THashMap<TPCollectionNode*, TPCollectionNode*> PCollectionNodeMapping_;
};

////////////////////////////////////////////////////////////////////////////////

TRawPipelinePtr MergeParDos(const TRawPipelinePtr& rawPipeline)
{
    TCollectingVisitor collectingVisitor;
    TraverseInTopologicalOrder(rawPipeline, &collectingVisitor);

    TBuildingVisitor buildingVisitor(std::move(collectingVisitor));
    TraverseInTopologicalOrder(rawPipeline, &buildingVisitor);

    return buildingVisitor.Finish();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
