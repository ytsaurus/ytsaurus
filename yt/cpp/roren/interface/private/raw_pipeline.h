#pragma once

#include "fwd.h"

#include "attributes.h"
#include "../type_tag.h"

#include <util/generic/hash_set.h>


namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
TPCollection<T> MakePCollection(TPCollectionNodePtr dataNode, TRawPipelinePtr pipeline);

template <typename T>
const TPCollectionNodePtr& GetRawDataNode(const TPCollection<T>& pCollection);

template <typename T>
const TRawPipelinePtr& GetRawPipeline(const TPCollection<T>& pCollection);

////////////////////////////////////////////////////////////////////////////////

TMultiPCollection MakeMultiPCollection(const std::vector<std::pair<TDynamicTypeTag, TPCollectionNodePtr>>& nodes, TRawPipelinePtr pipeline);

const TRawPipelinePtr& GetRawPipeline(const TMultiPCollection& multiPCollection);

const std::vector<std::pair<TDynamicTypeTag, TPCollectionNodePtr>>& GetTaggedNodeList(const TMultiPCollection&);

////////////////////////////////////////////////////////////////////////////////

const TRawPipelinePtr& GetRawPipeline(const TPipeline& pipeline);

////////////////////////////////////////////////////////////////////////////////

template <typename K, typename S>
TPState<K, S> MakePState(TRawPipelinePtr rawPipeline);

template <typename K, typename S>
const TRawPStateNodePtr& GetRawPStateNode(const TPState<K, S>& pState);

template <typename K, typename S>
const TRawPipelinePtr& GetRawPipeline(const TPState<K, S>& pState);

////////////////////////////////////////////////////////////////////////////////

class TPCollectionNode
    : public virtual NYT::TRefCounted
    , public IWithAttributes
{
public:
    TPCollectionNode(TRowVtable rowVtable, int id, size_t index, TTransformNode* outputOf)
        : RowVtable_(std::move(rowVtable))
        , Id_(id)
        , Index_(index)
        , SinkOf_(outputOf)
    {
        Y_ABORT_UNLESS(IsDefined(RowVtable_));
    }

    int GetId() const
    {
        return Id_;
    }

    size_t GetIndex() const
    {
        return Index_;
    }

    const TRowVtable& GetRowVtable() const
    {
        return RowVtable_;
    }

    TTransformNode* GetSinkOf()
    {
        return SinkOf_;
    }

    const TVector<TTransformNode*>& GetSourceFor() const
    {
        return SourceFor_;
    }

private:
    void SetAttribute(const TString& key, const std::any& value) override
    {
        Attributes_.SetAttribute(key, value);
    }

    const std::any* GetAttribute(const TString& key) const override
    {
        return Attributes_.GetAttribute(key);
    }

private:
    const TRowVtable RowVtable_;
    const int Id_;
    const size_t Index_;
    TTransformNode* const SinkOf_;
    TVector<TTransformNode*> SourceFor_;
    TAttributes Attributes_;

private:
    friend class TTransformNode;
};

DEFINE_REFCOUNTED_TYPE(TPCollectionNode);

////////////////////////////////////////////////////////////////////////////////

class TRawPStateNode
    : public NYT::TRefCounted
    , public TAttributes
{ };

DEFINE_REFCOUNTED_TYPE(TRawPStateNode);

////////////////////////////////////////////////////////////////////////////////

class TTransformNode
    : public NYT::TRefCounted
    , public TAttributes
{
public:
    const TString GetName() const {
        return Name_;
    }


    const IRawTransformPtr& GetRawTransform() const {
        return Transform_;
    }

    ssize_t GetSinkCount() const {
        return std::ssize(SinkList_);
    }

    const TPCollectionNodePtr& GetSink(ssize_t idx) const {
        Y_ABORT_UNLESS(idx >= 0 && idx < static_cast<ssize_t>(SinkList_.size()));
        return SinkList_[idx];
    }

    const std::vector<TPCollectionNodePtr>& GetSinkList() const {
        return SinkList_;
    }

    std::vector<std::pair<TDynamicTypeTag, TPCollectionNodePtr>> GetTaggedSinkNodeList() const;

    ssize_t GetSourceCount() const {
        return std::ssize(SourceList_);
    }

    const TPCollectionNodePtr& GetSource(ssize_t idx) const {
        Y_ABORT_UNLESS(idx >= 0 && idx < static_cast<ssize_t>(SourceList_.size()));
        return SourceList_[idx];
    }

    const std::vector<TPCollectionNodePtr>& GetSourceList() const {
        return SourceList_;
    }

    const TRawPStateNodePtr& GetPStateNode() const {
        return PState_;
    }

    /// Number of function creating debug descriptions of this transform node.
    /**
     * Debug description includes backtrace of the place where this transform was created.
     * Resolving this backtrace can take a while (few seconds) for large binaries, so this method shouln't be used too often.
     */
    TString SlowlyGetDebugDescription() const;
    void SlowlyPrintDebugDescription() const;
    void SlowlyPrintDebugDescription(IOutputStream* out) const;

private:
    DECLARE_NEW_FRIEND();
    TTransformNode(TString name, IRawTransformPtr transform);

    static TTransformNodePtr Allocate(
        TString name,
        const TRawPipelinePtr& rawPipeline,
        IRawTransformPtr transform,
        const std::vector<TPCollectionNode*>& inputs,
        const TRawPStateNodePtr& pState = nullptr);

private:
    // Name is unique throughout pipeline.
    const TString Name_;

    IRawTransformPtr Transform_;
    std::vector<TPCollectionNodePtr> SourceList_;
    std::vector<TPCollectionNodePtr> SinkList_;

    // PState_ is null for stateless transforms.
    TRawPStateNodePtr PState_;

    friend class TRawPipeline;
};

DEFINE_REFCOUNTED_TYPE(TTransformNode);

////////////////////////////////////////////////////////////////////////////////

class TRawPipeline
    : public NYT::TRefCounted
{
public:
    class TStartTransformGuard;

public:
    TRawPipeline();
    TRawPipeline(const TRawPipeline&) = delete;
    ~TRawPipeline();

    TTransformNodePtr AddTransform(
        IRawTransformPtr transform,
        const std::vector<TPCollectionNode*>& inputs,
        const TRawPStateNodePtr& pState = nullptr);

    [[nodiscard]] const std::vector<TTransformNodePtr>& GetTransformList() const
    {
        return TransformList_;
    }

    std::shared_ptr<TStartTransformGuard> StartTransformGuard(TString name);

    TString DumpDot() const;
    void Dump(NYT::NYson::IYsonConsumer* consumer) const;
private:
    TPCollectionNodePtr AllocatePCollectionNode(TRowVtable rowVtable, TTransformNode* outputOf, size_t index)
    {
        return NYT::New<TPCollectionNode>(std::move(rowVtable), GenerateId(), index, outputOf);
    }

    int GenerateId()
    {
        return NextNodeId_++;
    }

private:
    class TNameRegistry;

private:
    THashMap<TString, TTransformNodePtr> TransformByName_;
    std::vector<TTransformNodePtr> TransformList_;
    int NextNodeId_ = 0;
    std::unique_ptr<TNameRegistry> NameRegitstry_;

    friend class TTransformNode;
};

DEFINE_REFCOUNTED_TYPE(TRawPipeline);

////////////////////////////////////////////////////////////////////////////////

class IRawPipelineVisitor
{
public:
    virtual void OnPCollection(TPCollectionNode* /*pCollection*/)
    { }

    virtual void OnTransform(TTransformNode* /*transform*/)
    { }
};

void TraverseInTopologicalOrder(const TRawPipelinePtr& rawPipeline, IRawPipelineVisitor* visitor);

////////////////////////////////////////////////////////////////////////////////

TPipeline MakePipeline(IExecutorPtr executor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
