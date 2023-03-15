#pragma once

///
/// @file roren.h
///
/// Main header file of the Roren library.

///
/// @mainpage C++ library for describing and running pipelines on YT / BigRT.
///
/// Roren is a library for writing computation pipelines heavily inspired by Apache Beam.
///
/// Roren pipelines can be run in several modes:
///   - local debug mode, that is used in unittesting
///   - YT mode, that works with YT tables and runs operations
///   - BigRT mode, that operates on realtime data using BigRT library
///
/// Entry points to this library:
///  - @ref yt/cpp/roren/interface/roren.h main header file of the library.
///  - @ref yt/cpp/roren/interface/transforms.h header file describing basic transforms.
///  - @ref NRoren::TPCollection parallel collection central concept of Roren library representing collection of rows.

#include "fwd.h"

#include "co_gbk_result.h"
#include "input.h"
#include "key_value.h"
#include "transforms.h"

#include "private/raw_pipeline.h"

#include <vector>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

///
/// @brief Immutable collection of values of given type.
///
/// Usually PCollection is related to YT table (in YT pipelines) or queue of events (in BigRT pipelines).
///
/// PCollections might only be created as results of transforms. And transforms can be applied to PCollection
/// to produce new PCollections.
///
/// PCollections might be copied but in this case copied PCollection object refers to the same entity (YT table / BigRT queue)
/// and the same node in pipeline graph.
template <typename T>
class TPCollection
{
    // NB. We want TPCollection to be forward declared as
    //   template <typename> class TPCollection;
    // So we don't move `CRow` into template parameter declaration
    static_assert(CRow<T>, "Cannot create TPCollection for this type");

public:
    using TRowType = T;

public:

    /// @cond Doxygen_Suppress
    TPCollection() = delete;

    TPCollection(const TPCollection<T>& ) = default;

    TPCollection<T>& operator=(const TPCollection<T>&) = default;
    /// @endcond

    ///
    /// @brief Apply PTransform to PCollection.
    ///
    /// Returned type depends on transform and might be one of:
    ///   - void
    ///   - TPCollection<?>
    ///   - TMultiPCollection
    ///
    /// @see transforms.h
    template <CApplicableTo<TPCollection<T>> TTransform>
    auto operator|(const TTransform& transform) const
    {
        return transform.ApplyTo(*this);
    }

private:
    TPCollection(NPrivate::TPCollectionNodePtr dataNode, NPrivate::TRawPipelinePtr pipeline)
        : RawDataNode_(std::move(dataNode))
        , RawPipeline_(std::move(pipeline))
    { }

private:
    NPrivate::TPCollectionNodePtr RawDataNode_;
    NPrivate::TRawPipelinePtr RawPipeline_;

private:
    /// @cond Doxygen_Suppress
    friend TPCollection<T> NPrivate::MakePCollection<T>(NPrivate::TPCollectionNodePtr dataNode, NPrivate::TRawPipelinePtr pipeline);
    friend const NPrivate::TRawPipelinePtr& NPrivate::GetRawPipeline<T>(const TPCollection<T>& pCollection);
    friend const NPrivate::TPCollectionNodePtr& NPrivate::GetRawDataNode<T>(const TPCollection<T>& pCollection);
    /// @endcond
};

////////////////////////////////////////////////////////////////////////////////

///
/// @brief Immutable pack of multiple PCollections.
///
/// TMultiPCollection is produced as output of some PTransforms and some PTransforms use it as input.
/// Each TPCollection of TMultiPCollection has related TPCollection
class TMultiPCollection
{
public:
    TMultiPCollection() = delete;
    explicit TMultiPCollection(const TPipeline& pipeline);

    TMultiPCollection(const TMultiPCollection& ) = default;

    template <typename T, typename... Args>
    TMultiPCollection(TTypeTag<T> tag, TPCollection<T> pCollection, Args... args);

    ///
    /// @brief Get PCollection by TypeTag
    ///
    /// Aborts program if tag is not found in TMultiPCollection.
    template <typename TRow>
    TPCollection<TRow> Get(const TTypeTag<TRow>& tag) const;

    ///
    /// @brief Unpack multiple PCollections by thier TypeTags.
    ///
    /// Aborts program if any tag is not found in TMultiPCollection.
    template <typename... TypeTags>
    std::tuple<TPCollection<typename TypeTags::TRow>...> Unpack(TypeTags... tags) const;

    /// @brief Apply transform.
    template <CApplicableTo<TMultiPCollection> TTransform>
    auto operator|(const TTransform& transform) const;

private:
    using TNodeList = std::vector<std::pair<TDynamicTypeTag, NPrivate::TPCollectionNodePtr>>;
    using TNodeMap = THashMap<TDynamicTypeTag::TKey, NPrivate::TPCollectionNodePtr>;

private:
    TMultiPCollection(const std::vector<std::pair<TDynamicTypeTag, NPrivate::TPCollectionNodePtr>>& nodes, NPrivate::TRawPipelinePtr pipeline);

    template <typename T, typename... Args>
    static void FillDataNodes(
        TNodeList* nodeList,
        TNodeMap* nodes,
        TTypeTag<T> tag,
        TPCollection<T> pCollection,
        Args... args);

private:
    TNodeList NodeList_;
    TNodeMap NodeMap_;
    NPrivate::TRawPipelinePtr RawPipeline_;

private:
    friend TMultiPCollection NPrivate::MakeMultiPCollection(
        const std::vector<std::pair<TDynamicTypeTag, NPrivate::TPCollectionNodePtr>>& nodes,
        NPrivate::TRawPipelinePtr pipeline);
    friend const NPrivate::TRawPipelinePtr& NPrivate::GetRawPipeline(const TMultiPCollection& multiPCollection);
    friend const std::vector<std::pair<TDynamicTypeTag, NPrivate::TPCollectionNodePtr>>&
    NPrivate::GetTaggedNodeList(const TMultiPCollection&);
};

////////////////////////////////////////////////////////////////////////////////

///
/// @brief Computation pipeline.
///
/// Stores computation graph and launches its execution.
///
/// Users create pipelines using one of factory functions provided by one of executor libraries.
class TPipeline
{
public:
    TPipeline(const TPipeline& pipeline);
    TPipeline(TPipeline&& pipeline);
    ~TPipeline();

    void Run();

    template <CApplicableTo<TPipeline> TTransform>
    auto operator|(const TTransform& transform) const
    {
        return transform.ApplyTo(*this);
    }

private:
    explicit TPipeline(IExecutorPtr executor);

    void Optimize();

private:
    NPrivate::TRawPipelinePtr RawPipeline_ = MakeIntrusive<NPrivate::TRawPipeline>();
    IExecutorPtr Executor_;

private:
    friend const NPrivate::TRawPipelinePtr& NPrivate::GetRawPipeline(const TPipeline&);
    friend TPipeline NPrivate::MakePipeline(IExecutorPtr executor);
};

////////////////////////////////////////////////////////////////////////////////

///
/// @brief Runner independent representation of state.
///
/// Some transforms e.g. ( @ref NRoren::IStatefulParDo ) allow to keep state and use/update it inside computation.
/// PState abstracts away how state is stored.
///
/// To create a PState look for functions in particular runner.
///
/// @ref NRoren::IStatefulDoFn
template <typename TKey_, typename TState_>
class TPState
{
public:
    TPState() = delete;
    TPState(const TPState&) = default;

private:
    explicit TPState(NPrivate::TRawPStateNodePtr rawPStateNode, NPrivate::TRawPipelinePtr pipeline);

private:
    NPrivate::TRawPipelinePtr RawPipeline_;
    NPrivate::TRawPStateNodePtr RawPStateNode_;

private:
    template <typename K, typename S>
    friend TPState<K, S> NPrivate::MakePState(NPrivate::TRawPipelinePtr rawPipeline);

    template <typename K, typename S>
    friend const NPrivate::TRawPStateNodePtr& NPrivate::GetRawPStateNode(const TPState<K, S>& pState);

    template <typename K, typename S>
    friend const NPrivate::TRawPipelinePtr& NPrivate::GetRawPipeline(const TPState<K, S>& pState);
};

////////////////////////////////////////////////////////////////////////////////

// || |\    /| |==\\ ||     |=== |\    /| ||=== |\ || ====   /\   ====  ||  //\\  |\ ||
// || ||\  /|| |==// ||    ||=== ||\  /|| ||=== ||\||  ||   /__\   ||   || ||  || ||\||
// || || \/ || ||    ||===  |=== || \/ || ||=== || \|  ||  //  \\  ||   ||  \\//  || \|

////////////////////////////////////////////////////////////////////////////////

template <typename T, typename... Args>
TMultiPCollection::TMultiPCollection(TTypeTag<T> tag, TPCollection<T> pCollection, Args... args)
    : RawPipeline_(NPrivate::GetRawPipeline(pCollection))
{
    FillDataNodes(&NodeList_, &NodeMap_, tag, pCollection, args...);
}

template <typename TRow>
TPCollection<TRow> TMultiPCollection::Get(const TTypeTag<TRow>& tag) const
{
    auto dynamicTag = TDynamicTypeTag{tag};
    auto it = NodeMap_.find(dynamicTag.GetKey());
    if (it == NodeMap_.end()) {
        ythrow yexception() << "Tag: " << ToString(dynamicTag) << " is unknown";
    }
    return MakePCollection<TRow>(it->second, RawPipeline_);
}

template <typename... TypeTags>
std::tuple<TPCollection<typename TypeTags::TRow>...> TMultiPCollection::Unpack(TypeTags... tags) const
{
    return std::tuple(Get(tags)...);
}

template <CApplicableTo<TMultiPCollection> TTransform>
auto TMultiPCollection::operator|(const TTransform& transform) const
{
    return transform.ApplyTo(*this);
}

template <typename T, typename... Args>
void TMultiPCollection::FillDataNodes(
    TNodeList* nodeList,
    TNodeMap* nodeMap,
    TTypeTag<T> tag, TPCollection<T> pCollection, Args... args)
{
    auto dynamicTag = TDynamicTypeTag{tag};
    auto rawNode = NPrivate::GetRawDataNode(pCollection);
    nodeList->emplace_back(dynamicTag, rawNode);
    nodeMap->emplace(dynamicTag.GetKey(), rawNode);
    if constexpr (sizeof...(args) > 0) {
        FillDataNodes(nodeList, nodeMap, args...);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <typename K, typename S>
TPState<K, S>::TPState(NPrivate::TRawPStateNodePtr rawPStateNode, NPrivate::TRawPipelinePtr rawPipeline)
    : RawPipeline_(std::move(rawPipeline))
    , RawPStateNode_(std::move(rawPStateNode))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren


namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
TPCollection<T> MakePCollection(NPrivate::TPCollectionNodePtr dataNode, NPrivate::TRawPipelinePtr pipeline)
{
    return {std::move(dataNode), std::move(pipeline)};
}

template <typename T>
const TRawPipelinePtr& GetRawPipeline(const TPCollection<T>& pCollection)
{
    return pCollection.RawPipeline_;
}

template <typename T>
const NPrivate::TPCollectionNodePtr& GetRawDataNode(const TPCollection<T>& pCollection)
{
    return pCollection.RawDataNode_;
}

template <typename K, typename S>
TPState<K, S> MakePState(TRawPipelinePtr rawPipeline)
{
    auto result = TPState<K, S>{
        MakeIntrusive<TRawPStateNode>(),
        std::move(rawPipeline)
    };
    return result;
}

template <typename K, typename S>
const TRawPStateNodePtr& GetRawPStateNode(const TPState<K, S>& pState)
{
    return pState.RawPStateNode_;
}

template <typename K, typename S>
const TRawPipelinePtr& GetRawPipeline(const TPState<K, S>& pState)
{
    return pState.RawPipeline_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
