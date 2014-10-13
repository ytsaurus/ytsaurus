#pragma once

#include "public.h"
#include "callbacks.h"
#include "plan_fragment.h"
#include "query_statistics.h"
#include "key_trie.h"

#include <core/logging/log.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TCoordinator
    : public IEvaluateCallbacks
{
public:
    TCoordinator(
        ICoordinateCallbacks* callbacks,
        const TPlanFragmentPtr& fragment);

    ~TCoordinator();

    int GetPeerIndex(const TDataSplit& dataSplit);

    virtual ISchemafulReaderPtr GetReader(
        const TDataSplit& split,
        TNodeDirectoryPtr nodeDirectory) override;

    //! Actually evaluates query.
    //! NB: Does not throw.
    void Run();

    //! Returns a plan fragment to be evaluated by the coordinator.
    TPlanFragmentPtr GetCoordinatorFragment() const;

    //! Returns plan fragments to be evaluated by peers.
    std::vector<TPlanFragmentPtr> GetPeerFragments() const;

    //! Returns the totals by aggregating statistics for all involved peers.
    TQueryStatistics GetStatistics() const;

private:
    struct TPeer
    {
        TPeer(
            const TPlanFragmentPtr& fragment,
            const TDataSplit& collocatedSplit,
            ISchemafulReaderPtr reader,
            const TFuture<TErrorOr<TQueryStatistics>>& QueryResult)
            : Fragment(fragment)
            , CollocatedSplit(collocatedSplit)
            , Reader(std::move(reader))
            , QueryResult(QueryResult)
        { }

        TPeer(const TPeer&) = delete;
        TPeer(TPeer&&) = default;

        TPlanFragmentPtr Fragment;
        const TDataSplit& CollocatedSplit;
        ISchemafulReaderPtr Reader;
        TFuture<TErrorOr<TQueryStatistics>> QueryResult;
    };

private:
    std::vector<TOperatorPtr> Scatter(const TConstOperatorPtr& op, const TKeyTrieNode& keyTrie = TKeyTrieNode::Universal());
    TOperatorPtr Gather(
        const std::vector<TOperatorPtr>& ops,
        const TTableSchema& tableSchema,
        const TKeyColumns& keyColumns);

    TGroupedDataSplits SplitAndRegroup(
        const TDataSplits& splits,
        const TKeyTrieNode& keyTrie);

    TNullable<int> GetPlanFragmentPeer(const TDataSplit& split);

    void DelegateToPeers();

private:
    ICoordinateCallbacks* Callbacks_;
    TPlanFragmentPtr Fragment_;

    std::vector<TPeer> Peers_;

    TQueryStatistics QueryStat;

    NLog::TLogger Logger;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

