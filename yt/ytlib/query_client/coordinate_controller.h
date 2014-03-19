#pragma once

#include "public.h"
#include "callbacks.h"
#include "plan_fragment.h"

#include <core/logging/tagged_logger.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TCoordinateController
    : public NNonCopyable::TNonCopyable
    , public IEvaluateCallbacks
{
public:
    TCoordinateController(
        ICoordinateCallbacks* callbacks,
        const TPlanFragment& fragment);

    ~TCoordinateController();

    int GetPeerIndex(const TDataSplit& dataSplit);

    virtual ISchemafulReaderPtr GetReader(
        const TDataSplit& split,
        TPlanContextPtr context) override;

    //! Actually evaluates query.
    //! NB: Does not throw.
    TError Run();

    //! Returns a plan fragment to be evaluated by the coordinator.
    TPlanFragment GetCoordinatorFragment() const;

    //! Returns plan fragments to be evaluated by peers.
    std::vector<TPlanFragment> GetPeerFragments() const;

private:
    std::vector<const TOperator*> Scatter(const TOperator* op);
    const TOperator* Gather(const std::vector<const TOperator*>& ops);

    const TOperator* Simplify(const TOperator*);

    TDataSplits Split(const TDataSplits& splits);
    TGroupedDataSplits Regroup(const TDataSplits& splits);

    std::pair<bool, int> IsInternal(const TDataSplit& split);

    void DelegateToPeers();

private:
    ICoordinateCallbacks* Callbacks_;
    TPlanFragment Fragment_;

    std::vector<std::tuple<TPlanFragment, const TDataSplit&, ISchemafulReaderPtr>> Peers_;

    NLog::TTaggedLogger Logger;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

