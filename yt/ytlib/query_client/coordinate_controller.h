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

    virtual ISchemedReaderPtr GetReader(
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
    ICoordinateCallbacks* Callbacks_;
    TPlanFragment Fragment_;

    std::vector<std::tuple<TPlanFragment, ISchemedReaderPtr>> Peers_;

    NLog::TTaggedLogger Logger;


    void SplitFurther();
    void PushdownFilters();
    void PushdownGroups();
    void PushdownProjects();
    void DistributeToPeers();
    void InitializeReaders();

    template <class TFunctor>
    void Rewrite(const TFunctor& functor);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

