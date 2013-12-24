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
    virtual IReaderPtr GetReader(const TDataSplit& dataSplit) override;

    //! Actually evaluates query.
    //! NB: Does not throw.
    TError Run();

    //! Returns a plan fragment to be evaluated by the coordinator.
    TPlanFragment GetCoordinatorFragment() const;

    //! Returns plan fragments to be evaluated by peers.
    std::vector<TPlanFragment> GetPeerFragments() const;

    ICoordinateCallbacks* GetCallbacks()
    {
        return Callbacks_;
    }

    TPlanContext* GetContext()
    {
        return Fragment_.GetContext().Get();
    }

    const TOperator* GetHead()
    {
        return Fragment_.GetHead();
    }

private:
    void SplitFurther();
    void PushdownFilters();
    void PushdownGroups();
    void PushdownProjects();
    void DistributeToPeers();
    void InitializeReaders();

    void SetHead(const TOperator* head)
    {
        Fragment_.SetHead(head);
    }

    template <class TFunctor>
    void Rewrite(const TFunctor& functor)
    {
        Fragment_.Rewrite(functor);
    }

private:
    ICoordinateCallbacks* Callbacks_;
    TPlanFragment Fragment_;

    std::vector<std::tuple<TPlanFragment, IReaderPtr>> Peers_;

    NLog::TTaggedLogger Logger;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

