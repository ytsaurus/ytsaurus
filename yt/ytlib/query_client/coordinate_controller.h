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

    virtual IReaderPtr GetReader(const TDataSplit& dataSplit) override;

    TError Run();

    const TPlanFragment& GetCoordinatorSplit() const
    {
        return Fragment_;
    }

    std::vector<TPlanFragment> GetPeerSplits() const;

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
    void PushdownGroupBys();
    void PushdownProjects();
    void DistributeToPeers();

    void SetHead(const TOperator* head)
    {
        Fragment_.SetHead(head);
    }

    template <class TFunctor>
    void Rewrite(const TFunctor& functor)
    {
        SetHead(Apply(GetContext(), GetHead(), functor));
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

