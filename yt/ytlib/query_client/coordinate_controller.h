#pragma once

#include "public.h"
#include "callbacks.h"
#include "plan_fragment.h"

#include <core/logging/tagged_logger.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TCoordinateController
    : public TRefCounted
    , public IEvaluateCallbacks
{
public:
    TCoordinateController(
        ICoordinateCallbacks* callbacks,
        const TPlanFragment& fragment,
        TWriterPtr writer);

    ~TCoordinateController();

    virtual IReaderPtr GetReader(const TDataSplit& dataSplit) override;

    void Prepare(); // Throws.
    TError Run(); // Does not throw.

    TPlanFragment GetCoordinatorSplit() const;
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
    TWriterPtr Writer_;

    bool Prepared_;
    std::vector<std::tuple<TPlanFragment, IReaderPtr>> Peers_;

    NLog::TTaggedLogger Logger;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

