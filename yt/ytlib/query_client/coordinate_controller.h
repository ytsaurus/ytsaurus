#pragma once

#include "public.h"
#include "callbacks.h"
#include "query_fragment.h"

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
        const TQueryFragment& fragment,
        TWriterPtr writer);

    ~TCoordinateController();

    virtual IReaderPtr GetReader(const TDataSplit& dataSplit) override;

    TError Run();
    IReaderPtr GetPeer(int i);

    void SplitFurther();
    void PushdownFilters();
    void PushdownProjects();
    void DelegateToPeers();

    ICoordinateCallbacks* GetCallbacks()
    {
        return Callbacks_;
    }

    TQueryContext* GetContext()
    {
        return Fragment_.GetContext().Get();
    }

    const TOperator* GetHead()
    {
        return Fragment_.GetHead();
    }

private:
    ICoordinateCallbacks* Callbacks_;
    TQueryFragment Fragment_;
    TWriterPtr Writer_;

    std::vector<IReaderPtr> Peers_;

    void SetHead(const TOperator* head)
    {
        Fragment_.SetHead(head);
    }

    template <class TFunctor>
    void Rewrite(const TFunctor& functor)
    {
        SetHead(Apply(GetContext(), GetHead(), functor));
    }

    NLog::TTaggedLogger Logger;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

