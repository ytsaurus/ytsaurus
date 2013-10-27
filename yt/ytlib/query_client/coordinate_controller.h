#pragma once

#include "public.h"

#include "query_fragment.h"

#include <core/logging/tagged_logger.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TCoordinateController
{
public:
    TCoordinateController(
        ICoordinateCallbacks* callbacks,
        const TQueryFragment& fragment);

    ~TCoordinateController();

    IReaderPtr GetReader();
    IReaderPtr GetPeerReader(int i);

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

