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

    void InitializeReaders();

    TDataSplits GetUnitedDataSplit(
        TPlanContext* context,
        std::map<Stroka, const TOperator*> operatorsByLocation);

    TDataSplits SplitFurther(
        TPlanContext* context,
        const TDataSplits& splits);

    std::map<Stroka, const TOperator*> SplitOperator(
        TPlanContext* context,
        const TOperator* op);

    TPlanFragment SplitPlanFragment(const TPlanFragment& planFragment);


private:
    ICoordinateCallbacks* Callbacks_;
    TPlanFragment Fragment_;

    std::vector<std::tuple<TPlanFragment, Stroka, ISchemedReaderPtr>> Peers_;

    NLog::TTaggedLogger Logger;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

