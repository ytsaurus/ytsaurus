#pragma once

#include "operation_controller_detail.h"

namespace NYT::NControllerAgent::NControllers {

////////////////////////////////////////////////////////////////////////////////

//! Helper class that implements IPersistentChunkPoolInput interface for output tables.
class TSink
    : public NChunkPools::IPersistentChunkPoolInput
    , public NPhoenix::TFactoryTag<NPhoenix::TSimpleFactory>
{
public:
    //! Used only for persistence.
    TSink() = default;

    TSink(TOperationControllerBase* controller, int outputTableIndex);

    TCookie AddWithKey(NChunkPools::TChunkStripePtr stripe, NChunkPools::TChunkStripeKey key) override;

    TCookie Add(NChunkPools::TChunkStripePtr stripe) override;

    void Suspend(TCookie cookie) override;
    void Resume(TCookie cookie) override;
    void Reset(TCookie cookie, NChunkPools::TChunkStripePtr stripe, NChunkPools::TInputChunkMappingPtr chunkMapping) override;
    void Finish() override;
    bool IsFinished() const override;

    void Persist(const TPersistenceContext& context) override;

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TSink, 0x7fb74a90);

    TOperationControllerBase* Controller_;
    int OutputTableIndex_ = -1;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
