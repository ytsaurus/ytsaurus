#pragma once

#include "companion_client.h"
#include "companion_manager.h"
#include "companion_model.h"
#include "public.h"
#include <yt/yt/flow/library/cpp/computation/computation_base.h>

namespace NYT::NFlow::NCompanion {

////////////////////////////////////////////////////////////////////////////////

//! A CRTP base class template that provides common companion computation functionality.
/*!
 *  This class template inherits from TBase (which should be a computation class
 *  derived from TUniversalComputationBase) and adds companion-specific functionality.
 */
template <class TBase>
class TCompanionComputationBaseAdapter
    : public TBase
{
    // Verify that TBase is derived from TUniversalComputationBase.
    static_assert(std::derived_from<TBase, TUniversalComputationBase>);

public:
    TCompanionComputationBaseAdapter(
        TComputationContextPtr context,
        TDynamicComputationContextPtr dynamicContext);

protected:
    //! Fetches and validates companion info.
    /*!
     *  This method should be called from DoInit() of the derived class.
     *  It fetches the companion info and validates that the computation exists.
     */
    void FetchAndValidateCompanionInfo();

    //! Put job info to companion context.
    /*!
     *  This method should be called from DoInit() of the derived class.
     *  It puts the computation static spec, dynamic spec and streams to the companion context.
     */
    void PutJobInfoToCompanion();

    //! Puts job info to the companion and re-sends it whenever the dynamic spec is reconfigured.
    void PutJobInfoToCompanionWithReconfigure();

    //! Processes a request with the companion.
    /*!
     *  Sends a process request to the companion and handles potential job not found errors.
     *  If the companion reports that the job is not found, the method automatically resends the request
     *  with job information included.
     *
     *  \param request The process request to send to the companion.
     *  \return The response from the companion.
     */
    TCompanionResponsePtr DoProcessWithCompanion(const TCompanionProcessRequestPtr& request);

    //! Creates a new companion request of the specified type.
    /*!
     *  Creates a new request of type TRequestType and initializes it with the current job's
     *  information including job ID, computation ID, computation static and dynamic specs, and stream specs.
     *  This template method is used to create TCompanionProcessRequest.
     *
     *  \tparam TRequestType The type of request to create (e.g., TCompanionProcessRequest).
     *  \return A pointer to the newly created request.
     */
    template <class TRequestType>
    TIntrusivePtr<TRequestType> CreateCompanionRequest();

    ICompanionClientPtr CompanionClient_;
    TCompanionInfoPtr CompanionInfo_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion

#define COMPANION_COMPUTATION_BASE_INL_H_
#include "companion_computation_base-inl.h"
#undef COMPANION_COMPUTATION_BASE_INL_H_
