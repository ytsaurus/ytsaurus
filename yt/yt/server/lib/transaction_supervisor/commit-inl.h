#ifndef COMMIT_INL_H_
#error "Direct inclusion of this file is not allowed, include commit.h"
// For the sake of sane code completion.
#include "commit.h"
#endif

namespace NYT::NTransactionSupervisor {

////////////////////////////////////////////////////////////////////////////////

template <class TRequest>
TExpectedTransactionSignatureInfo BuildExpectedPrepareSignaturesFromRequest(
    const TRequest& request,
    int participantCount)
{
    using NYT::FromProto;

    TExpectedTransactionSignatureInfo result;
    // COMPAT(atalmenev): old peers / changelog entries don't carry signatures.
    if (!request.has_coordinator_expected_prepare_signature()) {
        result.Participants.assign(
            participantCount,
            FinalTransactionSignature);
    } else {
        FromProto(&result.Participants, request.expected_prepare_signatures());
        FromProto(&result.Coordinator, request.coordinator_expected_prepare_signature());
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionSupervisor
