#ifndef QUERY_EXECUTOR_HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include query_executor_helpers.h"
// For the sake of sane code completion.
#include "query_executor_helpers.h"
#endif

#include <yt/yt/orm/server/master/bootstrap.h>
#include <yt/yt/orm/server/master/yt_connector.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

constexpr inline int SelectContinuationTokenMajorVersion = 1;

template <class TSelectContinuationToken>
void InitializeSelectContinuationTokenVersion(
    TSelectContinuationToken& token,
    NMaster::IBootstrap* bootstrap)
{
    token.MajorVersion = SelectContinuationTokenMajorVersion;
    token.MinorVersion = bootstrap->GetYTConnector()->GetExpectedDBVersion();
}

template <class TSelectContinuationToken>
void ValidateSelectContinuationTokenVersion(
    const TSelectContinuationToken& token,
    NMaster::IBootstrap* bootstrap)
{
    auto minorVersion = bootstrap->GetYTConnector()->GetExpectedDBVersion();
    if (token.MajorVersion != SelectContinuationTokenMajorVersion || token.MinorVersion != minorVersion) {
        THROW_ERROR_EXCEPTION(NClient::EErrorCode::ContinuationTokenVersionMismatch,
            "Incorrect select continuation token version: expected major %v and minor %v, but got major %v and minor %v",
            SelectContinuationTokenMajorVersion,
            minorVersion,
            token.MajorVersion,
            token.MinorVersion);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
