#include "transaction.h"

namespace NYdb::inline V3::NTopic {

TTransactionId MakeTransactionId(const TTransactionBase& tx)
{
    return {tx.GetSessionId(), tx.GetId()};
}

TStatus MakeStatus(EStatus code, NYdb::NIssue::TIssues&& issues)
{
    return {code, std::move(issues)};
}

TStatus MakeSessionExpiredError()
{
    return MakeStatus(EStatus::SESSION_EXPIRED, {});
}

TStatus MakeCommitTransactionSuccess()
{
    return MakeStatus(EStatus::SUCCESS, {});
}

}
