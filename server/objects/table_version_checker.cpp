#include "table_version_checker.h"
#include "private.h"

#include <yt/client/api/transaction.h>

namespace NYP::NServer::NObjects {

using namespace NYT::NConcurrency;
using namespace NYT::NYTree;
using namespace NYT::NYson;

////////////////////////////////////////////////////////////////////////////////

void TTableVersionChecker::ScheduleCheck(const TDBTable* table)
{
    TablesToCheck_.push_back(table);
}

void TTableVersionChecker::Check()
{
    auto transaction = YTConnector_->GetInstanceLockTransaction();
    std::vector<TFuture<void>> asyncResults;

    for (const auto* table : TablesToCheck_) {
        const auto path = YTConnector_->GetTablePath(table);
        asyncResults.push_back(transaction->GetNode(path + "/@version")
            .Apply(BIND([=] (const TErrorOr<TYsonString>& ysonVersionOrError) {
                if (ysonVersionOrError.IsOK()) {
                    auto version = ConvertTo<int>(ysonVersionOrError.Value());
                    if (version != DBVersion) {
                        THROW_ERROR_EXCEPTION("Table %v version mismatch: expected %v, found %v",
                            path,
                            DBVersion,
                            version);
                    }
                } else {
                    THROW_ERROR_EXCEPTION("Error getting version of table %v",
                        path)
                        << ysonVersionOrError;
                }
            })));
    }

    WaitFor(Combine(asyncResults))
        .ThrowOnError();
    TablesToCheck_.clear();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
