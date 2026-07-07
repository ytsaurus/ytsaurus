#include "conductor.h"

#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/fetch/fetch.h>

#include <util/generic/vector.h>
#include <util/string/builder.h>


namespace NYql {

void ConductorGroupToHosts(TStringBuf group, TVector<TString>* hosts) {
    auto res = Fetch(ParseURL(TStringBuilder() << "https://c.yandex-team.ru/api/groups2hosts/" << group), {}, TDuration::Minutes(1), 10);
    if (res->GetRetCode() == 200) {
        TString host;
        while (res->GetStream().ReadLine(host)) {
            hosts->emplace_back(host);
        }
    } else {
        YQL_LOG(ERROR) << "Can't resolve conductor group " << group << ", status=" << res->GetRetCode() << ", body:" << res->GetStream().ReadAll();
        throw yexception() << "Can't resolve conductor group " << group;
    }
}

} // namespace NYql
