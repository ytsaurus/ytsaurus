#include "public.h"

#include <yt/yt/client/query_client/query_statistics.h>

#include "key_util.h"

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

const TObjectKey EveryoneSubjectKey({EveryoneSubjectId});

////////////////////////////////////////////////////////////////////////////////

TPerformanceStatistics& TPerformanceStatistics::operator+=(const TPerformanceStatistics& rhs)
{
    ReadPhaseCount += rhs.ReadPhaseCount;
    for (const auto& [table, rhsStatistics] : rhs.SelectQueryStatistics) {
        auto& statistics = SelectQueryStatistics[table];
        statistics.insert(statistics.end(), rhsStatistics.begin(), rhsStatistics.end());
    }
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TAttributeValueList& valueList, TStringBuf /*spec*/)
{
    TDelimitedStringBuilderWrapper wrapper(builder);

    builder->AppendChar('{');
    wrapper->AppendFormat("Values: %v", valueList.Values);
    wrapper->AppendFormat("Timestamps: %v", valueList.Timestamps);
    builder->AppendChar('}');
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TAttributeSelector& selector, TStringBuf spec)
{
    FormatValue(builder, Format("{Paths: %v}", selector.Paths), spec);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
