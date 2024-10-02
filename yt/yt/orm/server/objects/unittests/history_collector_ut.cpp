#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/orm/server/objects/history_events.h>
#include <yt/yt/orm/server/objects/config.h>

using namespace NYT::NOrm::NClient::NObjects;

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

void PrintTo(const THistoryEvent &historyEvent, std::ostream *os)
{
    *os << ToString(historyEvent);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NObjects

namespace NYT::NOrm::NServer::NObjects::NTests {

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

std::vector<THistoryEvent> SqueezeThroughCollector(
    std::vector<THistoryEvent> events,
    bool distinct,
    std::optional<int> limit = std::nullopt)
{
    std::vector<THistoryEvent> result;
    auto collector = New<THistoryEventCollector>(
        distinct,
        [&] (THistoryEvent event) {
            result.push_back(event);
        },
        limit);

    for (auto& event : events) {
        collector->OnEvent(std::move(event));
    }
    collector->Flush();

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

TEST(THistoryCollectorTest, SameResultDespiteOrder)
{
    std::vector<THistoryEvent> events = {
        THistoryEvent{
            .Time = NTableClient::TTimestamp{0},
            .EventTypeValue = ObjectCreatedEventTypeValue,
            .DistinctByAttributes = {
                .Values={NYson::ConvertToYsonString("value1")
            }}},
        THistoryEvent{
            .Time = NTableClient::TTimestamp{1},
            .EventTypeValue = ObjectUpdatedEventTypeValue,
            .DistinctByAttributes = {
                .Values={NYson::ConvertToYsonString("value1")
            }}},
        THistoryEvent{
            .Time = NTableClient::TTimestamp{2},
            .EventTypeValue = ObjectUpdatedEventTypeValue,
            .DistinctByAttributes = {
                .Values={NYson::ConvertToYsonString("value2")
            }}},
        THistoryEvent{
            .Time = NTableClient::TTimestamp{3},
            .EventTypeValue = ObjectUpdatedEventTypeValue,
            .DistinctByAttributes = {
                .Values={NYson::ConvertToYsonString("value2")
            }}},
    };

    auto resultsAscending = SqueezeThroughCollector(events, true);
    std::reverse(events.begin(), events.end());
    auto resultsDescending = SqueezeThroughCollector(events, true);
    std::reverse(resultsDescending.begin(), resultsDescending.end());

    ASSERT_EQ(resultsAscending, resultsDescending);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects::NTests
