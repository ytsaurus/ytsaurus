// AUTOMATICALLY GENERATED. DO NOT EDIT!

#include "enums.h"

#include <utility>

namespace NYT::NOrm::NExample::NClient::NApi {

////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr std::pair<EObjectType, TStringBuf> ValueStrings[] = {
    {EObjectType::Null, ""},
    {EObjectType::Book, "book"},
    {EObjectType::Author, "author"},
    {EObjectType::Publisher, "publisher"},
    {EObjectType::Editor, "editor"},
    {EObjectType::Illustrator, "illustrator"},
    {EObjectType::Typographer, "typographer"},
    {EObjectType::Genre, "genre"},
    {EObjectType::User, "user"},
    {EObjectType::Group, "group"},
    {EObjectType::Hitchhiker, "hitchhiker"},
    {EObjectType::Nexus, "nexus"},
    {EObjectType::MotherShip, "mother_ship"},
    {EObjectType::Interceptor, "interceptor"},
    {EObjectType::Executor, "executor"},
    {EObjectType::NirvanaDMProcessInstance, "nirvana_dm_process_instance"},
    {EObjectType::ManualId, "manual_id"},
    {EObjectType::RandomId, "random_id"},
    {EObjectType::TimestampId, "timestamp_id"},
    {EObjectType::BufferedTimestampId, "buffered_timestamp_id"},
    {EObjectType::IndexedIncrementId, "indexed_increment_id"},
    {EObjectType::MultipolicyId, "multipolicy_id"},
    {EObjectType::Employer, "employer"},
    {EObjectType::NestedColumns, "nested_columns"},
    {EObjectType::Schema, "schema"},
    {EObjectType::WatchLogConsumer, "watch_log_consumer"},
    {EObjectType::Semaphore, "semaphore"},
    {EObjectType::SemaphoreSet, "semaphore_set"},
    {EObjectType::Cat, "cat"},
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

TStringBuf FormatType(EObjectType v)
{
    switch (v) {
        case EObjectType::Null:
            return ValueStrings[0].second;
        case EObjectType::Book:
            return ValueStrings[1].second;
        case EObjectType::Author:
            return ValueStrings[2].second;
        case EObjectType::Publisher:
            return ValueStrings[3].second;
        case EObjectType::Editor:
            return ValueStrings[4].second;
        case EObjectType::Illustrator:
            return ValueStrings[5].second;
        case EObjectType::Typographer:
            return ValueStrings[6].second;
        case EObjectType::Genre:
            return ValueStrings[7].second;
        case EObjectType::User:
            return ValueStrings[8].second;
        case EObjectType::Group:
            return ValueStrings[9].second;
        case EObjectType::Hitchhiker:
            return ValueStrings[10].second;
        case EObjectType::Nexus:
            return ValueStrings[11].second;
        case EObjectType::MotherShip:
            return ValueStrings[12].second;
        case EObjectType::Interceptor:
            return ValueStrings[13].second;
        case EObjectType::Executor:
            return ValueStrings[14].second;
        case EObjectType::NirvanaDMProcessInstance:
            return ValueStrings[15].second;
        case EObjectType::ManualId:
            return ValueStrings[16].second;
        case EObjectType::RandomId:
            return ValueStrings[17].second;
        case EObjectType::TimestampId:
            return ValueStrings[18].second;
        case EObjectType::BufferedTimestampId:
            return ValueStrings[19].second;
        case EObjectType::IndexedIncrementId:
            return ValueStrings[20].second;
        case EObjectType::MultipolicyId:
            return ValueStrings[21].second;
        case EObjectType::Employer:
            return ValueStrings[22].second;
        case EObjectType::NestedColumns:
            return ValueStrings[23].second;
        case EObjectType::Schema:
            return ValueStrings[24].second;
        case EObjectType::WatchLogConsumer:
            return ValueStrings[25].second;
        case EObjectType::Semaphore:
            return ValueStrings[26].second;
        case EObjectType::SemaphoreSet:
            return ValueStrings[27].second;
        case EObjectType::Cat:
            return ValueStrings[28].second;
    }
    return {"Unknown"};
}

bool TryParseType(TStringBuf s, EObjectType* v)
{
    for (size_t i = 0; i < std::size(ValueStrings); ++i) {
        if (ValueStrings[i].second == s) {
            *v = ValueStrings[i].first;
            return true;
        }
    }
    return false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NExample::NClient::NApi
