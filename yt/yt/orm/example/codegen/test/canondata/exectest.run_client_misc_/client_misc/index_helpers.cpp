// AUTOMATICALLY GENERATED. DO NOT EDIT!

#include "enums.h"
#include "index_helpers.h"

#include <util/generic/hash.h>

namespace NYT::NOrm::NExample::NClient::NApi {

////////////////////////////////////////////////////////////////////////////////

namespace {

const THashMap<TString, NYT::NOrm::NClient::NObjects::TObjectTypeValue> ObjectTypeValuePerIndexName{
    {"books_by_authors", static_cast<int>(EObjectType::Book)},
    {"books_by_cover_dpi", static_cast<int>(EObjectType::Book)},
    {"books_by_cover_image", static_cast<int>(EObjectType::Book)},
    {"books_by_cover_size", static_cast<int>(EObjectType::Book)},
    {"books_by_creation_time", static_cast<int>(EObjectType::Book)},
    {"books_by_editor", static_cast<int>(EObjectType::Book)},
    {"books_by_editor_and_year_with_predicate", static_cast<int>(EObjectType::Book)},
    {"books_by_font", static_cast<int>(EObjectType::Book)},
    {"books_by_genres", static_cast<int>(EObjectType::Book)},
    {"books_by_illustrations", static_cast<int>(EObjectType::Book)},
    {"books_by_illustrations_with_predicate", static_cast<int>(EObjectType::Book)},
    {"books_by_isbn", static_cast<int>(EObjectType::Book)},
    {"books_by_isbn_with_predicate", static_cast<int>(EObjectType::Book)},
    {"books_by_keywords", static_cast<int>(EObjectType::Book)},
    {"books_by_name", static_cast<int>(EObjectType::Book)},
    {"books_by_page_count", static_cast<int>(EObjectType::Book)},
    {"books_by_peer_reviewers", static_cast<int>(EObjectType::Book)},
    {"books_by_store_rating", static_cast<int>(EObjectType::Book)},
    {"books_by_year", static_cast<int>(EObjectType::Book)},
    {"books_by_year_and_font", static_cast<int>(EObjectType::Book)},
    {"books_by_year_and_page_count_with_predicate", static_cast<int>(EObjectType::Book)},
    {"books_by_cover_size_with_predicate", static_cast<int>(EObjectType::Book)},
    {"cats_by_names_with_predicate", static_cast<int>(EObjectType::Cat)},
    {"editors_by_achievements", static_cast<int>(EObjectType::Editor)},
    {"editors_by_name", static_cast<int>(EObjectType::Editor)},
    {"editors_by_phone_number", static_cast<int>(EObjectType::Editor)},
    {"editors_by_post", static_cast<int>(EObjectType::Editor)},
    {"genre_by_name", static_cast<int>(EObjectType::Genre)},
    {"illustrators_by_publisher", static_cast<int>(EObjectType::Illustrator)},
    {"nirvana_dm_process_instances_by_created", static_cast<int>(EObjectType::NirvanaDMProcessInstance)},
    {"nirvana_dm_process_instances_by_definition_id", static_cast<int>(EObjectType::NirvanaDMProcessInstance)},
    {"publishers_by_column_field", static_cast<int>(EObjectType::Publisher)},
    {"publishers_by_number_of_awards", static_cast<int>(EObjectType::Publisher)},
    {"typographers_by_login", static_cast<int>(EObjectType::Typographer)},
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

NYT::NOrm::NClient::NObjects::TObjectTypeValue GetObjectTypeValueByIndex(const TString& indexName)
{
    auto it = ObjectTypeValuePerIndexName.find(indexName);
    if (it == ObjectTypeValuePerIndexName.end()) {
        THROW_ERROR_EXCEPTION("Index %Qv not found",
            indexName);
    }

    return it->second;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NExample::NClient::NApi
