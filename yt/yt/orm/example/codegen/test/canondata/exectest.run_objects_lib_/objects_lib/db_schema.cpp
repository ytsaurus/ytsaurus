// AUTOMATICALLY GENERATED. DO NOT EDIT!

#include "db_schema.h"

namespace NYT::NOrm::NExample::NServer::NLibrary {

////////////////////////////////////////////////////////////////////////////////

NYT::NOrm::NServer::NObjects::TDBConfig CreateDBConfig()
{
    return NYT::NOrm::NServer::NObjects::TDBConfig{
        .UpdateObjectMode = NYT::NOrm::NServer::NObjects::ESetUpdateObjectMode::Overwrite,
        .VersionCompatibility = NYT::NOrm::NServer::NObjects::EDBVersionCompatibility::DoNotValidate,
        .EnableTombstones = false,
        .EnableAnnotations = true,
        .EnableHistorySnapshotColumn = true,
        .EnableFinalizers = true,
        .EnableAsynchronousRemovals = true,
    };
}

////////////////////////////////////////////////////////////////////////////////

const NYT::NOrm::NServer::NObjects::THistoryEventsTable HistoryEventsTable(
    /*addHashKeyField*/ true,
    /*useUuidInKey*/ false,
    /*usePositiveEventTypes*/ true,
    /*optimizeForAscendingTime*/ true,
    /*commitTime*/ NYT::NOrm::NServer::NObjects::EHistoryCommitTime::TransactionStart,
    /*timeMode*/ NYT::NOrm::NServer::NObjects::EHistoryTimeMode::Logical,
    /*tableName*/ "history_events");

const NYT::NOrm::NServer::NObjects::THistoryIndexTable HistoryIndexTable(
    /*addHashKeyField*/ true,
    /*primary_table*/ HistoryEventsTable,
    /*tableName*/ "history_index");

const NYT::NOrm::NServer::NObjects::THistoryEventsTable HistoryEventsV2Table(
    /*addHashKeyField*/ true,
    /*useUuidInKey*/ false,
    /*usePositiveEventTypes*/ true,
    /*optimizeForAscendingTime*/ false,
    /*commitTime*/ NYT::NOrm::NServer::NObjects::EHistoryCommitTime::TransactionStart,
    /*timeMode*/ NYT::NOrm::NServer::NObjects::EHistoryTimeMode::Logical,
    /*tableName*/ "history_events_v2");

const NYT::NOrm::NServer::NObjects::THistoryIndexTable HistoryIndexV2Table(
    /*addHashKeyField*/ true,
    /*primary_table*/ HistoryEventsV2Table,
    /*tableName*/ "history_index_v2");

////////////////////////////////////////////////////////////////////////////////
const TAuthorsTable AuthorsTable;
const TBooksTable BooksTable;
const TBooksToPublishersTable BooksToPublishersTable;
const TBufferedTimestampIdsTable BufferedTimestampIdsTable;
const TCatsTable CatsTable;
const TEditorsTable EditorsTable;
const TEmployersTable EmployersTable;
const TExecutorsTable ExecutorsTable;
const TGenresTable GenresTable;
const TGroupsTable GroupsTable;
const THitchhikersTable HitchhikersTable;
const TIllustratorsTable IllustratorsTable;
const TIllustratorsToPublishersTable IllustratorsToPublishersTable;
const TIndexedIncrementIdsTable IndexedIncrementIdsTable;
const TInterceptorsTable InterceptorsTable;
const TInterceptorsToMotherShipsTable InterceptorsToMotherShipsTable;
const TManualIdsTable ManualIdsTable;
const TMotherShipsTable MotherShipsTable;
const TMotherShipsToNexusTable MotherShipsToNexusTable;
const TMultipolicyIdsTable MultipolicyIdsTable;
const TNestedColumnsTable NestedColumnsTable;
const TNexusTable NexusTable;
const TNirvanaDMProcessInstancesTable NirvanaDMProcessInstancesTable;
const TPublishersTable PublishersTable;
const TRandomIdsTable RandomIdsTable;
const TSchemasTable SchemasTable;
const TSemaphoresTable SemaphoresTable;
const TSemaphoresToSemaphoreSetsTable SemaphoresToSemaphoreSetsTable;
const TSemaphoreSetsTable SemaphoreSetsTable;
const TTimestampIdsTable TimestampIdsTable;
const TTypographersTable TypographersTable;
const TTypographersToPublishersTable TypographersToPublishersTable;
const TUsersTable UsersTable;
const TWatchLogConsumersTable WatchLogConsumersTable;
const TEditorToBooksTable EditorToBooksTable;
const TIllustratorToBooksTable IllustratorToBooksTable;
const TPublishersToBooksTable PublishersToBooksTable;
const TIllustratorToBooksForCoverIllustratorTable IllustratorToBooksForCoverIllustratorTable;
const TAuthorsToBooksForPeerReviewersTable AuthorsToBooksForPeerReviewersTable;
const TPublisherToIllustratorsTable PublisherToIllustratorsTable;
const TExecutorToMotherShipsTable ExecutorToMotherShipsTable;
const TEditorToPublishersTable EditorToPublishersTable;
const TIllustratorToPublishersTable IllustratorToPublishersTable;
const TPublisherToPublishersTable PublisherToPublishersTable;
const TIllustratorsToPublishersForOldFeaturedIllustratorsTable IllustratorsToPublishersForOldFeaturedIllustratorsTable;
const TIllustratorsToPublishersForNewFeaturedIllustratorsTable IllustratorsToPublishersForNewFeaturedIllustratorsTable;
const TAuthorsToBooksTable AuthorsToBooksTable;
const TBooksByCoverDpiTable BooksByCoverDpiTable;
const TBooksByCoverImageTable BooksByCoverImageTable;
const TBooksByCoverSizeTable BooksByCoverSizeTable;
const TBooksByCreationTimeTable BooksByCreationTimeTable;
const TBooksByEditorAndYearWithPredicateTable BooksByEditorAndYearWithPredicateTable;
const TBooksByFontTable BooksByFontTable;
const TBooksByGenresTable BooksByGenresTable;
const TBooksByIllustrationsTable BooksByIllustrationsTable;
const TBooksByIllustrationsWithPredicateTable BooksByIllustrationsWithPredicateTable;
const TBooksByIsbnTable BooksByIsbnTable;
const TBooksByIsbnWithPredicateTable BooksByIsbnWithPredicateTable;
const TBooksByKeywordsTable BooksByKeywordsTable;
const TBooksByNameTable BooksByNameTable;
const TBooksByPageCountTable BooksByPageCountTable;
const TBooksByStoreRatingTable BooksByStoreRatingTable;
const TBooksByYearTable BooksByYearTable;
const TBooksByYearAndFontTable BooksByYearAndFontTable;
const TBooksByYearAndPageCountWithPredicateTable BooksByYearAndPageCountWithPredicateTable;
const TBooksByYearWithPredicateTable BooksByYearWithPredicateTable;
const TCatsByNamesWithPredicateTable CatsByNamesWithPredicateTable;
const TEditorsByAchievementsTable EditorsByAchievementsTable;
const TEditorsByNameTable EditorsByNameTable;
const TEditorsByPhoneNumberTable EditorsByPhoneNumberTable;
const TEditorsByPostTable EditorsByPostTable;
const TGenresByNameTable GenresByNameTable;
const TNirvanaDMProcessInstancesByCreatedTable NirvanaDMProcessInstancesByCreatedTable;
const TNirvanaDMProcessInstancesByDefinitionIdTable NirvanaDMProcessInstancesByDefinitionIdTable;
const TPublishersByColumnFieldTable PublishersByColumnFieldTable;
const TPublishersByNumberOfAwardsTable PublishersByNumberOfAwardsTable;
const TTypographersByLoginTable TypographersByLoginTable;

const std::vector<const NYT::NOrm::NServer::NObjects::TDBTable*> Tables = {
    // Object tables.
    &AuthorsTable,
    &BooksTable,
    &BufferedTimestampIdsTable,
    &CatsTable,
    &EditorsTable,
    &EmployersTable,
    &ExecutorsTable,
    &GenresTable,
    &GroupsTable,
    &HitchhikersTable,
    &IllustratorsTable,
    &IndexedIncrementIdsTable,
    &InterceptorsTable,
    &ManualIdsTable,
    &MotherShipsTable,
    &MultipolicyIdsTable,
    &NestedColumnsTable,
    &NexusTable,
    &NirvanaDMProcessInstancesTable,
    &PublishersTable,
    &RandomIdsTable,
    &SchemasTable,
    &SemaphoresTable,
    &SemaphoreSetsTable,
    &TimestampIdsTable,
    &TypographersTable,
    &UsersTable,
    &WatchLogConsumersTable,

    // Index tables.
    &BooksByCoverDpiTable,
    &BooksByCoverImageTable,
    &BooksByCoverSizeTable,
    &BooksByCreationTimeTable,
    &BooksByEditorAndYearWithPredicateTable,
    &BooksByFontTable,
    &BooksByGenresTable,
    &BooksByIllustrationsTable,
    &BooksByIllustrationsWithPredicateTable,
    &BooksByIsbnTable,
    &BooksByIsbnWithPredicateTable,
    &BooksByKeywordsTable,
    &BooksByNameTable,
    &BooksByPageCountTable,
    &BooksByStoreRatingTable,
    &BooksByYearTable,
    &BooksByYearAndFontTable,
    &BooksByYearAndPageCountWithPredicateTable,
    &BooksByYearWithPredicateTable,
    &CatsByNamesWithPredicateTable,
    &EditorsByAchievementsTable,
    &EditorsByNameTable,
    &EditorsByPhoneNumberTable,
    &EditorsByPostTable,
    &GenresByNameTable,
    &NirvanaDMProcessInstancesByCreatedTable,
    &NirvanaDMProcessInstancesByDefinitionIdTable,
    &PublishersByColumnFieldTable,
    &PublishersByNumberOfAwardsTable,
    &TypographersByLoginTable,

    // References tables.
    &EditorToBooksTable,
    &IllustratorToBooksTable,
    &PublishersToBooksTable,
    &IllustratorToBooksForCoverIllustratorTable,
    &AuthorsToBooksForPeerReviewersTable,
    &PublisherToIllustratorsTable,
    &ExecutorToMotherShipsTable,
    &EditorToPublishersTable,
    &IllustratorToPublishersTable,
    &PublisherToPublishersTable,
    &IllustratorsToPublishersForOldFeaturedIllustratorsTable,
    &IllustratorsToPublishersForNewFeaturedIllustratorsTable,
    &AuthorsToBooksTable,

    // Parents tables.
    &BooksToPublishersTable,
    &IllustratorsToPublishersTable,
    &InterceptorsToMotherShipsTable,
    &MotherShipsToNexusTable,
    &SemaphoresToSemaphoreSetsTable,
    &TypographersToPublishersTable,

    // Tombstones table.

    // Pending removals table.
    &NYT::NOrm::NServer::NObjects::PendingRemovalsTable,

    // Annotations table.
    &NYT::NOrm::NServer::NObjects::AnnotationsTable,
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NExample::NServer::NLibrary
