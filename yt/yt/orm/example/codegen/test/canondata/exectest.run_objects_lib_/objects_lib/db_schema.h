// AUTOMATICALLY GENERATED. DO NOT EDIT!

#pragma once

#include "public.h"

#include <yt/yt/orm/server/objects/db_schema.h>
#include <yt/yt/orm/server/objects/db_config.h>

#include <vector>

namespace NYT::NOrm::NExample::NServer::NLibrary {

////////////////////////////////////////////////////////////////////////////////

static constexpr int DBVersion = 2;

////////////////////////////////////////////////////////////////////////////////

NYT::NOrm::NServer::NObjects::TDBConfig CreateDBConfig();

////////////////////////////////////////////////////////////////////////////////

inline const NYT::NOrm::NServer::NObjects::TParentsTable* DoGetParentsTable()
{
    return nullptr;
}
extern const NYT::NOrm::NServer::NObjects::THistoryEventsTable HistoryEventsTable;
extern const NYT::NOrm::NServer::NObjects::THistoryIndexTable HistoryIndexTable;
extern const NYT::NOrm::NServer::NObjects::THistoryEventsTable HistoryEventsV2Table;
extern const NYT::NOrm::NServer::NObjects::THistoryIndexTable HistoryIndexV2Table;
////////////////////////////////////////////////////////////////////////////////

extern const struct TAuthorsTable
    : public NYT::NOrm::NServer::NObjects::TDBTable
{
    TAuthorsTable()
        : NYT::NOrm::NServer::NObjects::TDBTable("authors")
    {
        Initialize(/*keyFields*/ {
            &Fields.MetaId
        },
        /*otherFields*/ {
            &Fields.MetaEtc,
            &Fields.ExistenceLock,
            &Fields.MetaHistorySnapshotTimestamp,
            &Fields.MetaRemovalTime,
            &Fields.MetaAcl,
            &Fields.MetaCreationTime,
            &Fields.MetaInheritAcl,
            &Fields.MetaFinalizationStartTime,
            &Fields.MetaFinalizers,
            &Fields.SpecName,
            &Fields.SpecAge,
            &Fields.SpecEtc,
            &Fields.StatusEtc,
            &Fields.Labels
        });
    }

    struct TFields
        : public NYT::NOrm::NServer::NObjects::TObjectTableBase::TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField MetaId{
            .Name = "meta.id",
            .Type = NYT::NTableClient::EValueType::String,
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaEtc{
            .Name = "meta.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFinalizationStartTime{
            .Name = "meta.finalization_start_time",
            .Type = NYT::NTableClient::EValueType::Uint64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFinalizers{
            .Name = "meta.finalizers",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecName{
            .Name = "spec.name",
            .Type = NYT::NTableClient::EValueType::String,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecAge{
            .Name = "spec.age",
            .Type = NYT::NTableClient::EValueType::Uint64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecEtc{
            .Name = "spec.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField StatusEtc{
            .Name = "status.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
    } Fields;
} AuthorsTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TBooksTable
    : public NYT::NOrm::NServer::NObjects::TDBTable
{
    TBooksTable()
        : NYT::NOrm::NServer::NObjects::TDBTable("books")
    {
        Initialize(/*keyFields*/ {
            &Fields.Hash,
            &Fields.MetaPublisherId,
            &Fields.MetaId,
            &Fields.MetaId2
        },
        /*otherFields*/ {
            &Fields.MetaEtc,
            &Fields.ExistenceLock,
            &Fields.MetaHistorySnapshotTimestamp,
            &Fields.MetaIsbn,
            &Fields.MetaRemovalTime,
            &Fields.MetaAcl,
            &Fields.MetaCreationTime,
            &Fields.MetaInheritAcl,
            &Fields.MetaFinalizationStartTime,
            &Fields.MetaFinalizers,
            &Fields.SpecName,
            &Fields.SpecYear,
            &Fields.SpecFont,
            &Fields.SpecGenres,
            &Fields.SpecKeywords,
            &Fields.SpecEditorId,
            &Fields.SpecDigitalData,
            &Fields.SpecAuthorIds,
            &Fields.SpecIllustratorId,
            &Fields.SpecAlternativePublisherIds,
            &Fields.SpecChapterDescriptions,
            &Fields.SpecCoverIllustratorId,
            &Fields.SpecApiRevision,
            &Fields.SpecPeerReviewReviewerIds,
            &Fields.SpecPeerReviewEtc,
            &Fields.SpecEtc,
            &Fields.SpecEtcWarehouse,
            &Fields.StatusReleased,
            &Fields.StatusEtc,
            &Fields.Labels
        });
    }

    struct TFields
        : public NYT::NOrm::NServer::NObjects::TObjectTableBase::TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField Hash{
            .Name = "hash",
            .Type = NYT::NTableClient::EValueType::Uint64,
            .Evaluated = true,
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaPublisherId{
            .Name = "meta.publisher_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaId{
            .Name = "meta.id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaId2{
            .Name = "meta.id2",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaEtc{
            .Name = "meta.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaIsbn{
            .Name = "meta.isbn",
            .Type = NYT::NTableClient::EValueType::String,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFinalizationStartTime{
            .Name = "meta.finalization_start_time",
            .Type = NYT::NTableClient::EValueType::Uint64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFinalizers{
            .Name = "meta.finalizers",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecName{
            .Name = "spec.name",
            .Type = NYT::NTableClient::EValueType::String,
            .LockGroup = "web",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecYear{
            .Name = "spec.year",
            .Type = NYT::NTableClient::EValueType::Int64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecFont{
            .Name = "spec.font",
            .Type = NYT::NTableClient::EValueType::String,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecGenres{
            .Name = "spec.genres",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecKeywords{
            .Name = "spec.keywords",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "search",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecEditorId{
            .Name = "spec.editor_id",
            .Type = NYT::NTableClient::EValueType::String,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecDigitalData{
            .Name = "spec.digital_data",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecAuthorIds{
            .Name = "spec.author_ids",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecIllustratorId{
            .Name = "spec.illustrator_id",
            .Type = NYT::NTableClient::EValueType::Int64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecAlternativePublisherIds{
            .Name = "spec.alternative_publisher_ids",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecChapterDescriptions{
            .Name = "spec.chapter_descriptions",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecCoverIllustratorId{
            .Name = "spec.cover_illustrator_id",
            .Type = NYT::NTableClient::EValueType::Int64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecApiRevision{
            .Name = "spec.api_revision",
            .Type = NYT::NTableClient::EValueType::Uint64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecPeerReviewReviewerIds{
            .Name = "spec.peer_review.reviewer_ids",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecPeerReviewEtc{
            .Name = "spec.peer_review.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecEtc{
            .Name = "spec.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecEtcWarehouse{
            .Name = "spec.etc$warehouse",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "warehouse",
        };
        NYT::NOrm::NServer::NObjects::TDBField StatusReleased{
            .Name = "status.released",
            .Type = NYT::NTableClient::EValueType::Boolean,
            .LockGroup = "status",
        };
        NYT::NOrm::NServer::NObjects::TDBField StatusEtc{
            .Name = "status.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
    } Fields;
} BooksTable;

extern const struct TBooksToPublishersTable
    : public NYT::NOrm::NServer::NObjects::TDBTable
{
    TBooksToPublishersTable()
        : NYT::NOrm::NServer::NObjects::TDBTable("books_to_publishers")
    {
        Initialize(/*keyFields*/ {
            &Fields.Hash,
            &Fields.MetaId,
            &Fields.MetaId2
        },
        /*otherFields*/ {
            &Fields.MetaPublisherId
        });
    }

    struct TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField Hash{
            .Name = "hash",
            .Type = NYT::NTableClient::EValueType::Uint64,
            .Evaluated = true,
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaId{
            .Name = "meta.id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaId2{
            .Name = "meta.id2",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaPublisherId{
            .Name = "meta.publisher_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
    } Fields;
} BooksToPublishersTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TBufferedTimestampIdsTable
    : public NYT::NOrm::NServer::NObjects::TDBTable
{
    TBufferedTimestampIdsTable()
        : NYT::NOrm::NServer::NObjects::TDBTable("buffered_timestamp_ids")
    {
        Initialize(/*keyFields*/ {
            &Fields.MetaI64Id,
            &Fields.MetaUi64Id
        },
        /*otherFields*/ {
            &Fields.MetaEtc,
            &Fields.ExistenceLock,
            &Fields.MetaHistorySnapshotTimestamp,
            &Fields.MetaRemovalTime,
            &Fields.MetaAcl,
            &Fields.MetaCreationTime,
            &Fields.MetaInheritAcl,
            &Fields.MetaFinalizationStartTime,
            &Fields.MetaFinalizers,
            &Fields.SpecI64Value,
            &Fields.SpecEtc,
            &Fields.StatusEtc,
            &Fields.Labels
        });
    }

    struct TFields
        : public NYT::NOrm::NServer::NObjects::TObjectTableBase::TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField MetaI64Id{
            .Name = "meta.i64_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaUi64Id{
            .Name = "meta.ui64_id",
            .Type = NYT::NTableClient::EValueType::Uint64,
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaEtc{
            .Name = "meta.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFinalizationStartTime{
            .Name = "meta.finalization_start_time",
            .Type = NYT::NTableClient::EValueType::Uint64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFinalizers{
            .Name = "meta.finalizers",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecI64Value{
            .Name = "spec.i64_value",
            .Type = NYT::NTableClient::EValueType::Int64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecEtc{
            .Name = "spec.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField StatusEtc{
            .Name = "status.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
    } Fields;
} BufferedTimestampIdsTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TCatsTable
    : public NYT::NOrm::NServer::NObjects::TDBTable
{
    TCatsTable()
        : NYT::NOrm::NServer::NObjects::TDBTable("cats")
    {
        Initialize(/*keyFields*/ {
            &Fields.MetaId
        },
        /*otherFields*/ {
            &Fields.MetaEtc,
            &Fields.ExistenceLock,
            &Fields.MetaHistorySnapshotTimestamp,
            &Fields.MetaBreed,
            &Fields.MetaRemovalTime,
            &Fields.MetaAcl,
            &Fields.MetaCreationTime,
            &Fields.MetaInheritAcl,
            &Fields.MetaFinalizationStartTime,
            &Fields.MetaFinalizers,
            &Fields.SpecLastSleepDuration,
            &Fields.SpecFavouriteFood,
            &Fields.SpecFavouriteToy,
            &Fields.SpecRevision,
            &Fields.SpecMood,
            &Fields.SpecHealthCondition,
            &Fields.SpecEyeColorWithDefaultYsonStorageType,
            &Fields.SpecFriendCatsCount,
            &Fields.SpecStatisticsForDaysOfYear,
            &Fields.SpecEtc,
            &Fields.StatusUpdatableNestedInnerFirst,
            &Fields.StatusUpdatableNestedInnerSecond,
            &Fields.StatusUpdatableNestedInnerThird,
            &Fields.StatusUpdatableNestedEtc,
            &Fields.StatusReadOnlyNestedInnerFirst,
            &Fields.StatusReadOnlyNestedInnerSecond,
            &Fields.StatusReadOnlyNestedInnerThird,
            &Fields.StatusReadOnlyNestedEtc,
            &Fields.StatusOpaqueRoNestedInnerFirst,
            &Fields.StatusOpaqueRoNestedInnerSecond,
            &Fields.StatusOpaqueRoNestedInnerThird,
            &Fields.StatusOpaqueRoNestedEtc,
            &Fields.StatusOpaqueNestedInnerFirst,
            &Fields.StatusOpaqueNestedInnerSecond,
            &Fields.StatusOpaqueNestedInnerThird,
            &Fields.StatusOpaqueNestedEtc,
            &Fields.StatusEtc,
            &Fields.Labels
        });
    }

    struct TFields
        : public NYT::NOrm::NServer::NObjects::TObjectTableBase::TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField MetaId{
            .Name = "meta.id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaEtc{
            .Name = "meta.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaBreed{
            .Name = "meta.breed",
            .Type = NYT::NTableClient::EValueType::String,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFinalizationStartTime{
            .Name = "meta.finalization_start_time",
            .Type = NYT::NTableClient::EValueType::Uint64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFinalizers{
            .Name = "meta.finalizers",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecLastSleepDuration{
            .Name = "spec.last_sleep_duration",
            .Type = NYT::NTableClient::EValueType::Int64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecFavouriteFood{
            .Name = "spec.favourite_food",
            .Type = NYT::NTableClient::EValueType::String,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecFavouriteToy{
            .Name = "spec.favourite_toy",
            .Type = NYT::NTableClient::EValueType::String,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecRevision{
            .Name = "spec.revision",
            .Type = NYT::NTableClient::EValueType::Uint64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecMood{
            .Name = "spec.mood_state",
            .Type = NYT::NTableClient::EValueType::Int64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecHealthCondition{
            .Name = "spec.health_condition",
            .Type = NYT::NTableClient::EValueType::String,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecEyeColorWithDefaultYsonStorageType{
            .Name = "spec.eye_color_with_default_yson_storage_type",
            .Type = NYT::NTableClient::EValueType::Int64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecFriendCatsCount{
            .Name = "spec.friend_cats_count",
            .Type = NYT::NTableClient::EValueType::Int64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecStatisticsForDaysOfYear{
            .Name = "spec.statistics_for_days_of_year",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecEtc{
            .Name = "spec.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField StatusUpdatableNestedInnerFirst{
            .Name = "status.updatable_nested.inner_first",
            .Type = NYT::NTableClient::EValueType::Int64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField StatusUpdatableNestedInnerSecond{
            .Name = "status.updatable_nested.inner_second",
            .Type = NYT::NTableClient::EValueType::Int64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField StatusUpdatableNestedInnerThird{
            .Name = "status.updatable_nested.inner_third",
            .Type = NYT::NTableClient::EValueType::Int64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField StatusUpdatableNestedEtc{
            .Name = "status.updatable_nested.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField StatusReadOnlyNestedInnerFirst{
            .Name = "status.read_only_nested.inner_first",
            .Type = NYT::NTableClient::EValueType::Int64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField StatusReadOnlyNestedInnerSecond{
            .Name = "status.read_only_nested.inner_second",
            .Type = NYT::NTableClient::EValueType::Int64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField StatusReadOnlyNestedInnerThird{
            .Name = "status.read_only_nested.inner_third",
            .Type = NYT::NTableClient::EValueType::Int64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField StatusReadOnlyNestedEtc{
            .Name = "status.read_only_nested.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField StatusOpaqueRoNestedInnerFirst{
            .Name = "status.opaque_ro_nested.inner_first",
            .Type = NYT::NTableClient::EValueType::Int64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField StatusOpaqueRoNestedInnerSecond{
            .Name = "status.opaque_ro_nested.inner_second",
            .Type = NYT::NTableClient::EValueType::Int64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField StatusOpaqueRoNestedInnerThird{
            .Name = "status.opaque_ro_nested.inner_third",
            .Type = NYT::NTableClient::EValueType::Int64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField StatusOpaqueRoNestedEtc{
            .Name = "status.opaque_ro_nested.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField StatusOpaqueNestedInnerFirst{
            .Name = "status.opaque_nested.inner_first",
            .Type = NYT::NTableClient::EValueType::Int64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField StatusOpaqueNestedInnerSecond{
            .Name = "status.opaque_nested.inner_second",
            .Type = NYT::NTableClient::EValueType::Int64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField StatusOpaqueNestedInnerThird{
            .Name = "status.opaque_nested.inner_third",
            .Type = NYT::NTableClient::EValueType::Int64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField StatusOpaqueNestedEtc{
            .Name = "status.opaque_nested.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField StatusEtc{
            .Name = "status.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
    } Fields;
} CatsTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TEditorsTable
    : public NYT::NOrm::NServer::NObjects::TDBTable
{
    TEditorsTable()
        : NYT::NOrm::NServer::NObjects::TDBTable("editors")
    {
        Initialize(/*keyFields*/ {
            &Fields.MetaId
        },
        /*otherFields*/ {
            &Fields.MetaEtc,
            &Fields.ExistenceLock,
            &Fields.MetaHistorySnapshotTimestamp,
            &Fields.MetaRemovalTime,
            &Fields.MetaAcl,
            &Fields.MetaCreationTime,
            &Fields.MetaInheritAcl,
            &Fields.MetaFinalizationStartTime,
            &Fields.MetaFinalizers,
            &Fields.SpecAchievements,
            &Fields.SpecPhoneNumber,
            &Fields.SpecEtc,
            &Fields.StatusEtc,
            &Fields.Labels
        });
    }

    struct TFields
        : public NYT::NOrm::NServer::NObjects::TObjectTableBase::TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField MetaId{
            .Name = "meta.id",
            .Type = NYT::NTableClient::EValueType::String,
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaEtc{
            .Name = "meta.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFinalizationStartTime{
            .Name = "meta.finalization_start_time",
            .Type = NYT::NTableClient::EValueType::Uint64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFinalizers{
            .Name = "meta.finalizers",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecAchievements{
            .Name = "spec.achievements",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecPhoneNumber{
            .Name = "spec.phone_number",
            .Type = NYT::NTableClient::EValueType::String,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecEtc{
            .Name = "spec.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField StatusEtc{
            .Name = "status.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
    } Fields;
} EditorsTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TEmployersTable
    : public NYT::NOrm::NServer::NObjects::TDBTable
{
    TEmployersTable()
        : NYT::NOrm::NServer::NObjects::TDBTable("employers")
    {
        Initialize(/*keyFields*/ {
            &Fields.MetaId
        },
        /*otherFields*/ {
            &Fields.MetaEtc,
            &Fields.ExistenceLock,
            &Fields.MetaHistorySnapshotTimestamp,
            &Fields.MetaEmail,
            &Fields.MetaRemovalTime,
            &Fields.MetaAcl,
            &Fields.MetaCreationTime,
            &Fields.MetaInheritAcl,
            &Fields.MetaFinalizationStartTime,
            &Fields.MetaFinalizers,
            &Fields.Spec,
            &Fields.StatusSalary,
            &Fields.Status,
            &Fields.Labels
        });
    }

    struct TFields
        : public NYT::NOrm::NServer::NObjects::TObjectTableBase::TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField MetaId{
            .Name = "meta.id",
            .Type = NYT::NTableClient::EValueType::String,
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaEtc{
            .Name = "meta.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaEmail{
            .Name = "meta.email",
            .Type = NYT::NTableClient::EValueType::String,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFinalizationStartTime{
            .Name = "meta.finalization_start_time",
            .Type = NYT::NTableClient::EValueType::Uint64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFinalizers{
            .Name = "meta.finalizers",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField Spec{
            .Name = "spec",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField StatusSalary{
            .Name = "status.salary",
            .Type = NYT::NTableClient::EValueType::Int64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField Status{
            .Name = "status",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
    } Fields;
} EmployersTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TExecutorsTable
    : public NYT::NOrm::NServer::NObjects::TDBTable
{
    TExecutorsTable()
        : NYT::NOrm::NServer::NObjects::TDBTable("executors")
    {
        Initialize(/*keyFields*/ {
            &Fields.MetaId
        },
        /*otherFields*/ {
            &Fields.MetaEtc,
            &Fields.ExistenceLock,
            &Fields.MetaHistorySnapshotTimestamp,
            &Fields.MetaRemovalTime,
            &Fields.MetaAcl,
            &Fields.MetaCreationTime,
            &Fields.MetaInheritAcl,
            &Fields.MetaFinalizationStartTime,
            &Fields.MetaFinalizers,
            &Fields.SpecEtc,
            &Fields.StatusEtc,
            &Fields.Labels
        });
    }

    struct TFields
        : public NYT::NOrm::NServer::NObjects::TObjectTableBase::TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField MetaId{
            .Name = "meta.id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaEtc{
            .Name = "meta.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFinalizationStartTime{
            .Name = "meta.finalization_start_time",
            .Type = NYT::NTableClient::EValueType::Uint64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFinalizers{
            .Name = "meta.finalizers",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecEtc{
            .Name = "spec.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField StatusEtc{
            .Name = "status.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
    } Fields;
} ExecutorsTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TGenresTable
    : public NYT::NOrm::NServer::NObjects::TDBTable
{
    TGenresTable()
        : NYT::NOrm::NServer::NObjects::TDBTable("genres")
    {
        Initialize(/*keyFields*/ {
            &Fields.MetaId
        },
        /*otherFields*/ {
            &Fields.MetaEtc,
            &Fields.ExistenceLock,
            &Fields.MetaHistorySnapshotTimestamp,
            &Fields.MetaRemovalTime,
            &Fields.MetaAcl,
            &Fields.MetaCreationTime,
            &Fields.MetaInheritAcl,
            &Fields.MetaFinalizationStartTime,
            &Fields.MetaFinalizers,
            &Fields.SpecEtc,
            &Fields.StatusEtc,
            &Fields.Labels
        });
    }

    struct TFields
        : public NYT::NOrm::NServer::NObjects::TObjectTableBase::TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField MetaId{
            .Name = "meta.id",
            .Type = NYT::NTableClient::EValueType::String,
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaEtc{
            .Name = "meta.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFinalizationStartTime{
            .Name = "meta.finalization_start_time",
            .Type = NYT::NTableClient::EValueType::Uint64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFinalizers{
            .Name = "meta.finalizers",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecEtc{
            .Name = "spec.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField StatusEtc{
            .Name = "status.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
    } Fields;
} GenresTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TGroupsTable
    : public NYT::NOrm::NServer::NObjects::TDBTable
{
    TGroupsTable()
        : NYT::NOrm::NServer::NObjects::TDBTable("groups")
    {
        Initialize(/*keyFields*/ {
            &Fields.MetaId
        },
        /*otherFields*/ {
            &Fields.MetaEtc,
            &Fields.ExistenceLock,
            &Fields.MetaHistorySnapshotTimestamp,
            &Fields.MetaRemovalTime,
            &Fields.MetaAcl,
            &Fields.MetaCreationTime,
            &Fields.MetaInheritAcl,
            &Fields.MetaFinalizationStartTime,
            &Fields.MetaFinalizers,
            &Fields.Spec,
            &Fields.StatusEtc,
            &Fields.Labels
        });
    }

    struct TFields
        : public NYT::NOrm::NServer::NObjects::TObjectTableBase::TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField MetaId{
            .Name = "meta.id",
            .Type = NYT::NTableClient::EValueType::String,
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaEtc{
            .Name = "meta.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFinalizationStartTime{
            .Name = "meta.finalization_start_time",
            .Type = NYT::NTableClient::EValueType::Uint64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFinalizers{
            .Name = "meta.finalizers",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField Spec{
            .Name = "spec",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField StatusEtc{
            .Name = "status.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
    } Fields;
} GroupsTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct THitchhikersTable
    : public NYT::NOrm::NServer::NObjects::TDBTable
{
    THitchhikersTable()
        : NYT::NOrm::NServer::NObjects::TDBTable("hitchhikers")
    {
        Initialize(/*keyFields*/ {
            &Fields.MetaId
        },
        /*otherFields*/ {
            &Fields.MetaEtc,
            &Fields.ExistenceLock,
            &Fields.MetaHistorySnapshotTimestamp,
            &Fields.MetaFormativeBookId,
            &Fields.MetaFormativeBookId2,
            &Fields.MetaRemovalTime,
            &Fields.MetaAcl,
            &Fields.MetaCreationTime,
            &Fields.MetaInheritAcl,
            &Fields.MetaFinalizationStartTime,
            &Fields.MetaFinalizers,
            &Fields.SpecHatedBooks,
            &Fields.SpecEtc,
            &Fields.StatusEtc,
            &Fields.Labels
        });
    }

    struct TFields
        : public NYT::NOrm::NServer::NObjects::TObjectTableBase::TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField MetaId{
            .Name = "meta.id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaEtc{
            .Name = "meta.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFormativeBookId{
            .Name = "meta.formative_book_id",
            .Type = NYT::NTableClient::EValueType::Int64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFormativeBookId2{
            .Name = "meta.formative_book_id2",
            .Type = NYT::NTableClient::EValueType::Int64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFinalizationStartTime{
            .Name = "meta.finalization_start_time",
            .Type = NYT::NTableClient::EValueType::Uint64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFinalizers{
            .Name = "meta.finalizers",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecHatedBooks{
            .Name = "spec.hated_books",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecEtc{
            .Name = "spec.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField StatusEtc{
            .Name = "status.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
    } Fields;
} HitchhikersTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TIllustratorsTable
    : public NYT::NOrm::NServer::NObjects::TDBTable
{
    TIllustratorsTable()
        : NYT::NOrm::NServer::NObjects::TDBTable("illustrators")
    {
        Initialize(/*keyFields*/ {
            &Fields.MetaPublisherId,
            &Fields.MetaUid
        },
        /*otherFields*/ {
            &Fields.MetaEtc,
            &Fields.ExistenceLock,
            &Fields.MetaHistorySnapshotTimestamp,
            &Fields.MetaPartTimeJob,
            &Fields.MetaRemovalTime,
            &Fields.MetaAcl,
            &Fields.MetaCreationTime,
            &Fields.MetaInheritAcl,
            &Fields.MetaFinalizationStartTime,
            &Fields.MetaFinalizers,
            &Fields.Spec,
            &Fields.StatusEtc,
            &Fields.Labels
        });
    }

    struct TFields
        : public NYT::NOrm::NServer::NObjects::TObjectTableBase::TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField MetaPublisherId{
            .Name = "meta.publisher_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaUid{
            .Name = "meta.uid",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaEtc{
            .Name = "meta.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaPartTimeJob{
            .Name = "meta.part_time_job",
            .Type = NYT::NTableClient::EValueType::Int64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFinalizationStartTime{
            .Name = "meta.finalization_start_time",
            .Type = NYT::NTableClient::EValueType::Uint64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFinalizers{
            .Name = "meta.finalizers",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField Spec{
            .Name = "spec",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField StatusEtc{
            .Name = "status.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
    } Fields;
} IllustratorsTable;

extern const struct TIllustratorsToPublishersTable
    : public NYT::NOrm::NServer::NObjects::TDBTable
{
    TIllustratorsToPublishersTable()
        : NYT::NOrm::NServer::NObjects::TDBTable("illustrators_to_publishers")
    {
        Initialize(/*keyFields*/ {
            &Fields.MetaUid
        },
        /*otherFields*/ {
            &Fields.MetaPublisherId
        });
    }

    struct TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField MetaUid{
            .Name = "meta.uid",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaPublisherId{
            .Name = "meta.publisher_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
    } Fields;
} IllustratorsToPublishersTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TIndexedIncrementIdsTable
    : public NYT::NOrm::NServer::NObjects::TDBTable
{
    TIndexedIncrementIdsTable()
        : NYT::NOrm::NServer::NObjects::TDBTable("indexed_increment_ids")
    {
        Initialize(/*keyFields*/ {
            &Fields.MetaI64Id
        },
        /*otherFields*/ {
            &Fields.MetaEtc,
            &Fields.ExistenceLock,
            &Fields.MetaHistorySnapshotTimestamp,
            &Fields.MetaRemovalTime,
            &Fields.MetaAcl,
            &Fields.MetaCreationTime,
            &Fields.MetaInheritAcl,
            &Fields.MetaFinalizationStartTime,
            &Fields.MetaFinalizers,
            &Fields.SpecEtc,
            &Fields.StatusEtc,
            &Fields.Labels
        });
    }

    struct TFields
        : public NYT::NOrm::NServer::NObjects::TObjectTableBase::TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField MetaI64Id{
            .Name = "meta.i64_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaEtc{
            .Name = "meta.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFinalizationStartTime{
            .Name = "meta.finalization_start_time",
            .Type = NYT::NTableClient::EValueType::Uint64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFinalizers{
            .Name = "meta.finalizers",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecEtc{
            .Name = "spec.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField StatusEtc{
            .Name = "status.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
    } Fields;
} IndexedIncrementIdsTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TInterceptorsTable
    : public NYT::NOrm::NServer::NObjects::TDBTable
{
    TInterceptorsTable()
        : NYT::NOrm::NServer::NObjects::TDBTable("interceptors")
    {
        Initialize(/*keyFields*/ {
            &Fields.MetaMotherShipId,
            &Fields.MetaId
        },
        /*otherFields*/ {
            &Fields.MetaEtc,
            &Fields.ExistenceLock,
            &Fields.MetaHistorySnapshotTimestamp,
            &Fields.MetaNexusId,
            &Fields.MetaRemovalTime,
            &Fields.MetaAcl,
            &Fields.MetaCreationTime,
            &Fields.MetaInheritAcl,
            &Fields.MetaFinalizationStartTime,
            &Fields.MetaFinalizers,
            &Fields.SpecEtc,
            &Fields.StatusEtc,
            &Fields.Labels
        });
    }

    struct TFields
        : public NYT::NOrm::NServer::NObjects::TObjectTableBase::TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField MetaMotherShipId{
            .Name = "meta.mother_ship_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaId{
            .Name = "meta.id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaEtc{
            .Name = "meta.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaNexusId{
            .Name = "meta.nexus_id",
            .Type = NYT::NTableClient::EValueType::Int64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFinalizationStartTime{
            .Name = "meta.finalization_start_time",
            .Type = NYT::NTableClient::EValueType::Uint64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFinalizers{
            .Name = "meta.finalizers",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecEtc{
            .Name = "spec.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField StatusEtc{
            .Name = "status.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
    } Fields;
} InterceptorsTable;

extern const struct TInterceptorsToMotherShipsTable
    : public NYT::NOrm::NServer::NObjects::TDBTable
{
    TInterceptorsToMotherShipsTable()
        : NYT::NOrm::NServer::NObjects::TDBTable("interceptors_to_mother_ships")
    {
        Initialize(/*keyFields*/ {
            &Fields.MetaId
        },
        /*otherFields*/ {
            &Fields.MetaMotherShipId
        });
    }

    struct TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField MetaId{
            .Name = "meta.id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaMotherShipId{
            .Name = "meta.mother_ship_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
    } Fields;
} InterceptorsToMotherShipsTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TManualIdsTable
    : public NYT::NOrm::NServer::NObjects::TDBTable
{
    TManualIdsTable()
        : NYT::NOrm::NServer::NObjects::TDBTable("manual_ids")
    {
        Initialize(/*keyFields*/ {
            &Fields.MetaStrId,
            &Fields.MetaI64Id,
            &Fields.MetaUi64Id
        },
        /*otherFields*/ {
            &Fields.MetaEtc,
            &Fields.ExistenceLock,
            &Fields.MetaHistorySnapshotTimestamp,
            &Fields.MetaRemovalTime,
            &Fields.MetaAcl,
            &Fields.MetaCreationTime,
            &Fields.MetaInheritAcl,
            &Fields.MetaFinalizationStartTime,
            &Fields.MetaFinalizers,
            &Fields.SpecStrValue,
            &Fields.SpecI32Value,
            &Fields.SpecUi32Value,
            &Fields.SpecEtc,
            &Fields.StatusEtc,
            &Fields.Labels
        });
    }

    struct TFields
        : public NYT::NOrm::NServer::NObjects::TObjectTableBase::TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField MetaStrId{
            .Name = "meta.str_id",
            .Type = NYT::NTableClient::EValueType::String,
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaI64Id{
            .Name = "meta.i64_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaUi64Id{
            .Name = "meta.ui64_id",
            .Type = NYT::NTableClient::EValueType::Uint64,
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaEtc{
            .Name = "meta.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFinalizationStartTime{
            .Name = "meta.finalization_start_time",
            .Type = NYT::NTableClient::EValueType::Uint64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFinalizers{
            .Name = "meta.finalizers",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecStrValue{
            .Name = "spec.str_value",
            .Type = NYT::NTableClient::EValueType::String,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecI32Value{
            .Name = "spec.i32_value",
            .Type = NYT::NTableClient::EValueType::Int64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecUi32Value{
            .Name = "spec.ui32_value",
            .Type = NYT::NTableClient::EValueType::Uint64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecEtc{
            .Name = "spec.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField StatusEtc{
            .Name = "status.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
    } Fields;
} ManualIdsTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TMotherShipsTable
    : public NYT::NOrm::NServer::NObjects::TDBTable
{
    TMotherShipsTable()
        : NYT::NOrm::NServer::NObjects::TDBTable("mother_ships")
    {
        Initialize(/*keyFields*/ {
            &Fields.MetaNexusId,
            &Fields.MetaId
        },
        /*otherFields*/ {
            &Fields.MetaEtc,
            &Fields.ExistenceLock,
            &Fields.MetaHistorySnapshotTimestamp,
            &Fields.MetaRevision,
            &Fields.MetaReleaseYear,
            &Fields.MetaRemovalTime,
            &Fields.MetaAcl,
            &Fields.MetaCreationTime,
            &Fields.MetaInheritAcl,
            &Fields.MetaFinalizationStartTime,
            &Fields.MetaFinalizers,
            &Fields.SpecExecutorId,
            &Fields.SpecRevision,
            &Fields.SpecSectorNames,
            &Fields.SpecPrice,
            &Fields.SpecEtc,
            &Fields.StatusEtc,
            &Fields.Labels
        });
    }

    struct TFields
        : public NYT::NOrm::NServer::NObjects::TObjectTableBase::TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField MetaNexusId{
            .Name = "meta.nexus_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaId{
            .Name = "meta.id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaEtc{
            .Name = "meta.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaRevision{
            .Name = "meta.revision",
            .Type = NYT::NTableClient::EValueType::Uint64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaReleaseYear{
            .Name = "meta.release_year",
            .Type = NYT::NTableClient::EValueType::Int64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFinalizationStartTime{
            .Name = "meta.finalization_start_time",
            .Type = NYT::NTableClient::EValueType::Uint64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFinalizers{
            .Name = "meta.finalizers",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecExecutorId{
            .Name = "spec.executor_id",
            .Type = NYT::NTableClient::EValueType::Int64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecRevision{
            .Name = "spec.revision",
            .Type = NYT::NTableClient::EValueType::Uint64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecSectorNames{
            .Name = "spec.sector_names",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecPrice{
            .Name = "spec.price",
            .Type = NYT::NTableClient::EValueType::Int64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecEtc{
            .Name = "spec.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField StatusEtc{
            .Name = "status.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
    } Fields;
} MotherShipsTable;

extern const struct TMotherShipsToNexusTable
    : public NYT::NOrm::NServer::NObjects::TDBTable
{
    TMotherShipsToNexusTable()
        : NYT::NOrm::NServer::NObjects::TDBTable("mother_ships_to_nexus")
    {
        Initialize(/*keyFields*/ {
            &Fields.MetaId
        },
        /*otherFields*/ {
            &Fields.MetaNexusId
        });
    }

    struct TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField MetaId{
            .Name = "meta.id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaNexusId{
            .Name = "meta.nexus_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
    } Fields;
} MotherShipsToNexusTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TMultipolicyIdsTable
    : public NYT::NOrm::NServer::NObjects::TDBTable
{
    TMultipolicyIdsTable()
        : NYT::NOrm::NServer::NObjects::TDBTable("multipolicy_ids")
    {
        Initialize(/*keyFields*/ {
            &Fields.MetaStrId,
            &Fields.MetaI64Id,
            &Fields.MetaUi64Id,
            &Fields.MetaAnotherUi64Id
        },
        /*otherFields*/ {
            &Fields.MetaEtc,
            &Fields.ExistenceLock,
            &Fields.MetaHistorySnapshotTimestamp,
            &Fields.MetaRemovalTime,
            &Fields.MetaAcl,
            &Fields.MetaCreationTime,
            &Fields.MetaInheritAcl,
            &Fields.MetaFinalizationStartTime,
            &Fields.MetaFinalizers,
            &Fields.SpecStrValue,
            &Fields.SpecI64Value,
            &Fields.SpecUi64Value,
            &Fields.SpecAnotherUi64Value,
            &Fields.SpecEtc,
            &Fields.StatusEtc,
            &Fields.Labels
        });
    }

    struct TFields
        : public NYT::NOrm::NServer::NObjects::TObjectTableBase::TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField MetaStrId{
            .Name = "meta.str_id",
            .Type = NYT::NTableClient::EValueType::String,
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaI64Id{
            .Name = "meta.i64_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaUi64Id{
            .Name = "meta.ui64_id",
            .Type = NYT::NTableClient::EValueType::Uint64,
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaAnotherUi64Id{
            .Name = "meta.another_ui64_id",
            .Type = NYT::NTableClient::EValueType::Uint64,
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaEtc{
            .Name = "meta.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFinalizationStartTime{
            .Name = "meta.finalization_start_time",
            .Type = NYT::NTableClient::EValueType::Uint64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFinalizers{
            .Name = "meta.finalizers",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecStrValue{
            .Name = "spec.str_value",
            .Type = NYT::NTableClient::EValueType::String,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecI64Value{
            .Name = "spec.i64_value",
            .Type = NYT::NTableClient::EValueType::Int64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecUi64Value{
            .Name = "spec.ui64_value",
            .Type = NYT::NTableClient::EValueType::Uint64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecAnotherUi64Value{
            .Name = "spec.another_ui64_value",
            .Type = NYT::NTableClient::EValueType::Uint64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecEtc{
            .Name = "spec.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField StatusEtc{
            .Name = "status.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
    } Fields;
} MultipolicyIdsTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TNestedColumnsTable
    : public NYT::NOrm::NServer::NObjects::TDBTable
{
    TNestedColumnsTable()
        : NYT::NOrm::NServer::NObjects::TDBTable("nested_columns")
    {
        Initialize(/*keyFields*/ {
            &Fields.MetaId
        },
        /*otherFields*/ {
            &Fields.MetaEtc,
            &Fields.ExistenceLock,
            &Fields.MetaHistorySnapshotTimestamp,
            &Fields.MetaRemovalTime,
            &Fields.MetaAcl,
            &Fields.MetaCreationTime,
            &Fields.MetaInheritAcl,
            &Fields.MetaFinalizationStartTime,
            &Fields.MetaFinalizers,
            &Fields.SpecColumnSingular,
            &Fields.SpecColumnRepeated,
            &Fields.SpecColumnMap,
            &Fields.SpecCompositeSingularColumnSingular,
            &Fields.SpecCompositeSingularColumnRepeated,
            &Fields.SpecCompositeSingularColumnMap,
            &Fields.SpecCompositeSingularDeprecatedColumnSingular,
            &Fields.SpecCompositeSingular,
            &Fields.SpecDeprecatedColumnSingular,
            &Fields.SpecEtc,
            &Fields.StatusEtc,
            &Fields.Labels
        });
    }

    struct TFields
        : public NYT::NOrm::NServer::NObjects::TObjectTableBase::TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField MetaId{
            .Name = "meta.id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaEtc{
            .Name = "meta.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFinalizationStartTime{
            .Name = "meta.finalization_start_time",
            .Type = NYT::NTableClient::EValueType::Uint64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFinalizers{
            .Name = "meta.finalizers",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecColumnSingular{
            .Name = "spec.column_singular",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecColumnRepeated{
            .Name = "spec.column_repeated",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecColumnMap{
            .Name = "spec.column_map",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecCompositeSingularColumnSingular{
            .Name = "spec.composite_singular.column_singular",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecCompositeSingularColumnRepeated{
            .Name = "spec.composite_singular.column_repeated",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecCompositeSingularColumnMap{
            .Name = "spec.composite_singular.column_map",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecCompositeSingularDeprecatedColumnSingular{
            .Name = "spec.composite_singular.deprecated_column_singular",
            .Type = NYT::NTableClient::EValueType::Int64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecCompositeSingular{
            .Name = "spec.composite_singular",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecDeprecatedColumnSingular{
            .Name = "spec.deprecated_column_singular",
            .Type = NYT::NTableClient::EValueType::Int64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecEtc{
            .Name = "spec.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField StatusEtc{
            .Name = "status.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
    } Fields;
} NestedColumnsTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TNexusTable
    : public NYT::NOrm::NServer::NObjects::TDBTable
{
    TNexusTable()
        : NYT::NOrm::NServer::NObjects::TDBTable("nexus")
    {
        Initialize(/*keyFields*/ {
            &Fields.MetaId
        },
        /*otherFields*/ {
            &Fields.MetaEtc,
            &Fields.ExistenceLock,
            &Fields.MetaHistorySnapshotTimestamp,
            &Fields.MetaRemovalTime,
            &Fields.MetaAcl,
            &Fields.MetaCreationTime,
            &Fields.MetaInheritAcl,
            &Fields.MetaFinalizationStartTime,
            &Fields.MetaFinalizers,
            &Fields.SpecSomeMapToMessageColumn,
            &Fields.SpecEtc,
            &Fields.StatusColumnSemaphore,
            &Fields.StatusEtc,
            &Fields.Labels
        });
    }

    struct TFields
        : public NYT::NOrm::NServer::NObjects::TObjectTableBase::TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField MetaId{
            .Name = "meta.id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaEtc{
            .Name = "meta.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFinalizationStartTime{
            .Name = "meta.finalization_start_time",
            .Type = NYT::NTableClient::EValueType::Uint64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFinalizers{
            .Name = "meta.finalizers",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecSomeMapToMessageColumn{
            .Name = "spec.some_map_to_message_column",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecEtc{
            .Name = "spec.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField StatusColumnSemaphore{
            .Name = "status.column_semaphore",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField StatusEtc{
            .Name = "status.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
    } Fields;
} NexusTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TNirvanaDMProcessInstancesTable
    : public NYT::NOrm::NServer::NObjects::TDBTable
{
    TNirvanaDMProcessInstancesTable()
        : NYT::NOrm::NServer::NObjects::TDBTable("nirvana_dm_process_instances")
    {
        Initialize(/*keyFields*/ {
            &Fields.MetaId
        },
        /*otherFields*/ {
            &Fields.MetaEtc,
            &Fields.ExistenceLock,
            &Fields.MetaHistorySnapshotTimestamp,
            &Fields.MetaRemovalTime,
            &Fields.MetaAcl,
            &Fields.MetaCreationTime,
            &Fields.MetaInheritAcl,
            &Fields.MetaFinalizationStartTime,
            &Fields.MetaFinalizers,
            &Fields.SpecEtc,
            &Fields.StatusEtc,
            &Fields.Labels
        });
    }

    struct TFields
        : public NYT::NOrm::NServer::NObjects::TObjectTableBase::TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField MetaId{
            .Name = "meta.id",
            .Type = NYT::NTableClient::EValueType::String,
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaEtc{
            .Name = "meta.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFinalizationStartTime{
            .Name = "meta.finalization_start_time",
            .Type = NYT::NTableClient::EValueType::Uint64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFinalizers{
            .Name = "meta.finalizers",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecEtc{
            .Name = "spec.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField StatusEtc{
            .Name = "status.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
    } Fields;
} NirvanaDMProcessInstancesTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TPublishersTable
    : public NYT::NOrm::NServer::NObjects::TDBTable
{
    TPublishersTable()
        : NYT::NOrm::NServer::NObjects::TDBTable("publishers")
    {
        Initialize(/*keyFields*/ {
            &Fields.Hash,
            &Fields.MetaId
        },
        /*otherFields*/ {
            &Fields.MetaEtc,
            &Fields.ExistenceLock,
            &Fields.MetaHistorySnapshotTimestamp,
            &Fields.MetaRemovalTime,
            &Fields.MetaAcl,
            &Fields.MetaCreationTime,
            &Fields.MetaInheritAcl,
            &Fields.MetaFinalizationStartTime,
            &Fields.MetaFinalizers,
            &Fields.SpecName,
            &Fields.SpecEditorInChief,
            &Fields.SpecIllustratorInChief,
            &Fields.SpecPublisherGroup,
            &Fields.SpecColumnField,
            &Fields.SpecFeaturedIllustrators,
            &Fields.SpecEtc,
            &Fields.StatusColumnField,
            &Fields.StatusColumnList,
            &Fields.StatusFeaturedIllustrators,
            &Fields.StatusEtc,
            &Fields.Labels
        });
    }

    struct TFields
        : public NYT::NOrm::NServer::NObjects::TObjectTableBase::TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField Hash{
            .Name = "hash",
            .Type = NYT::NTableClient::EValueType::Uint64,
            .Evaluated = true,
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaId{
            .Name = "meta.id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaEtc{
            .Name = "meta.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFinalizationStartTime{
            .Name = "meta.finalization_start_time",
            .Type = NYT::NTableClient::EValueType::Uint64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFinalizers{
            .Name = "meta.finalizers",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecName{
            .Name = "spec.name",
            .Type = NYT::NTableClient::EValueType::String,
            .LockGroup = "web",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecEditorInChief{
            .Name = "spec.editor_in_chief",
            .Type = NYT::NTableClient::EValueType::String,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecIllustratorInChief{
            .Name = "spec.illustrator_in_chief",
            .Type = NYT::NTableClient::EValueType::Int64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecPublisherGroup{
            .Name = "spec.publisher_group",
            .Type = NYT::NTableClient::EValueType::Int64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecColumnField{
            .Name = "spec.column_field",
            .Type = NYT::NTableClient::EValueType::Int64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecFeaturedIllustrators{
            .Name = "spec.featured_illustrators",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecEtc{
            .Name = "spec.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField StatusColumnField{
            .Name = "status.column_field",
            .Type = NYT::NTableClient::EValueType::Int64,
            .LockGroup = "worker",
        };
        NYT::NOrm::NServer::NObjects::TDBField StatusColumnList{
            .Name = "status.column_list",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "worker",
        };
        NYT::NOrm::NServer::NObjects::TDBField StatusFeaturedIllustrators{
            .Name = "status.featured_illustrators",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "worker",
        };
        NYT::NOrm::NServer::NObjects::TDBField StatusEtc{
            .Name = "status.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "worker",
        };
    } Fields;
} PublishersTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TRandomIdsTable
    : public NYT::NOrm::NServer::NObjects::TDBTable
{
    TRandomIdsTable()
        : NYT::NOrm::NServer::NObjects::TDBTable("random_ids")
    {
        Initialize(/*keyFields*/ {
            &Fields.MetaStrId,
            &Fields.MetaI64Id,
            &Fields.MetaUi64Id
        },
        /*otherFields*/ {
            &Fields.MetaEtc,
            &Fields.ExistenceLock,
            &Fields.MetaHistorySnapshotTimestamp,
            &Fields.MetaRemovalTime,
            &Fields.MetaAcl,
            &Fields.MetaCreationTime,
            &Fields.MetaInheritAcl,
            &Fields.MetaFinalizationStartTime,
            &Fields.MetaFinalizers,
            &Fields.SpecStrValue,
            &Fields.SpecEtc,
            &Fields.StatusEtc,
            &Fields.Labels
        });
    }

    struct TFields
        : public NYT::NOrm::NServer::NObjects::TObjectTableBase::TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField MetaStrId{
            .Name = "meta.str_id",
            .Type = NYT::NTableClient::EValueType::String,
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaI64Id{
            .Name = "meta.i64_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaUi64Id{
            .Name = "meta.ui64_id",
            .Type = NYT::NTableClient::EValueType::Uint64,
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaEtc{
            .Name = "meta.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFinalizationStartTime{
            .Name = "meta.finalization_start_time",
            .Type = NYT::NTableClient::EValueType::Uint64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFinalizers{
            .Name = "meta.finalizers",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecStrValue{
            .Name = "spec.str_value",
            .Type = NYT::NTableClient::EValueType::String,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecEtc{
            .Name = "spec.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField StatusEtc{
            .Name = "status.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
    } Fields;
} RandomIdsTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TSchemasTable
    : public NYT::NOrm::NServer::NObjects::TDBTable
{
    TSchemasTable()
        : NYT::NOrm::NServer::NObjects::TDBTable("schemas")
    {
        Initialize(/*keyFields*/ {
            &Fields.MetaId
        },
        /*otherFields*/ {
            &Fields.MetaEtc,
            &Fields.ExistenceLock,
            &Fields.MetaHistorySnapshotTimestamp,
            &Fields.MetaRemovalTime,
            &Fields.MetaAcl,
            &Fields.MetaCreationTime,
            &Fields.MetaInheritAcl,
            &Fields.MetaFinalizationStartTime,
            &Fields.MetaFinalizers,
            &Fields.SpecEtc,
            &Fields.StatusEtc,
            &Fields.Labels
        });
    }

    struct TFields
        : public NYT::NOrm::NServer::NObjects::TObjectTableBase::TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField MetaId{
            .Name = "meta.id",
            .Type = NYT::NTableClient::EValueType::String,
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaEtc{
            .Name = "meta.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFinalizationStartTime{
            .Name = "meta.finalization_start_time",
            .Type = NYT::NTableClient::EValueType::Uint64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFinalizers{
            .Name = "meta.finalizers",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecEtc{
            .Name = "spec.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField StatusEtc{
            .Name = "status.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
    } Fields;
} SchemasTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TSemaphoresTable
    : public NYT::NOrm::NServer::NObjects::TDBTable
{
    TSemaphoresTable()
        : NYT::NOrm::NServer::NObjects::TDBTable("semaphores")
    {
        Initialize(/*keyFields*/ {
            &Fields.MetaSemaphoreSetId,
            &Fields.MetaId
        },
        /*otherFields*/ {
            &Fields.MetaEtc,
            &Fields.ExistenceLock,
            &Fields.MetaHistorySnapshotTimestamp,
            &Fields.MetaRemovalTime,
            &Fields.MetaAcl,
            &Fields.MetaCreationTime,
            &Fields.MetaInheritAcl,
            &Fields.MetaFinalizationStartTime,
            &Fields.MetaFinalizers,
            &Fields.Spec,
            &Fields.StatusEtc,
            &Fields.Labels
        });
    }

    struct TFields
        : public NYT::NOrm::NServer::NObjects::TObjectTableBase::TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField MetaSemaphoreSetId{
            .Name = "meta.semaphore_set_id",
            .Type = NYT::NTableClient::EValueType::String,
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaId{
            .Name = "meta.id",
            .Type = NYT::NTableClient::EValueType::String,
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaEtc{
            .Name = "meta.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFinalizationStartTime{
            .Name = "meta.finalization_start_time",
            .Type = NYT::NTableClient::EValueType::Uint64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFinalizers{
            .Name = "meta.finalizers",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField Spec{
            .Name = "spec",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField StatusEtc{
            .Name = "status.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
    } Fields;
} SemaphoresTable;

extern const struct TSemaphoresToSemaphoreSetsTable
    : public NYT::NOrm::NServer::NObjects::TDBTable
{
    TSemaphoresToSemaphoreSetsTable()
        : NYT::NOrm::NServer::NObjects::TDBTable("semaphores_to_semaphore_sets")
    {
        Initialize(/*keyFields*/ {
            &Fields.MetaId
        },
        /*otherFields*/ {
            &Fields.MetaSemaphoreSetId
        });
    }

    struct TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField MetaId{
            .Name = "meta.id",
            .Type = NYT::NTableClient::EValueType::String,
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaSemaphoreSetId{
            .Name = "meta.semaphore_set_id",
            .Type = NYT::NTableClient::EValueType::String,
        };
    } Fields;
} SemaphoresToSemaphoreSetsTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TSemaphoreSetsTable
    : public NYT::NOrm::NServer::NObjects::TDBTable
{
    TSemaphoreSetsTable()
        : NYT::NOrm::NServer::NObjects::TDBTable("semaphore_sets")
    {
        Initialize(/*keyFields*/ {
            &Fields.MetaId
        },
        /*otherFields*/ {
            &Fields.MetaEtc,
            &Fields.ExistenceLock,
            &Fields.MetaHistorySnapshotTimestamp,
            &Fields.MetaRemovalTime,
            &Fields.MetaAcl,
            &Fields.MetaCreationTime,
            &Fields.MetaInheritAcl,
            &Fields.MetaFinalizationStartTime,
            &Fields.MetaFinalizers,
            &Fields.Spec,
            &Fields.Status,
            &Fields.Labels
        });
    }

    struct TFields
        : public NYT::NOrm::NServer::NObjects::TObjectTableBase::TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField MetaId{
            .Name = "meta.id",
            .Type = NYT::NTableClient::EValueType::String,
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaEtc{
            .Name = "meta.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFinalizationStartTime{
            .Name = "meta.finalization_start_time",
            .Type = NYT::NTableClient::EValueType::Uint64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFinalizers{
            .Name = "meta.finalizers",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField Spec{
            .Name = "spec",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField Status{
            .Name = "status",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
    } Fields;
} SemaphoreSetsTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TTimestampIdsTable
    : public NYT::NOrm::NServer::NObjects::TDBTable
{
    TTimestampIdsTable()
        : NYT::NOrm::NServer::NObjects::TDBTable("timestamp_ids")
    {
        Initialize(/*keyFields*/ {
            &Fields.MetaI64Id,
            &Fields.MetaUi64Id
        },
        /*otherFields*/ {
            &Fields.MetaEtc,
            &Fields.ExistenceLock,
            &Fields.MetaHistorySnapshotTimestamp,
            &Fields.MetaRemovalTime,
            &Fields.MetaAcl,
            &Fields.MetaCreationTime,
            &Fields.MetaInheritAcl,
            &Fields.MetaFinalizationStartTime,
            &Fields.MetaFinalizers,
            &Fields.SpecUi64Value,
            &Fields.SpecEtc,
            &Fields.StatusEtc,
            &Fields.Labels
        });
    }

    struct TFields
        : public NYT::NOrm::NServer::NObjects::TObjectTableBase::TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField MetaI64Id{
            .Name = "meta.i64_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaUi64Id{
            .Name = "meta.ui64_id",
            .Type = NYT::NTableClient::EValueType::Uint64,
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaEtc{
            .Name = "meta.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFinalizationStartTime{
            .Name = "meta.finalization_start_time",
            .Type = NYT::NTableClient::EValueType::Uint64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFinalizers{
            .Name = "meta.finalizers",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecUi64Value{
            .Name = "spec.ui64_value",
            .Type = NYT::NTableClient::EValueType::Uint64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecEtc{
            .Name = "spec.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField StatusEtc{
            .Name = "status.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
    } Fields;
} TimestampIdsTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TTypographersTable
    : public NYT::NOrm::NServer::NObjects::TDBTable
{
    TTypographersTable()
        : NYT::NOrm::NServer::NObjects::TDBTable("typographers")
    {
        Initialize(/*keyFields*/ {
            &Fields.MetaPublisherId,
            &Fields.MetaId
        },
        /*otherFields*/ {
            &Fields.MetaEtc,
            &Fields.ExistenceLock,
            &Fields.MetaHistorySnapshotTimestamp,
            &Fields.MetaLogin,
            &Fields.MetaRemovalTime,
            &Fields.MetaAcl,
            &Fields.MetaCreationTime,
            &Fields.MetaInheritAcl,
            &Fields.MetaFinalizationStartTime,
            &Fields.MetaFinalizers,
            &Fields.SpecTestMandatoryColumnField,
            &Fields.SpecEtc,
            &Fields.StatusEtc,
            &Fields.Labels
        });
    }

    struct TFields
        : public NYT::NOrm::NServer::NObjects::TObjectTableBase::TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField MetaPublisherId{
            .Name = "meta.publisher_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaId{
            .Name = "meta.id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaEtc{
            .Name = "meta.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaLogin{
            .Name = "meta.login",
            .Type = NYT::NTableClient::EValueType::String,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFinalizationStartTime{
            .Name = "meta.finalization_start_time",
            .Type = NYT::NTableClient::EValueType::Uint64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFinalizers{
            .Name = "meta.finalizers",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecTestMandatoryColumnField{
            .Name = "spec.test_mandatory_column_field",
            .Type = NYT::NTableClient::EValueType::String,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField SpecEtc{
            .Name = "spec.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField StatusEtc{
            .Name = "status.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
    } Fields;
} TypographersTable;

extern const struct TTypographersToPublishersTable
    : public NYT::NOrm::NServer::NObjects::TDBTable
{
    TTypographersToPublishersTable()
        : NYT::NOrm::NServer::NObjects::TDBTable("typographers_to_publishers")
    {
        Initialize(/*keyFields*/ {
            &Fields.MetaId
        },
        /*otherFields*/ {
            &Fields.MetaPublisherId
        });
    }

    struct TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField MetaId{
            .Name = "meta.id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaPublisherId{
            .Name = "meta.publisher_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
    } Fields;
} TypographersToPublishersTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TUsersTable
    : public NYT::NOrm::NServer::NObjects::TDBTable
{
    TUsersTable()
        : NYT::NOrm::NServer::NObjects::TDBTable("users")
    {
        Initialize(/*keyFields*/ {
            &Fields.MetaId
        },
        /*otherFields*/ {
            &Fields.MetaEtc,
            &Fields.ExistenceLock,
            &Fields.MetaHistorySnapshotTimestamp,
            &Fields.MetaRemovalTime,
            &Fields.MetaAcl,
            &Fields.MetaCreationTime,
            &Fields.MetaInheritAcl,
            &Fields.MetaFinalizationStartTime,
            &Fields.MetaFinalizers,
            &Fields.Spec,
            &Fields.StatusEtc,
            &Fields.Labels
        });
    }

    struct TFields
        : public NYT::NOrm::NServer::NObjects::TObjectTableBase::TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField MetaId{
            .Name = "meta.id",
            .Type = NYT::NTableClient::EValueType::String,
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaEtc{
            .Name = "meta.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFinalizationStartTime{
            .Name = "meta.finalization_start_time",
            .Type = NYT::NTableClient::EValueType::Uint64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFinalizers{
            .Name = "meta.finalizers",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField Spec{
            .Name = "spec",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField StatusEtc{
            .Name = "status.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
    } Fields;
} UsersTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TWatchLogConsumersTable
    : public NYT::NOrm::NServer::NObjects::TDBTable
{
    TWatchLogConsumersTable()
        : NYT::NOrm::NServer::NObjects::TDBTable("watch_log_consumers")
    {
        Initialize(/*keyFields*/ {
            &Fields.MetaId
        },
        /*otherFields*/ {
            &Fields.MetaEtc,
            &Fields.ExistenceLock,
            &Fields.MetaHistorySnapshotTimestamp,
            &Fields.MetaRemovalTime,
            &Fields.MetaAcl,
            &Fields.MetaCreationTime,
            &Fields.MetaInheritAcl,
            &Fields.MetaFinalizationStartTime,
            &Fields.MetaFinalizers,
            &Fields.Spec,
            &Fields.Status,
            &Fields.Labels
        });
    }

    struct TFields
        : public NYT::NOrm::NServer::NObjects::TObjectTableBase::TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField MetaId{
            .Name = "meta.id",
            .Type = NYT::NTableClient::EValueType::String,
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaEtc{
            .Name = "meta.etc",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFinalizationStartTime{
            .Name = "meta.finalization_start_time",
            .Type = NYT::NTableClient::EValueType::Uint64,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField MetaFinalizers{
            .Name = "meta.finalizers",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField Spec{
            .Name = "spec",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
        NYT::NOrm::NServer::NObjects::TDBField Status{
            .Name = "status",
            .Type = NYT::NTableClient::EValueType::Any,
            .LockGroup = "api",
        };
    } Fields;
} WatchLogConsumersTable;

////////////////////////////////////////////////////////////////////////////////


extern const struct TIllustratorToBooksTable
    : public NYT::NOrm::NServer::NObjects::TDBTable
{
    TIllustratorToBooksTable()
        : NYT::NOrm::NServer::NObjects::TDBTable("illustrator_to_books")
    {
        Initialize(/*keyFields*/ {
            &Fields.IllustratorUid,
            &Fields.BookId,
            &Fields.BookId2
        },
        /*otherFields*/ {
        });
    }

    struct TFields {
        NYT::NOrm::NServer::NObjects::TDBField IllustratorUid{
            .Name = "illustrator_uid",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField BookId{
            .Name = "book_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField BookId2{
            .Name = "book_id2",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
    } Fields;
} IllustratorToBooksTable;
extern const struct TPublishersToBooksTable
    : public NYT::NOrm::NServer::NObjects::TDBTable
{
    TPublishersToBooksTable()
        : NYT::NOrm::NServer::NObjects::TDBTable("publishers_to_books")
    {
        Initialize(/*keyFields*/ {
            &Fields.PublisherId,
            &Fields.BookId,
            &Fields.BookId2
        },
        /*otherFields*/ {
        });
    }

    struct TFields {
        NYT::NOrm::NServer::NObjects::TDBField PublisherId{
            .Name = "publisher_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField BookId{
            .Name = "book_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField BookId2{
            .Name = "book_id2",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
    } Fields;
} PublishersToBooksTable;
extern const struct TIllustratorToBooksForCoverIllustratorTable
    : public NYT::NOrm::NServer::NObjects::TDBTable
{
    TIllustratorToBooksForCoverIllustratorTable()
        : NYT::NOrm::NServer::NObjects::TDBTable("illustrator_to_books_for_cover_illustrator")
    {
        Initialize(/*keyFields*/ {
            &Fields.IllustratorUid,
            &Fields.BookId,
            &Fields.BookId2
        },
        /*otherFields*/ {
        });
    }

    struct TFields {
        NYT::NOrm::NServer::NObjects::TDBField IllustratorUid{
            .Name = "illustrator_uid",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField BookId{
            .Name = "book_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField BookId2{
            .Name = "book_id2",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
    } Fields;
} IllustratorToBooksForCoverIllustratorTable;
extern const struct TExecutorToMotherShipsTable
    : public NYT::NOrm::NServer::NObjects::TDBTable
{
    TExecutorToMotherShipsTable()
        : NYT::NOrm::NServer::NObjects::TDBTable("executor_to_mother_ships")
    {
        Initialize(/*keyFields*/ {
            &Fields.ExecutorId,
            &Fields.MotherShipId
        },
        /*otherFields*/ {
        });
    }

    struct TFields {
        NYT::NOrm::NServer::NObjects::TDBField ExecutorId{
            .Name = "executor_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField MotherShipId{
            .Name = "mother_ship_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
    } Fields;
} ExecutorToMotherShipsTable;
extern const struct TEditorToPublishersTable
    : public NYT::NOrm::NServer::NObjects::TDBTable
{
    TEditorToPublishersTable()
        : NYT::NOrm::NServer::NObjects::TDBTable("editor_to_publishers")
    {
        Initialize(/*keyFields*/ {
            &Fields.EditorId,
            &Fields.PublisherId
        },
        /*otherFields*/ {
        });
    }

    struct TFields {
        NYT::NOrm::NServer::NObjects::TDBField EditorId{
            .Name = "editor_id",
            .Type = NYT::NTableClient::EValueType::String,
        };
        NYT::NOrm::NServer::NObjects::TDBField PublisherId{
            .Name = "publisher_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
    } Fields;
} EditorToPublishersTable;
extern const struct TIllustratorToPublishersTable
    : public NYT::NOrm::NServer::NObjects::TDBTable
{
    TIllustratorToPublishersTable()
        : NYT::NOrm::NServer::NObjects::TDBTable("illustrator_to_publishers")
    {
        Initialize(/*keyFields*/ {
            &Fields.IllustratorUid,
            &Fields.PublisherId
        },
        /*otherFields*/ {
        });
    }

    struct TFields {
        NYT::NOrm::NServer::NObjects::TDBField IllustratorUid{
            .Name = "illustrator_uid",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField PublisherId{
            .Name = "publisher_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
    } Fields;
} IllustratorToPublishersTable;
extern const struct TPublisherToPublishersTable
    : public NYT::NOrm::NServer::NObjects::TDBTable
{
    TPublisherToPublishersTable()
        : NYT::NOrm::NServer::NObjects::TDBTable("publisher_to_publishers")
    {
        Initialize(/*keyFields*/ {
            &Fields.TargetPublisherId,
            &Fields.SourcePublisherId
        },
        /*otherFields*/ {
        });
    }

    struct TFields {
        NYT::NOrm::NServer::NObjects::TDBField TargetPublisherId{
            .Name = "target_publisher_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField SourcePublisherId{
            .Name = "source_publisher_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
    } Fields;
} PublisherToPublishersTable;
extern const struct TIllustratorsToPublishersForOldFeaturedIllustratorsTable
    : public NYT::NOrm::NServer::NObjects::TDBTable
{
    TIllustratorsToPublishersForOldFeaturedIllustratorsTable()
        : NYT::NOrm::NServer::NObjects::TDBTable("illustrators_to_publishers_for_old_featured_illustrators")
    {
        Initialize(/*keyFields*/ {
            &Fields.IllustratorUid,
            &Fields.PublisherId
        },
        /*otherFields*/ {
        });
    }

    struct TFields {
        NYT::NOrm::NServer::NObjects::TDBField IllustratorUid{
            .Name = "illustrator_uid",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField PublisherId{
            .Name = "publisher_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
    } Fields;
} IllustratorsToPublishersForOldFeaturedIllustratorsTable;
extern const struct TIllustratorsToPublishersForNewFeaturedIllustratorsTable
    : public NYT::NOrm::NServer::NObjects::TDBTable
{
    TIllustratorsToPublishersForNewFeaturedIllustratorsTable()
        : NYT::NOrm::NServer::NObjects::TDBTable("illustrators_to_publishers_for_new_featured_illustrators")
    {
        Initialize(/*keyFields*/ {
            &Fields.IllustratorUid,
            &Fields.PublisherId
        },
        /*otherFields*/ {
        });
    }

    struct TFields {
        NYT::NOrm::NServer::NObjects::TDBField IllustratorUid{
            .Name = "illustrator_uid",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField PublisherId{
            .Name = "publisher_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
    } Fields;
} IllustratorsToPublishersForNewFeaturedIllustratorsTable;

////////////////////////////////////////////////////////////////////////////////
extern const struct TEditorToBooksTable
    : public NYT::NOrm::NServer::NObjects::TDBIndexTable
{
    TEditorToBooksTable()
        : NYT::NOrm::NServer::NObjects::TDBIndexTable("editor_to_books")
    {
        IndexKey = {
            &Fields.EditorId
        };
        ObjectTableKey = {
            &Fields.PublisherId,
            &Fields.BookId,
            &Fields.BookId2
        };
        Initialize(/*keyFields*/ {
            &Fields.Hash,
            &Fields.EditorId,
            &Fields.PublisherId,
            &Fields.BookId,
            &Fields.BookId2
        },
        /*otherFields*/ {
        });
    }

    struct TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField Hash{
            .Name = "hash",
            .Type = NYT::NTableClient::EValueType::Uint64,
            .Evaluated = true,
        };
        NYT::NOrm::NServer::NObjects::TDBField EditorId{
            .Name = "editor_id",
            .Type = NYT::NTableClient::EValueType::String,
        };
        NYT::NOrm::NServer::NObjects::TDBField PublisherId{
            .Name = "publisher_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField BookId{
            .Name = "book_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField BookId2{
            .Name = "book_id2",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
    } Fields;
} EditorToBooksTable;

extern const struct TAuthorsToBooksForPeerReviewersTable
    : public NYT::NOrm::NServer::NObjects::TDBIndexTable
{
    TAuthorsToBooksForPeerReviewersTable()
        : NYT::NOrm::NServer::NObjects::TDBIndexTable("authors_to_books_for_peer_reviewers")
    {
        IndexKey = {
            &Fields.AuthorId
        };
        ObjectTableKey = {
            &Fields.PublisherId,
            &Fields.BookId,
            &Fields.BookId2
        };
        Initialize(/*keyFields*/ {
            &Fields.AuthorId,
            &Fields.PublisherId,
            &Fields.BookId,
            &Fields.BookId2
        },
        /*otherFields*/ {
        });
    }

    struct TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField AuthorId{
            .Name = "author_id",
            .Type = NYT::NTableClient::EValueType::String,
        };
        NYT::NOrm::NServer::NObjects::TDBField PublisherId{
            .Name = "publisher_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField BookId{
            .Name = "book_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField BookId2{
            .Name = "book_id2",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
    } Fields;
} AuthorsToBooksForPeerReviewersTable;

extern const struct TPublisherToIllustratorsTable
    : public NYT::NOrm::NServer::NObjects::TDBIndexTable
{
    TPublisherToIllustratorsTable()
        : NYT::NOrm::NServer::NObjects::TDBIndexTable("publisher_to_illustrators")
    {
        IndexKey = {
            &Fields.TargetPublisherId
        };
        ObjectTableKey = {
            &Fields.SourcePublisherId,
            &Fields.SourceIllustratorUid
        };
        Initialize(/*keyFields*/ {
            &Fields.TargetPublisherId,
            &Fields.SourcePublisherId,
            &Fields.SourceIllustratorUid
        },
        /*otherFields*/ {
        });
    }

    struct TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField TargetPublisherId{
            .Name = "target_publisher_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField SourcePublisherId{
            .Name = "source_publisher_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField SourceIllustratorUid{
            .Name = "source_illustrator_uid",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
    } Fields;
} PublisherToIllustratorsTable;

extern const struct TAuthorsToBooksTable
    : public NYT::NOrm::NServer::NObjects::TDBIndexTable
{
    TAuthorsToBooksTable()
        : NYT::NOrm::NServer::NObjects::TDBIndexTable("authors_to_books")
    {
        IndexKey = {
            &Fields.AuthorId
        };
        ObjectTableKey = {
            &Fields.PublisherId,
            &Fields.BookId,
            &Fields.BookId2
        };
        Initialize(/*keyFields*/ {
            &Fields.AuthorId,
            &Fields.PublisherId,
            &Fields.BookId,
            &Fields.BookId2
        },
        /*otherFields*/ {
        });
    }

    struct TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField AuthorId{
            .Name = "author_id",
            .Type = NYT::NTableClient::EValueType::String,
        };
        NYT::NOrm::NServer::NObjects::TDBField PublisherId{
            .Name = "publisher_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField BookId{
            .Name = "book_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField BookId2{
            .Name = "book_id2",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
    } Fields;
} AuthorsToBooksTable;


////////////////////////////////////////////////////////////////////////////////

extern const struct TBooksByCoverDpiTable
    : public NYT::NOrm::NServer::NObjects::TDBIndexTable
{
    TBooksByCoverDpiTable()
        : NYT::NOrm::NServer::NObjects::TDBIndexTable("books_by_cover_dpi")
    {
        IndexKey = {
            &Fields.DesignCoverDpi
        };
        ObjectTableKey = {
            &Fields.PublisherId,
            &Fields.BookId,
            &Fields.BookId2
        };
        Initialize(/*keyFields*/ {
            &Fields.DesignCoverDpi,
            &Fields.PublisherId,
            &Fields.BookId,
            &Fields.BookId2
        },
        /*otherFields*/ {
        });
    }

    struct TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField DesignCoverDpi{
            .Name = "design_cover_dpi",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField PublisherId{
            .Name = "publisher_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField BookId{
            .Name = "book_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField BookId2{
            .Name = "book_id2",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
    } Fields;
} BooksByCoverDpiTable;

extern const struct TBooksByCoverImageTable
    : public NYT::NOrm::NServer::NObjects::TDBIndexTable
{
    TBooksByCoverImageTable()
        : NYT::NOrm::NServer::NObjects::TDBIndexTable("books_by_cover_image")
    {
        IndexKey = {
            &Fields.DesignCoverImage
        };
        ObjectTableKey = {
            &Fields.PublisherId,
            &Fields.BookId,
            &Fields.BookId2
        };
        Initialize(/*keyFields*/ {
            &Fields.DesignCoverImage,
            &Fields.PublisherId,
            &Fields.BookId,
            &Fields.BookId2
        },
        /*otherFields*/ {
        });
    }

    struct TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField DesignCoverImage{
            .Name = "design_cover_image",
            .Type = NYT::NTableClient::EValueType::String,
        };
        NYT::NOrm::NServer::NObjects::TDBField PublisherId{
            .Name = "publisher_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField BookId{
            .Name = "book_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField BookId2{
            .Name = "book_id2",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
    } Fields;
} BooksByCoverImageTable;

extern const struct TBooksByCoverSizeTable
    : public NYT::NOrm::NServer::NObjects::TDBIndexTable
{
    TBooksByCoverSizeTable()
        : NYT::NOrm::NServer::NObjects::TDBIndexTable("books_by_cover_size")
    {
        IndexKey = {
            &Fields.DesignCoverSize
        };
        ObjectTableKey = {
            &Fields.PublisherId,
            &Fields.BookId,
            &Fields.BookId2
        };
        Initialize(/*keyFields*/ {
            &Fields.DesignCoverSize,
            &Fields.PublisherId,
            &Fields.BookId,
            &Fields.BookId2
        },
        /*otherFields*/ {
        });
    }

    struct TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField DesignCoverSize{
            .Name = "design_cover_size",
            .Type = NYT::NTableClient::EValueType::Uint64,
        };
        NYT::NOrm::NServer::NObjects::TDBField PublisherId{
            .Name = "publisher_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField BookId{
            .Name = "book_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField BookId2{
            .Name = "book_id2",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
    } Fields;
} BooksByCoverSizeTable;

extern const struct TBooksByCreationTimeTable
    : public NYT::NOrm::NServer::NObjects::TDBIndexTable
{
    TBooksByCreationTimeTable()
        : NYT::NOrm::NServer::NObjects::TDBIndexTable("books_by_creation_time")
    {
        IndexKey = {
            &Fields.CreationTime
        };
        ObjectTableKey = {
            &Fields.PublisherId,
            &Fields.BookId,
            &Fields.BookId2
        };
        Initialize(/*keyFields*/ {
            &Fields.CreationTime,
            &Fields.PublisherId,
            &Fields.BookId,
            &Fields.BookId2
        },
        /*otherFields*/ {
        });
    }

    struct TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField CreationTime{
            .Name = "creation_time",
            .Type = NYT::NTableClient::EValueType::Uint64,
        };
        NYT::NOrm::NServer::NObjects::TDBField PublisherId{
            .Name = "publisher_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField BookId{
            .Name = "book_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField BookId2{
            .Name = "book_id2",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
    } Fields;
} BooksByCreationTimeTable;

extern const struct TBooksByEditorAndYearWithPredicateTable
    : public NYT::NOrm::NServer::NObjects::TDBIndexTable
{
    TBooksByEditorAndYearWithPredicateTable()
        : NYT::NOrm::NServer::NObjects::TDBIndexTable("books_by_editor_and_year_with_predicate")
    {
        IndexKey = {
            &Fields.EditorId,
            &Fields.Year
        };
        ObjectTableKey = {
            &Fields.PublisherId,
            &Fields.BookId,
            &Fields.BookId2
        };
        Initialize(/*keyFields*/ {
            &Fields.EditorId,
            &Fields.Year,
            &Fields.PublisherId,
            &Fields.BookId,
            &Fields.BookId2
        },
        /*otherFields*/ {
        });
    }

    struct TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField EditorId{
            .Name = "editor_id",
            .Type = NYT::NTableClient::EValueType::String,
        };
        NYT::NOrm::NServer::NObjects::TDBField Year{
            .Name = "year",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField PublisherId{
            .Name = "publisher_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField BookId{
            .Name = "book_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField BookId2{
            .Name = "book_id2",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
    } Fields;
} BooksByEditorAndYearWithPredicateTable;

extern const struct TBooksByFontTable
    : public NYT::NOrm::NServer::NObjects::TDBIndexTable
{
    TBooksByFontTable()
        : NYT::NOrm::NServer::NObjects::TDBIndexTable("books_by_font")
    {
        IndexKey = {
            &Fields.Font
        };
        ObjectTableKey = {
            &Fields.PublisherId,
            &Fields.BookId,
            &Fields.BookId2
        };
        Initialize(/*keyFields*/ {
            &Fields.Font,
            &Fields.PublisherId,
            &Fields.BookId,
            &Fields.BookId2
        },
        /*otherFields*/ {
        });
    }

    struct TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField Font{
            .Name = "font",
            .Type = NYT::NTableClient::EValueType::String,
        };
        NYT::NOrm::NServer::NObjects::TDBField PublisherId{
            .Name = "publisher_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField BookId{
            .Name = "book_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField BookId2{
            .Name = "book_id2",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
    } Fields;
} BooksByFontTable;

extern const struct TBooksByGenresTable
    : public NYT::NOrm::NServer::NObjects::TDBIndexTable
{
    TBooksByGenresTable()
        : NYT::NOrm::NServer::NObjects::TDBIndexTable("books_by_genres")
    {
        IndexKey = {
            &Fields.Genres
        };
        ObjectTableKey = {
            &Fields.PublisherId,
            &Fields.BookId,
            &Fields.BookId2
        };
        Initialize(/*keyFields*/ {
            &Fields.Genres,
            &Fields.PublisherId,
            &Fields.BookId,
            &Fields.BookId2
        },
        /*otherFields*/ {
        });
    }

    struct TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField Genres{
            .Name = "genres",
            .Type = NYT::NTableClient::EValueType::String,
        };
        NYT::NOrm::NServer::NObjects::TDBField PublisherId{
            .Name = "publisher_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField BookId{
            .Name = "book_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField BookId2{
            .Name = "book_id2",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
    } Fields;
} BooksByGenresTable;

extern const struct TBooksByIllustrationsTable
    : public NYT::NOrm::NServer::NObjects::TDBIndexTable
{
    TBooksByIllustrationsTable()
        : NYT::NOrm::NServer::NObjects::TDBIndexTable("books_by_illustrations")
    {
        IndexKey = {
            &Fields.DesignIllustrations
        };
        ObjectTableKey = {
            &Fields.PublisherId,
            &Fields.BookId,
            &Fields.BookId2
        };
        Initialize(/*keyFields*/ {
            &Fields.DesignIllustrations,
            &Fields.PublisherId,
            &Fields.BookId,
            &Fields.BookId2
        },
        /*otherFields*/ {
        });
    }

    struct TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField DesignIllustrations{
            .Name = "design_illustrations",
            .Type = NYT::NTableClient::EValueType::String,
        };
        NYT::NOrm::NServer::NObjects::TDBField PublisherId{
            .Name = "publisher_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField BookId{
            .Name = "book_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField BookId2{
            .Name = "book_id2",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
    } Fields;
} BooksByIllustrationsTable;

extern const struct TBooksByIllustrationsWithPredicateTable
    : public NYT::NOrm::NServer::NObjects::TDBIndexTable
{
    TBooksByIllustrationsWithPredicateTable()
        : NYT::NOrm::NServer::NObjects::TDBIndexTable("books_by_illustrations_with_predicate")
    {
        IndexKey = {
            &Fields.DesignIllustrations
        };
        ObjectTableKey = {
            &Fields.PublisherId,
            &Fields.BookId,
            &Fields.BookId2
        };
        Initialize(/*keyFields*/ {
            &Fields.DesignIllustrations,
            &Fields.PublisherId,
            &Fields.BookId,
            &Fields.BookId2
        },
        /*otherFields*/ {
        });
    }

    struct TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField DesignIllustrations{
            .Name = "design_illustrations",
            .Type = NYT::NTableClient::EValueType::String,
        };
        NYT::NOrm::NServer::NObjects::TDBField PublisherId{
            .Name = "publisher_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField BookId{
            .Name = "book_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField BookId2{
            .Name = "book_id2",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
    } Fields;
} BooksByIllustrationsWithPredicateTable;

extern const struct TBooksByIsbnTable
    : public NYT::NOrm::NServer::NObjects::TDBIndexTable
{
    TBooksByIsbnTable()
        : NYT::NOrm::NServer::NObjects::TDBIndexTable("books_by_isbn")
    {
        IndexKey = {
            &Fields.Isbn
        };
        ObjectTableKey = {
            &Fields.PublisherId,
            &Fields.BookId,
            &Fields.BookId2
        };
        Initialize(/*keyFields*/ {
            &Fields.Hash,
            &Fields.Isbn,
            &Fields.PublisherId,
            &Fields.BookId,
            &Fields.BookId2
        },
        /*otherFields*/ {
        });
    }

    struct TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField Hash{
            .Name = "hash",
            .Type = NYT::NTableClient::EValueType::Uint64,
            .Evaluated = true,
        };
        NYT::NOrm::NServer::NObjects::TDBField Isbn{
            .Name = "isbn",
            .Type = NYT::NTableClient::EValueType::String,
        };
        NYT::NOrm::NServer::NObjects::TDBField PublisherId{
            .Name = "publisher_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField BookId{
            .Name = "book_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField BookId2{
            .Name = "book_id2",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
    } Fields;
} BooksByIsbnTable;

extern const struct TBooksByIsbnWithPredicateTable
    : public NYT::NOrm::NServer::NObjects::TDBIndexTable
{
    TBooksByIsbnWithPredicateTable()
        : NYT::NOrm::NServer::NObjects::TDBIndexTable("books_by_isbn_with_predicate")
    {
        IndexKey = {
            &Fields.Isbn
        };
        ObjectTableKey = {
            &Fields.PublisherId,
            &Fields.BookId,
            &Fields.BookId2
        };
        Initialize(/*keyFields*/ {
            &Fields.Isbn,
            &Fields.PublisherId,
            &Fields.BookId,
            &Fields.BookId2
        },
        /*otherFields*/ {
        });
    }

    struct TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField Isbn{
            .Name = "isbn",
            .Type = NYT::NTableClient::EValueType::String,
        };
        NYT::NOrm::NServer::NObjects::TDBField PublisherId{
            .Name = "publisher_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField BookId{
            .Name = "book_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField BookId2{
            .Name = "book_id2",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
    } Fields;
} BooksByIsbnWithPredicateTable;

extern const struct TBooksByKeywordsTable
    : public NYT::NOrm::NServer::NObjects::TDBIndexTable
{
    TBooksByKeywordsTable()
        : NYT::NOrm::NServer::NObjects::TDBIndexTable("books_by_keywords")
    {
        IndexKey = {
            &Fields.Keywords
        };
        ObjectTableKey = {
            &Fields.PublisherId,
            &Fields.BookId,
            &Fields.BookId2
        };
        Initialize(/*keyFields*/ {
            &Fields.Hash,
            &Fields.Keywords,
            &Fields.PublisherId,
            &Fields.BookId,
            &Fields.BookId2
        },
        /*otherFields*/ {
        });
    }

    struct TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField Hash{
            .Name = "hash",
            .Type = NYT::NTableClient::EValueType::Uint64,
            .Evaluated = true,
        };
        NYT::NOrm::NServer::NObjects::TDBField Keywords{
            .Name = "keywords",
            .Type = NYT::NTableClient::EValueType::String,
        };
        NYT::NOrm::NServer::NObjects::TDBField PublisherId{
            .Name = "publisher_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField BookId{
            .Name = "book_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField BookId2{
            .Name = "book_id2",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
    } Fields;
} BooksByKeywordsTable;

extern const struct TBooksByNameTable
    : public NYT::NOrm::NServer::NObjects::TDBIndexTable
{
    TBooksByNameTable()
        : NYT::NOrm::NServer::NObjects::TDBIndexTable("books_by_name")
    {
        IndexKey = {
            &Fields.Name
        };
        ObjectTableKey = {
            &Fields.PublisherId,
            &Fields.BookId,
            &Fields.BookId2
        };
        Initialize(/*keyFields*/ {
            &Fields.Name,
            &Fields.PublisherId,
            &Fields.BookId,
            &Fields.BookId2
        },
        /*otherFields*/ {
        });
    }

    struct TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField Name{
            .Name = "name",
            .Type = NYT::NTableClient::EValueType::String,
        };
        NYT::NOrm::NServer::NObjects::TDBField PublisherId{
            .Name = "publisher_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField BookId{
            .Name = "book_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField BookId2{
            .Name = "book_id2",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
    } Fields;
} BooksByNameTable;

extern const struct TBooksByPageCountTable
    : public NYT::NOrm::NServer::NObjects::TDBIndexTable
{
    TBooksByPageCountTable()
        : NYT::NOrm::NServer::NObjects::TDBIndexTable("books_by_page_count")
    {
        IndexKey = {
            &Fields.PageCount
        };
        ObjectTableKey = {
            &Fields.PublisherId,
            &Fields.BookId,
            &Fields.BookId2
        };
        Initialize(/*keyFields*/ {
            &Fields.PageCount,
            &Fields.PublisherId,
            &Fields.BookId,
            &Fields.BookId2
        },
        /*otherFields*/ {
        });
    }

    struct TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField PageCount{
            .Name = "page_count",
            .Type = NYT::NTableClient::EValueType::Uint64,
        };
        NYT::NOrm::NServer::NObjects::TDBField PublisherId{
            .Name = "publisher_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField BookId{
            .Name = "book_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField BookId2{
            .Name = "book_id2",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
    } Fields;
} BooksByPageCountTable;

extern const struct TBooksByStoreRatingTable
    : public NYT::NOrm::NServer::NObjects::TDBIndexTable
{
    TBooksByStoreRatingTable()
        : NYT::NOrm::NServer::NObjects::TDBIndexTable("books_by_store_rating")
    {
        IndexKey = {
            &Fields.StoreRating
        };
        ObjectTableKey = {
            &Fields.PublisherId,
            &Fields.BookId,
            &Fields.BookId2
        };
        Initialize(/*keyFields*/ {
            &Fields.StoreRating,
            &Fields.PublisherId,
            &Fields.BookId,
            &Fields.BookId2
        },
        /*otherFields*/ {
        });
    }

    struct TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField StoreRating{
            .Name = "store_rating",
            .Type = NYT::NTableClient::EValueType::Double,
        };
        NYT::NOrm::NServer::NObjects::TDBField PublisherId{
            .Name = "publisher_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField BookId{
            .Name = "book_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField BookId2{
            .Name = "book_id2",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
    } Fields;
} BooksByStoreRatingTable;

extern const struct TBooksByYearTable
    : public NYT::NOrm::NServer::NObjects::TDBIndexTable
{
    TBooksByYearTable()
        : NYT::NOrm::NServer::NObjects::TDBIndexTable("books_by_year")
    {
        IndexKey = {
            &Fields.Year
        };
        ObjectTableKey = {
            &Fields.PublisherId,
            &Fields.BookId,
            &Fields.BookId2
        };
        Initialize(/*keyFields*/ {
            &Fields.Hash,
            &Fields.Year,
            &Fields.PublisherId,
            &Fields.BookId,
            &Fields.BookId2
        },
        /*otherFields*/ {
        });
    }

    struct TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField Hash{
            .Name = "hash",
            .Type = NYT::NTableClient::EValueType::Uint64,
            .Evaluated = true,
        };
        NYT::NOrm::NServer::NObjects::TDBField Year{
            .Name = "year",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField PublisherId{
            .Name = "publisher_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField BookId{
            .Name = "book_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField BookId2{
            .Name = "book_id2",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
    } Fields;
} BooksByYearTable;

extern const struct TBooksByYearAndFontTable
    : public NYT::NOrm::NServer::NObjects::TDBIndexTable
{
    TBooksByYearAndFontTable()
        : NYT::NOrm::NServer::NObjects::TDBIndexTable("books_by_year_and_font")
    {
        IndexKey = {
            &Fields.Year,
            &Fields.Font
        };
        ObjectTableKey = {
            &Fields.PublisherId,
            &Fields.BookId,
            &Fields.BookId2
        };
        Initialize(/*keyFields*/ {
            &Fields.Year,
            &Fields.Font,
            &Fields.PublisherId,
            &Fields.BookId,
            &Fields.BookId2
        },
        /*otherFields*/ {
        });
    }

    struct TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField Year{
            .Name = "year",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField Font{
            .Name = "font",
            .Type = NYT::NTableClient::EValueType::String,
        };
        NYT::NOrm::NServer::NObjects::TDBField PublisherId{
            .Name = "publisher_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField BookId{
            .Name = "book_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField BookId2{
            .Name = "book_id2",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
    } Fields;
} BooksByYearAndFontTable;

extern const struct TBooksByYearAndPageCountWithPredicateTable
    : public NYT::NOrm::NServer::NObjects::TDBIndexTable
{
    TBooksByYearAndPageCountWithPredicateTable()
        : NYT::NOrm::NServer::NObjects::TDBIndexTable("books_by_year_and_page_count_with_predicate")
    {
        IndexKey = {
            &Fields.Year,
            &Fields.PageCount
        };
        ObjectTableKey = {
            &Fields.PublisherId,
            &Fields.BookId,
            &Fields.BookId2
        };
        Initialize(/*keyFields*/ {
            &Fields.Year,
            &Fields.PageCount,
            &Fields.PublisherId,
            &Fields.BookId,
            &Fields.BookId2
        },
        /*otherFields*/ {
        });
    }

    struct TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField Year{
            .Name = "year",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField PageCount{
            .Name = "page_count",
            .Type = NYT::NTableClient::EValueType::Uint64,
        };
        NYT::NOrm::NServer::NObjects::TDBField PublisherId{
            .Name = "publisher_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField BookId{
            .Name = "book_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField BookId2{
            .Name = "book_id2",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
    } Fields;
} BooksByYearAndPageCountWithPredicateTable;

extern const struct TBooksByYearWithPredicateTable
    : public NYT::NOrm::NServer::NObjects::TDBIndexTable
{
    TBooksByYearWithPredicateTable()
        : NYT::NOrm::NServer::NObjects::TDBIndexTable("books_by_cover_size_with_predicate")
    {
        IndexKey = {
            &Fields.Year
        };
        ObjectTableKey = {
            &Fields.PublisherId,
            &Fields.BookId,
            &Fields.BookId2
        };
        Initialize(/*keyFields*/ {
            &Fields.Year,
            &Fields.PublisherId,
            &Fields.BookId,
            &Fields.BookId2
        },
        /*otherFields*/ {
        });
    }

    struct TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField Year{
            .Name = "year",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField PublisherId{
            .Name = "publisher_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField BookId{
            .Name = "book_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField BookId2{
            .Name = "book_id2",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
    } Fields;
} BooksByYearWithPredicateTable;

extern const struct TCatsByNamesWithPredicateTable
    : public NYT::NOrm::NServer::NObjects::TDBIndexTable
{
    TCatsByNamesWithPredicateTable()
        : NYT::NOrm::NServer::NObjects::TDBIndexTable("cats_by_names_with_predicate")
    {
        IndexKey = {
            &Fields.Name
        };
        ObjectTableKey = {
            &Fields.CatId
        };
        Initialize(/*keyFields*/ {
            &Fields.Name,
            &Fields.CatId
        },
        /*otherFields*/ {
        });
    }

    struct TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField Name{
            .Name = "name",
            .Type = NYT::NTableClient::EValueType::String,
        };
        NYT::NOrm::NServer::NObjects::TDBField CatId{
            .Name = "cat_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
    } Fields;
} CatsByNamesWithPredicateTable;

extern const struct TEditorsByAchievementsTable
    : public NYT::NOrm::NServer::NObjects::TDBIndexTable
{
    TEditorsByAchievementsTable()
        : NYT::NOrm::NServer::NObjects::TDBIndexTable("editors_by_achievements")
    {
        IndexKey = {
            &Fields.Achievements
        };
        ObjectTableKey = {
            &Fields.EditorId
        };
        Initialize(/*keyFields*/ {
            &Fields.Achievements,
            &Fields.EditorId
        },
        /*otherFields*/ {
        });
    }

    struct TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField Achievements{
            .Name = "achievements",
            .Type = NYT::NTableClient::EValueType::String,
        };
        NYT::NOrm::NServer::NObjects::TDBField EditorId{
            .Name = "editor_id",
            .Type = NYT::NTableClient::EValueType::String,
        };
    } Fields;
} EditorsByAchievementsTable;

extern const struct TEditorsByNameTable
    : public NYT::NOrm::NServer::NObjects::TDBIndexTable
{
    TEditorsByNameTable()
        : NYT::NOrm::NServer::NObjects::TDBIndexTable("editors_by_name")
    {
        IndexKey = {
            &Fields.Name
        };
        ObjectTableKey = {
            &Fields.EditorId
        };
        Initialize(/*keyFields*/ {
            &Fields.Name,
            &Fields.EditorId
        },
        /*otherFields*/ {
        });
    }

    struct TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField Name{
            .Name = "name",
            .Type = NYT::NTableClient::EValueType::String,
        };
        NYT::NOrm::NServer::NObjects::TDBField EditorId{
            .Name = "editor_id",
            .Type = NYT::NTableClient::EValueType::String,
        };
    } Fields;
} EditorsByNameTable;

extern const struct TEditorsByPhoneNumberTable
    : public NYT::NOrm::NServer::NObjects::TDBIndexTable
{
    TEditorsByPhoneNumberTable()
        : NYT::NOrm::NServer::NObjects::TDBIndexTable("editors_by_phone_number")
    {
        IndexKey = {
            &Fields.PhoneNumber
        };
        ObjectTableKey = {
            &Fields.EditorId
        };
        Initialize(/*keyFields*/ {
            &Fields.PhoneNumber,
            &Fields.EditorId
        },
        /*otherFields*/ {
        });
    }

    struct TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField PhoneNumber{
            .Name = "phone_number",
            .Type = NYT::NTableClient::EValueType::String,
        };
        NYT::NOrm::NServer::NObjects::TDBField EditorId{
            .Name = "editor_id",
            .Type = NYT::NTableClient::EValueType::String,
        };
    } Fields;
} EditorsByPhoneNumberTable;

extern const struct TEditorsByPostTable
    : public NYT::NOrm::NServer::NObjects::TDBIndexTable
{
    TEditorsByPostTable()
        : NYT::NOrm::NServer::NObjects::TDBIndexTable("editors_by_post")
    {
        IndexKey = {
            &Fields.Post
        };
        ObjectTableKey = {
            &Fields.EditorId
        };
        Initialize(/*keyFields*/ {
            &Fields.Post,
            &Fields.EditorId
        },
        /*otherFields*/ {
        });
    }

    struct TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField Post{
            .Name = "post",
            .Type = NYT::NTableClient::EValueType::String,
        };
        NYT::NOrm::NServer::NObjects::TDBField EditorId{
            .Name = "editor_id",
            .Type = NYT::NTableClient::EValueType::String,
        };
    } Fields;
} EditorsByPostTable;

extern const struct TGenresByNameTable
    : public NYT::NOrm::NServer::NObjects::TDBIndexTable
{
    TGenresByNameTable()
        : NYT::NOrm::NServer::NObjects::TDBIndexTable("genre_by_name")
    {
        IndexKey = {
            &Fields.Name
        };
        ObjectTableKey = {
            &Fields.GenreId
        };
        Initialize(/*keyFields*/ {
            &Fields.Name
        },
        /*otherFields*/ {
            &Fields.GenreId
        });
    }

    struct TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField Name{
            .Name = "name",
            .Type = NYT::NTableClient::EValueType::String,
        };
        NYT::NOrm::NServer::NObjects::TDBField GenreId{
            .Name = "genre_id",
            .Type = NYT::NTableClient::EValueType::String,
        };
    } Fields;
} GenresByNameTable;

extern const struct TNirvanaDMProcessInstancesByCreatedTable
    : public NYT::NOrm::NServer::NObjects::TDBIndexTable
{
    TNirvanaDMProcessInstancesByCreatedTable()
        : NYT::NOrm::NServer::NObjects::TDBIndexTable("nirvana_dm_process_instances_by_created")
    {
        IndexKey = {
            &Fields.Created
        };
        ObjectTableKey = {
            &Fields.NirvanaDMProcessInstanceId
        };
        Initialize(/*keyFields*/ {
            &Fields.Created,
            &Fields.NirvanaDMProcessInstanceId
        },
        /*otherFields*/ {
        });
    }

    struct TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField Created{
            .Name = "created",
            .Type = NYT::NTableClient::EValueType::String,
        };
        NYT::NOrm::NServer::NObjects::TDBField NirvanaDMProcessInstanceId{
            .Name = "nirvana_dm_process_instance_id",
            .Type = NYT::NTableClient::EValueType::String,
        };
    } Fields;
} NirvanaDMProcessInstancesByCreatedTable;

extern const struct TNirvanaDMProcessInstancesByDefinitionIdTable
    : public NYT::NOrm::NServer::NObjects::TDBIndexTable
{
    TNirvanaDMProcessInstancesByDefinitionIdTable()
        : NYT::NOrm::NServer::NObjects::TDBIndexTable("nirvana_dm_process_instances_by_definition_id")
    {
        IndexKey = {
            &Fields.DefinitionId
        };
        ObjectTableKey = {
            &Fields.NirvanaDMProcessInstanceId
        };
        Initialize(/*keyFields*/ {
            &Fields.DefinitionId,
            &Fields.NirvanaDMProcessInstanceId
        },
        /*otherFields*/ {
        });
    }

    struct TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField DefinitionId{
            .Name = "definition_id",
            .Type = NYT::NTableClient::EValueType::String,
        };
        NYT::NOrm::NServer::NObjects::TDBField NirvanaDMProcessInstanceId{
            .Name = "nirvana_dm_process_instance_id",
            .Type = NYT::NTableClient::EValueType::String,
        };
    } Fields;
} NirvanaDMProcessInstancesByDefinitionIdTable;

extern const struct TPublishersByColumnFieldTable
    : public NYT::NOrm::NServer::NObjects::TDBIndexTable
{
    TPublishersByColumnFieldTable()
        : NYT::NOrm::NServer::NObjects::TDBIndexTable("publishers_by_column_field")
    {
        IndexKey = {
            &Fields.ColumnField
        };
        ObjectTableKey = {
            &Fields.PublisherId
        };
        Initialize(/*keyFields*/ {
            &Fields.ColumnField,
            &Fields.PublisherId
        },
        /*otherFields*/ {
        });
    }

    struct TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField ColumnField{
            .Name = "column_field",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField PublisherId{
            .Name = "publisher_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
    } Fields;
} PublishersByColumnFieldTable;

extern const struct TPublishersByNumberOfAwardsTable
    : public NYT::NOrm::NServer::NObjects::TDBIndexTable
{
    TPublishersByNumberOfAwardsTable()
        : NYT::NOrm::NServer::NObjects::TDBIndexTable("publishers_by_number_of_awards")
    {
        IndexKey = {
            &Fields.NumberOfAwards
        };
        ObjectTableKey = {
            &Fields.PublisherId
        };
        Initialize(/*keyFields*/ {
            &Fields.NumberOfAwards,
            &Fields.PublisherId
        },
        /*otherFields*/ {
        });
    }

    struct TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField NumberOfAwards{
            .Name = "number_of_awards",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField PublisherId{
            .Name = "publisher_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
    } Fields;
} PublishersByNumberOfAwardsTable;

extern const struct TTypographersByLoginTable
    : public NYT::NOrm::NServer::NObjects::TDBIndexTable
{
    TTypographersByLoginTable()
        : NYT::NOrm::NServer::NObjects::TDBIndexTable("typographers_by_login")
    {
        IndexKey = {
            &Fields.Login
        };
        ObjectTableKey = {
            &Fields.PublisherId,
            &Fields.TypographerId
        };
        Initialize(/*keyFields*/ {
            &Fields.Login,
            &Fields.PublisherId,
            &Fields.TypographerId
        },
        /*otherFields*/ {
        });
    }

    struct TFields
    {
        NYT::NOrm::NServer::NObjects::TDBField Login{
            .Name = "login",
            .Type = NYT::NTableClient::EValueType::String,
        };
        NYT::NOrm::NServer::NObjects::TDBField PublisherId{
            .Name = "publisher_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
        NYT::NOrm::NServer::NObjects::TDBField TypographerId{
            .Name = "typographer_id",
            .Type = NYT::NTableClient::EValueType::Int64,
        };
    } Fields;
} TypographersByLoginTable;

////////////////////////////////////////////////////////////////////////////////

extern const std::vector<const NYT::NOrm::NServer::NObjects::TDBTable*> Tables;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NExample::NServer::NLibrary
