// AUTOMATICALLY GENERATED. DO NOT EDIT!

#include "objects.h"

#include <yt/yt/orm/server/objects/helpers.h>

namespace NYT::NOrm::NExample::NServer::NLibrary {

////////////////////////////////////////////////////////////////////////////////

using NYT::NOrm::NServer::NObjects::IObjectTypeHandler;
using NYT::NOrm::NServer::NObjects::ISession;

////////////////////////////////////////////////////////////////////////////////

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TAuthor, ui64>
TAuthor::FinalizationStartTimeDescriptor{
    &AuthorsTable.Fields.MetaFinalizationStartTime,
    [] (TAuthor* obj) { return &obj->FinalizationStartTime(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TAuthor, THashMap<TString, NYT::NOrm::NDataModel::TFinalizer>>
TAuthor::FinalizersDescriptor{
    &AuthorsTable.Fields.MetaFinalizers,
    [] (TAuthor* obj) { return &obj->Finalizers(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TAuthor, TAuthor::TMetaEtc>
TAuthor::MetaEtcDescriptor{
    &AuthorsTable.Fields.MetaEtc,
    [] (TAuthor* object) { return &object->MetaEtc(); }
};

////////////////////////////////////////////////////////////////////////////////

TAuthor::TSpec::TSpec(TAuthor* object)
    : Name_(object, &NameDescriptor)
    , Age_(object, &AgeDescriptor)
    , Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TAuthor, TString>
TAuthor::TSpec::NameDescriptor{
    &AuthorsTable.Fields.SpecName,
    [] (TAuthor* obj) { return &obj->Spec().Name(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TAuthor, ui64>
TAuthor::TSpec::AgeDescriptor{
    &AuthorsTable.Fields.SpecAge,
    [] (TAuthor* obj) { return &obj->Spec().Age(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TAuthor, TAuthor::TSpec::TEtc>
TAuthor::TSpec::EtcDescriptor{
    &AuthorsTable.Fields.SpecEtc,
    [] (TAuthor* obj) { return &obj->Spec().Etc(); }
};

TAuthor::TStatus::TStatus(TAuthor* object)
    : BooksForPeerReviewers_(object, &BooksForPeerReviewersDescriptor)
    , BookRefs_(
        object,
        BookRefsDescriptor,
        std::make_unique<NYT::NOrm::NServer::NObjects::TTabularMultiKeyStorageDriver>(object, BookRefsDescriptor.KeyStorageDescriptor))
    , Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TManyToManyTabularAttributeDescriptor<NYT::NOrm::NExample::NServer::NLibrary::TAuthor, NYT::NOrm::NExample::NServer::NLibrary::TBook> TAuthor::TStatus::BooksForPeerReviewersDescriptor {
    &AuthorsToBooksForPeerReviewersTable,
    { &AuthorsToBooksForPeerReviewersTable.Fields.AuthorId },
    { &AuthorsToBooksForPeerReviewersTable.Fields.BookId, &AuthorsToBooksForPeerReviewersTable.Fields.BookId2 },
    [] (NYT::NOrm::NExample::NServer::NLibrary::TAuthor* obj) { return &obj->Status().BooksForPeerReviewers(); },
    [] (NYT::NOrm::NExample::NServer::NLibrary::TBook* obj) { return &obj->Spec().PeerReview().ReviewerIds(); },
    /*foreignObjectTableKey*/ true,
    /*forbidNonEmptyRemoval*/ false
};

const NYT::NOrm::NServer::NObjects::TMultiReferenceDescriptor<TAuthor, TBook> TAuthor::TStatus::BookRefsDescriptor {
    .ForwardAttributeGetter = [] (TAuthor* obj) { return &obj->Status().BookRefs(); },
    .InverseAttributeGetter = [] (TBook* obj) { return &obj->Spec().AuthorRefs(); },
    .KeyStorageDescriptor = NYT::NOrm::NServer::NObjects::TTabularKeyStorageDescriptor{
        .Table = &AuthorsToBooksTable,
        .OwnerKeyFields = { &AuthorsToBooksTable.Fields.AuthorId },
        .ForeignKeyFields = { &AuthorsToBooksTable.Fields.PublisherId, &AuthorsToBooksTable.Fields.BookId, &AuthorsToBooksTable.Fields.BookId2 },
    },
    .Settings = NYT::NOrm::NServer::NObjects::TReferenceAttributeSettings{
        .StoreParentKey = true,
    },
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TAuthor, TAuthor::TStatus::TEtc>
TAuthor::TStatus::EtcDescriptor{
    &AuthorsTable.Fields.StatusEtc,
    [] (TAuthor* obj) { return &obj->Status().Etc(); }
};

////////////////////////////////////////////////////////////////////////////////

TAuthor::TAuthor(
    const TString& id,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TDataModelObject(typeHandler, session)
    , Id_(id)
    , FinalizationStartTime_(this, &FinalizationStartTimeDescriptor)
    , Finalizers_(this, &FinalizersDescriptor)
    , MetaEtc_(this, &MetaEtcDescriptor)
    , Spec_(this)
    , Status_(this)
{
}

////////////////////////////////////////////////////////////////////////////////

NYT::NOrm::NClient::NObjects::TObjectKey TAuthor::GetKey() const
{
    return NYT::NOrm::NServer::NObjects::TObjectKey(
        GetId());
}

void TAuthor::ScheduleUuidLoad() const
{
    MetaEtc().ScheduleLoad();
}

NYT::NOrm::NClient::NObjects::TObjectId TAuthor::GetUuid(
    std::source_location location) const
{
    return MetaEtc().Load(location).uuid();
}

TString TAuthor::GetName(
    std::source_location location) const
{
    return MetaEtc().Load(location).name();
}

////////////////////////////////////////////////////////////////////////////////

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TBook, TString>
TBook::IsbnDescriptor{
    &BooksTable.Fields.MetaIsbn,
    [] (TBook* obj) { return &obj->Isbn(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TBook, ui64>
TBook::FinalizationStartTimeDescriptor{
    &BooksTable.Fields.MetaFinalizationStartTime,
    [] (TBook* obj) { return &obj->FinalizationStartTime(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TBook, THashMap<TString, NYT::NOrm::NDataModel::TFinalizer>>
TBook::FinalizersDescriptor{
    &BooksTable.Fields.MetaFinalizers,
    [] (TBook* obj) { return &obj->Finalizers(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TBook, TBook::TMetaEtc>
TBook::MetaEtcDescriptor{
    &BooksTable.Fields.MetaEtc,
    [] (TBook* object) { return &object->MetaEtc(); }
};

////////////////////////////////////////////////////////////////////////////////

TBook::TSpec::TSpec(TBook* object)
    : PeerReview_(object)
    , Name_(object, &NameDescriptor)
    , Year_(object, &YearDescriptor)
    , Font_(object, &FontDescriptor)
    , Genres_(object, &GenresDescriptor)
    , Keywords_(object, &KeywordsDescriptor)
    , EditorId_(object, &EditorIdDescriptor)
    , DigitalData_(object, &DigitalDataDescriptor)
    , AuthorIds_(object, &AuthorIdsDescriptor)
    , IllustratorId_(object, &IllustratorIdDescriptor)
    , AlternativePublisherIds_(object, &AlternativePublisherIdsDescriptor)
    , ChapterDescriptions_(object, &ChapterDescriptionsDescriptor)
    , CoverIllustratorId_(object, &CoverIllustratorIdDescriptor)
    , ApiRevision_(object, &ApiRevisionDescriptor)
    , AuthorRefs_(
        object,
        AuthorRefsDescriptor,
        std::make_unique<NYT::NOrm::NServer::NObjects::TColumnarMultiKeyStorageDriver>(object, AuthorRefsDescriptor.KeyStorageDescriptor))
    , Editor_(object)
    , Illustrator_(object)
    , Publishers_(object)
    , CoverIllustrator_(object)
    , Authors_(object)
    , Etc_(object, &EtcDescriptor)
    , WarehouseEtc_(object, &WarehouseEtcDescriptor)
{ }

TBook::TSpec::TPeerReview::TPeerReview(TBook* object)
    : ReviewerIds_(object, &ReviewerIdsDescriptor)
    , Reviewers_(object)
    , Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TManyToManyInlineAttributeDescriptor<NYT::NOrm::NExample::NServer::NLibrary::TBook, NYT::NOrm::NExample::NServer::NLibrary::TAuthor> TBook::TSpec::TPeerReview::ReviewerIdsDescriptor {
    &BooksTable.Fields.SpecPeerReviewReviewerIds,
    [] (NYT::NOrm::NExample::NServer::NLibrary::TBook* obj) { return &obj->Spec().PeerReview().ReviewerIds(); },
    [] (NYT::NOrm::NExample::NServer::NLibrary::TAuthor* obj) { return &obj->Status().BooksForPeerReviewers(); },
    /*forbidNonEmptyRemoval*/ false
};

const NYT::NOrm::NServer::NObjects::TManyToManyInlineViewAttributeDescriptor<NYT::NOrm::NExample::NServer::NLibrary::TBook, NYT::NOrm::NExample::NServer::NLibrary::TAuthor> TBook::TSpec::TPeerReview::ReviewersDescriptor {
    TBook::TSpec::TPeerReview::ReviewerIdsDescriptor,
    [] (const TBook* obj) { return &obj->Spec().PeerReview().Reviewers(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TBook, TBook::TSpec::TPeerReview::TEtc>
TBook::TSpec::TPeerReview::EtcDescriptor{
    &BooksTable.Fields.SpecPeerReviewEtc,
    [] (TBook* obj) { return &obj->Spec().PeerReview().Etc(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TBook, TString>
TBook::TSpec::NameDescriptor{
    &BooksTable.Fields.SpecName,
    [] (TBook* obj) { return &obj->Spec().Name(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TBook, i32>
TBook::TSpec::YearDescriptor{
    &BooksTable.Fields.SpecYear,
    [] (TBook* obj) { return &obj->Spec().Year(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TBook, TString>
TBook::TSpec::FontDescriptor{
    &BooksTable.Fields.SpecFont,
    [] (TBook* obj) { return &obj->Spec().Font(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TBook, std::vector<TString>>
TBook::TSpec::GenresDescriptor{
    &BooksTable.Fields.SpecGenres,
    [] (TBook* obj) { return &obj->Spec().Genres(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TBook, std::vector<TString>>
TBook::TSpec::KeywordsDescriptor{
    &BooksTable.Fields.SpecKeywords,
    [] (TBook* obj) { return &obj->Spec().Keywords(); }
};

const NYT::NOrm::NServer::NObjects::TManyToOneAttributeDescriptor<NYT::NOrm::NExample::NServer::NLibrary::TBook, NYT::NOrm::NExample::NServer::NLibrary::TEditor> TBook::TSpec::EditorIdDescriptor {
    &BooksTable.Fields.SpecEditorId,
    [] (NYT::NOrm::NExample::NServer::NLibrary::TBook* obj) { return &obj->Spec().EditorId(); },
    [] (NYT::NOrm::NExample::NServer::NLibrary::TEditor* obj) { return &obj->Status().Books(); },
    /*forbidNonEmptyRemoval*/ false
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TBook, NYT::NOrm::NExample::NClient::NProto::NDataModel::TBookDescription::TDigitalData>
TBook::TSpec::DigitalDataDescriptor{
    &BooksTable.Fields.SpecDigitalData,
    [] (TBook* obj) { return &obj->Spec().DigitalData(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TBook, std::vector<TString>>
TBook::TSpec::AuthorIdsDescriptor{
    &BooksTable.Fields.SpecAuthorIds,
    [] (TBook* obj) { return &obj->Spec().AuthorIds(); }
};

const NYT::NOrm::NServer::NObjects::TManyToOneAttributeDescriptor<NYT::NOrm::NExample::NServer::NLibrary::TBook, NYT::NOrm::NExample::NServer::NLibrary::TIllustrator> TBook::TSpec::IllustratorIdDescriptor {
    &BooksTable.Fields.SpecIllustratorId,
    [] (NYT::NOrm::NExample::NServer::NLibrary::TBook* obj) { return &obj->Spec().IllustratorId(); },
    [] (NYT::NOrm::NExample::NServer::NLibrary::TIllustrator* obj) { return &obj->Status().Books(); },
    /*forbidNonEmptyRemoval*/ true
};

const NYT::NOrm::NServer::NObjects::TManyToManyInlineAttributeDescriptor<NYT::NOrm::NExample::NServer::NLibrary::TBook, NYT::NOrm::NExample::NServer::NLibrary::TPublisher> TBook::TSpec::AlternativePublisherIdsDescriptor {
    &BooksTable.Fields.SpecAlternativePublisherIds,
    [] (NYT::NOrm::NExample::NServer::NLibrary::TBook* obj) { return &obj->Spec().AlternativePublisherIds(); },
    [] (NYT::NOrm::NExample::NServer::NLibrary::TPublisher* obj) { return &obj->Status().Books(); },
    /*forbidNonEmptyRemoval*/ true
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TBook, std::vector<NYT::NOrm::NExample::NClient::NProto::NDataModel::TBookDescription::TChapterDescription>>
TBook::TSpec::ChapterDescriptionsDescriptor{
    &BooksTable.Fields.SpecChapterDescriptions,
    [] (TBook* obj) { return &obj->Spec().ChapterDescriptions(); }
};

const NYT::NOrm::NServer::NObjects::TManyToOneAttributeDescriptor<NYT::NOrm::NExample::NServer::NLibrary::TBook, NYT::NOrm::NExample::NServer::NLibrary::TIllustrator> TBook::TSpec::CoverIllustratorIdDescriptor {
    &BooksTable.Fields.SpecCoverIllustratorId,
    [] (NYT::NOrm::NExample::NServer::NLibrary::TBook* obj) { return &obj->Spec().CoverIllustratorId(); },
    [] (NYT::NOrm::NExample::NServer::NLibrary::TIllustrator* obj) { return &obj->Status().BooksForCoverIllustrator(); },
    /*forbidNonEmptyRemoval*/ true
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TBook, ui64>
TBook::TSpec::ApiRevisionDescriptor{
    &BooksTable.Fields.SpecApiRevision,
    [] (TBook* obj) { return &obj->Spec().ApiRevision(); }
};

const NYT::NOrm::NServer::NObjects::TMultiReferenceDescriptor<TBook, TAuthor> TBook::TSpec::AuthorRefsDescriptor {
    .ForwardAttributeGetter = [] (TBook* obj) { return &obj->Spec().AuthorRefs(); },
    .InverseAttributeGetter = [] (TAuthor* obj) { return &obj->Status().BookRefs(); },
    .KeyStorageDescriptor = NYT::NOrm::NServer::NObjects::TColumnarKeyStorageDescriptor{
        .KeyLocators = { { &TBook::TSpec::AuthorIdsDescriptor, "/*" } },
    },
    .Settings = NYT::NOrm::NServer::NObjects::TReferenceAttributeSettings{
        .StoreParentKey = false,
    },
};

const NYT::NOrm::NServer::NObjects::TManyToOneViewAttributeDescriptor<NYT::NOrm::NExample::NServer::NLibrary::TBook, NYT::NOrm::NExample::NServer::NLibrary::TEditor> TBook::TSpec::EditorDescriptor {
    TBook::TSpec::EditorIdDescriptor,
    [] (const TBook* obj) { return &obj->Spec().Editor(); }
};

const NYT::NOrm::NServer::NObjects::TManyToOneViewAttributeDescriptor<NYT::NOrm::NExample::NServer::NLibrary::TBook, NYT::NOrm::NExample::NServer::NLibrary::TIllustrator> TBook::TSpec::IllustratorDescriptor {
    TBook::TSpec::IllustratorIdDescriptor,
    [] (const TBook* obj) { return &obj->Spec().Illustrator(); }
};

const NYT::NOrm::NServer::NObjects::TManyToManyInlineViewAttributeDescriptor<NYT::NOrm::NExample::NServer::NLibrary::TBook, NYT::NOrm::NExample::NServer::NLibrary::TPublisher> TBook::TSpec::PublishersDescriptor {
    TBook::TSpec::AlternativePublisherIdsDescriptor,
    [] (const TBook* obj) { return &obj->Spec().Publishers(); }
};

const NYT::NOrm::NServer::NObjects::TManyToOneViewAttributeDescriptor<NYT::NOrm::NExample::NServer::NLibrary::TBook, NYT::NOrm::NExample::NServer::NLibrary::TIllustrator> TBook::TSpec::CoverIllustratorDescriptor {
    TBook::TSpec::CoverIllustratorIdDescriptor,
    [] (const TBook* obj) { return &obj->Spec().CoverIllustrator(); }
};

const NYT::NOrm::NServer::NObjects::TMultiViewDescriptor<TBook, TAuthor> TBook::TSpec::AuthorsDescriptor {
    .ReferenceDescriptor = TBook::TSpec::AuthorRefsDescriptor,
    .ViewAttributeGetter = [] (TBook* obj) { return &obj->Spec().Authors(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TBook, TBook::TSpec::TEtc>
TBook::TSpec::EtcDescriptor{
    &BooksTable.Fields.SpecEtc,
    [] (TBook* obj) { return &obj->Spec().Etc(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TBook, TBook::TSpec::TWarehouseEtc>
TBook::TSpec::WarehouseEtcDescriptor{
    &BooksTable.Fields.SpecEtcWarehouse,
    [] (TBook* obj) { return &obj->Spec().WarehouseEtc(); }
};

TBook::TStatus::TStatus(TBook* object)
    : Released_(object, &ReleasedDescriptor)
    , Hitchhikers_(
        object,
        HitchhikersDescriptor,
        std::make_unique<NYT::NOrm::NServer::NObjects::TColumnarMultiKeyStorageDriver>(object, HitchhikersDescriptor.KeyStorageDescriptor))
    , FormedHitchhikers_(
        object,
        FormedHitchhikersDescriptor,
        std::make_unique<NYT::NOrm::NServer::NObjects::TColumnarMultiKeyStorageDriver>(object, FormedHitchhikersDescriptor.KeyStorageDescriptor))
    , HitchhikersView_(object)
    , FormedHitchhikersView_(object)
    , Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TBook, bool>
TBook::TStatus::ReleasedDescriptor{
    &BooksTable.Fields.StatusReleased,
    [] (TBook* obj) { return &obj->Status().Released(); }
};

const NYT::NOrm::NServer::NObjects::TMultiReferenceDescriptor<TBook, THitchhiker> TBook::TStatus::HitchhikersDescriptor {
    .ForwardAttributeGetter = [] (TBook* obj) { return &obj->Status().Hitchhikers(); },
    .InverseAttributeGetter = [] (THitchhiker* obj) { return &obj->Spec().Books(); },
    .KeyStorageDescriptor = NYT::NOrm::NServer::NObjects::TColumnarKeyStorageDescriptor{
        .KeyLocators = { { &TBook::TStatus::EtcDescriptor, "/hitchhiker_ids/*" } },
    },
    .Settings = NYT::NOrm::NServer::NObjects::TReferenceAttributeSettings{
        .StoreParentKey = false,
    },
};

const NYT::NOrm::NServer::NObjects::TMultiReferenceDescriptor<TBook, THitchhiker> TBook::TStatus::FormedHitchhikersDescriptor {
    .ForwardAttributeGetter = [] (TBook* obj) { return &obj->Status().FormedHitchhikers(); },
    .InverseAttributeGetter = [] (THitchhiker* obj) { return &obj->FormativeBook(); },
    .KeyStorageDescriptor = NYT::NOrm::NServer::NObjects::TColumnarKeyStorageDescriptor{
        .KeyLocators = { { &TBook::TStatus::EtcDescriptor, "/formed_hitchhiker_ids/*" } },
    },
    .Settings = NYT::NOrm::NServer::NObjects::TReferenceAttributeSettings{
        .StoreParentKey = false,
    },
};

const NYT::NOrm::NServer::NObjects::TMultiViewDescriptor<TBook, THitchhiker> TBook::TStatus::HitchhikersViewDescriptor {
    .ReferenceDescriptor = TBook::TStatus::HitchhikersDescriptor,
    .ViewAttributeGetter = [] (TBook* obj) { return &obj->Status().HitchhikersView(); }
};

const NYT::NOrm::NServer::NObjects::TMultiViewDescriptor<TBook, THitchhiker> TBook::TStatus::FormedHitchhikersViewDescriptor {
    .ReferenceDescriptor = TBook::TStatus::FormedHitchhikersDescriptor,
    .ViewAttributeGetter = [] (TBook* obj) { return &obj->Status().FormedHitchhikersView(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TBook, TBook::TStatus::TEtc>
TBook::TStatus::EtcDescriptor{
    &BooksTable.Fields.StatusEtc,
    [] (TBook* obj) { return &obj->Status().Etc(); }
};

////////////////////////////////////////////////////////////////////////////////

TBook::TBook(
    const i64& id,
    const i64& id2,
    const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TDataModelObject(typeHandler, session)
    , Id_(id)
    , Id2_(id2)
    , ParentKeyAttribute_(this, parentKey)
    , Publisher_(this)
    , Isbn_(this, &IsbnDescriptor)
    , FinalizationStartTime_(this, &FinalizationStartTimeDescriptor)
    , Finalizers_(this, &FinalizersDescriptor)
    , MetaEtc_(this, &MetaEtcDescriptor)
    , Spec_(this)
    , Status_(this)
{
    RegisterScalarAttributeIndex(
        "books_by_cover_dpi",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndex>(
            typeHandler->GetIndexDescriptorOrThrow("books_by_cover_dpi"),
            this,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
                NYT::NOrm::NServer::NObjects::TIndexAttribute(
                    &Spec().Etc(),
                    "/design/cover/dpi",
                    /*repeated*/ false),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
            },
            /*unique*/ false));
    RegisterScalarAttributeIndex(
        "books_by_cover_image",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndex>(
            typeHandler->GetIndexDescriptorOrThrow("books_by_cover_image"),
            this,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
                NYT::NOrm::NServer::NObjects::TIndexAttribute(
                    &Spec().Etc(),
                    "/design/cover/image",
                    /*repeated*/ false),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
            },
            /*unique*/ false));
    RegisterScalarAttributeIndex(
        "books_by_cover_size",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndex>(
            typeHandler->GetIndexDescriptorOrThrow("books_by_cover_size"),
            this,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
                NYT::NOrm::NServer::NObjects::TIndexAttribute(
                    &Spec().Etc(),
                    "/design/cover/size",
                    /*repeated*/ false),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
            },
            /*unique*/ false));
    RegisterScalarAttributeIndex(
        "books_by_creation_time",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndex>(
            typeHandler->GetIndexDescriptorOrThrow("books_by_creation_time"),
            this,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
                NYT::NOrm::NServer::NObjects::TIndexAttribute(
                    &CreationTime(),
                    "",
                    /*repeated*/ false),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
            },
            /*unique*/ false));
    RegisterScalarAttributeIndex(
        "books_by_editor_and_year_with_predicate",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndex>(
            typeHandler->GetIndexDescriptorOrThrow("books_by_editor_and_year_with_predicate"),
            this,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
                NYT::NOrm::NServer::NObjects::TIndexAttribute(
                    &Spec().EditorId(),
                    "",
                    /*repeated*/ false),
                NYT::NOrm::NServer::NObjects::TIndexAttribute(
                    &Spec().Year(),
                    "",
                    /*repeated*/ false),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
                NYT::NOrm::NServer::NObjects::TIndexAttribute(
                    &Spec().Font(),
                    "",
                    /*repeated*/ false),
            },
            /*unique*/ false));
    RegisterScalarAttributeIndex(
        "books_by_font",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndex>(
            typeHandler->GetIndexDescriptorOrThrow("books_by_font"),
            this,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
                NYT::NOrm::NServer::NObjects::TIndexAttribute(
                    &Spec().Font(),
                    "",
                    /*repeated*/ false),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
            },
            /*unique*/ false));
    RegisterScalarAttributeIndex(
        "books_by_genres",
        std::make_unique<NYT::NOrm::NServer::NObjects::TRepeatedScalarAttributeIndex>(
            typeHandler->GetIndexDescriptorOrThrow("books_by_genres"),
            this,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
                NYT::NOrm::NServer::NObjects::TIndexAttribute(
                    &Spec().Genres(),
                    "",
                    /*repeated*/ true),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
            }));
    RegisterScalarAttributeIndex(
        "books_by_illustrations",
        std::make_unique<NYT::NOrm::NServer::NObjects::TRepeatedScalarAttributeIndex>(
            typeHandler->GetIndexDescriptorOrThrow("books_by_illustrations"),
            this,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
                NYT::NOrm::NServer::NObjects::TIndexAttribute(
                    &Spec().Etc(),
                    "/design/illustrations",
                    /*repeated*/ true),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
            }));
    RegisterScalarAttributeIndex(
        "books_by_illustrations_with_predicate",
        std::make_unique<NYT::NOrm::NServer::NObjects::TRepeatedScalarAttributeIndex>(
            typeHandler->GetIndexDescriptorOrThrow("books_by_illustrations_with_predicate"),
            this,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
                NYT::NOrm::NServer::NObjects::TIndexAttribute(
                    &Spec().Etc(),
                    "/design/illustrations",
                    /*repeated*/ true),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
                NYT::NOrm::NServer::NObjects::TIndexAttribute(
                    &Spec().Font(),
                    "",
                    /*repeated*/ false),
                NYT::NOrm::NServer::NObjects::TIndexAttribute(
                    &Spec().Etc(),
                    "/design/illustrations",
                    /*repeated*/ true),
            }));
    RegisterScalarAttributeIndex(
        "books_by_isbn",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndex>(
            typeHandler->GetIndexDescriptorOrThrow("books_by_isbn"),
            this,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
                NYT::NOrm::NServer::NObjects::TIndexAttribute(
                    &Isbn(),
                    "",
                    /*repeated*/ false),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
            },
            /*unique*/ false));
    RegisterScalarAttributeIndex(
        "books_by_isbn_with_predicate",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndex>(
            typeHandler->GetIndexDescriptorOrThrow("books_by_isbn_with_predicate"),
            this,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
                NYT::NOrm::NServer::NObjects::TIndexAttribute(
                    &Isbn(),
                    "",
                    /*repeated*/ false),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
                NYT::NOrm::NServer::NObjects::TIndexAttribute(
                    &Isbn(),
                    "",
                    /*repeated*/ false),
            },
            /*unique*/ false));
    RegisterScalarAttributeIndex(
        "books_by_keywords",
        std::make_unique<NYT::NOrm::NServer::NObjects::TRepeatedScalarAttributeIndex>(
            typeHandler->GetIndexDescriptorOrThrow("books_by_keywords"),
            this,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
                NYT::NOrm::NServer::NObjects::TIndexAttribute(
                    &Spec().Keywords(),
                    "",
                    /*repeated*/ true),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
            }));
    RegisterScalarAttributeIndex(
        "books_by_name",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndex>(
            typeHandler->GetIndexDescriptorOrThrow("books_by_name"),
            this,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
                NYT::NOrm::NServer::NObjects::TIndexAttribute(
                    &Spec().Name(),
                    "",
                    /*repeated*/ false),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
            },
            /*unique*/ false));
    RegisterScalarAttributeIndex(
        "books_by_page_count",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndex>(
            typeHandler->GetIndexDescriptorOrThrow("books_by_page_count"),
            this,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
                NYT::NOrm::NServer::NObjects::TIndexAttribute(
                    &Spec().Etc(),
                    "/page_count",
                    /*repeated*/ false),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
            },
            /*unique*/ false));
    RegisterScalarAttributeIndex(
        "books_by_store_rating",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndex>(
            typeHandler->GetIndexDescriptorOrThrow("books_by_store_rating"),
            this,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
                NYT::NOrm::NServer::NObjects::TIndexAttribute(
                    &Spec().DigitalData(),
                    "/store_rating",
                    /*repeated*/ false),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
            },
            /*unique*/ false));
    RegisterScalarAttributeIndex(
        "books_by_year",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndex>(
            typeHandler->GetIndexDescriptorOrThrow("books_by_year"),
            this,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
                NYT::NOrm::NServer::NObjects::TIndexAttribute(
                    &Spec().Year(),
                    "",
                    /*repeated*/ false),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
            },
            /*unique*/ false));
    RegisterScalarAttributeIndex(
        "books_by_year_and_font",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndex>(
            typeHandler->GetIndexDescriptorOrThrow("books_by_year_and_font"),
            this,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
                NYT::NOrm::NServer::NObjects::TIndexAttribute(
                    &Spec().Year(),
                    "",
                    /*repeated*/ false),
                NYT::NOrm::NServer::NObjects::TIndexAttribute(
                    &Spec().Font(),
                    "",
                    /*repeated*/ false),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
            },
            /*unique*/ false));
    RegisterScalarAttributeIndex(
        "books_by_year_and_page_count_with_predicate",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndex>(
            typeHandler->GetIndexDescriptorOrThrow("books_by_year_and_page_count_with_predicate"),
            this,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
                NYT::NOrm::NServer::NObjects::TIndexAttribute(
                    &Spec().Year(),
                    "",
                    /*repeated*/ false),
                NYT::NOrm::NServer::NObjects::TIndexAttribute(
                    &Spec().Etc(),
                    "/page_count",
                    /*repeated*/ false),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
                NYT::NOrm::NServer::NObjects::TIndexAttribute(
                    &Spec().Etc(),
                    "/design/cover/hardcover",
                    /*repeated*/ false),
            },
            /*unique*/ false));
    RegisterScalarAttributeIndex(
        "books_by_cover_size_with_predicate",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndex>(
            typeHandler->GetIndexDescriptorOrThrow("books_by_cover_size_with_predicate"),
            this,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
                NYT::NOrm::NServer::NObjects::TIndexAttribute(
                    &Spec().Year(),
                    "",
                    /*repeated*/ false),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
                NYT::NOrm::NServer::NObjects::TIndexAttribute(
                    &Spec().Etc(),
                    "/design/cover/dpi",
                    /*repeated*/ false),
            },
            /*unique*/ false));
}

////////////////////////////////////////////////////////////////////////////////

NYT::NOrm::NClient::NObjects::TObjectKey TBook::GetKey() const
{
    return NYT::NOrm::NServer::NObjects::TObjectKey(
        GetId(),
        GetId2());
}

NYT::NOrm::NClient::NObjects::TObjectKey TBook::GetParentKey(
    std::source_location location) const
{
    return ParentKeyAttribute_.GetKey(location);
}

NYT::NOrm::NServer::NObjects::TParentKeyAttribute* TBook::GetParentKeyAttribute()
{
    return &ParentKeyAttribute_;
}

i64 TBook::PublisherId(std::source_location location) const
{
    return GetParentKey(location).GetWithDefault<i64>(0);
}

void TBook::ScheduleUuidLoad() const
{
    MetaEtc().ScheduleLoad();
}

NYT::NOrm::NClient::NObjects::TObjectId TBook::GetUuid(
    std::source_location location) const
{
    return MetaEtc().Load(location).uuid();
}

TString TBook::GetName(
    std::source_location location) const
{
    return MetaEtc().Load(location).name();
}

////////////////////////////////////////////////////////////////////////////////

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TBufferedTimestampId, ui64>
TBufferedTimestampId::FinalizationStartTimeDescriptor{
    &BufferedTimestampIdsTable.Fields.MetaFinalizationStartTime,
    [] (TBufferedTimestampId* obj) { return &obj->FinalizationStartTime(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TBufferedTimestampId, THashMap<TString, NYT::NOrm::NDataModel::TFinalizer>>
TBufferedTimestampId::FinalizersDescriptor{
    &BufferedTimestampIdsTable.Fields.MetaFinalizers,
    [] (TBufferedTimestampId* obj) { return &obj->Finalizers(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TBufferedTimestampId, TBufferedTimestampId::TMetaEtc>
TBufferedTimestampId::MetaEtcDescriptor{
    &BufferedTimestampIdsTable.Fields.MetaEtc,
    [] (TBufferedTimestampId* object) { return &object->MetaEtc(); }
};

////////////////////////////////////////////////////////////////////////////////

TBufferedTimestampId::TSpec::TSpec(TBufferedTimestampId* object)
    : I64Value_(object, &I64ValueDescriptor)
    , Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TBufferedTimestampId, i64>
TBufferedTimestampId::TSpec::I64ValueDescriptor{
    &BufferedTimestampIdsTable.Fields.SpecI64Value,
    [] (TBufferedTimestampId* obj) { return &obj->Spec().I64Value(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TBufferedTimestampId, TBufferedTimestampId::TSpec::TEtc>
TBufferedTimestampId::TSpec::EtcDescriptor{
    &BufferedTimestampIdsTable.Fields.SpecEtc,
    [] (TBufferedTimestampId* obj) { return &obj->Spec().Etc(); }
};

TBufferedTimestampId::TStatus::TStatus(TBufferedTimestampId* object)
    : Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TBufferedTimestampId, TBufferedTimestampId::TStatus::TEtc>
TBufferedTimestampId::TStatus::EtcDescriptor{
    &BufferedTimestampIdsTable.Fields.StatusEtc,
    [] (TBufferedTimestampId* obj) { return &obj->Status().Etc(); }
};

////////////////////////////////////////////////////////////////////////////////

TBufferedTimestampId::TBufferedTimestampId(
    const i64& i64Id,
    const ui64& ui64Id,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TDataModelObject(typeHandler, session)
    , I64Id_(i64Id)
    , Ui64Id_(ui64Id)
    , FinalizationStartTime_(this, &FinalizationStartTimeDescriptor)
    , Finalizers_(this, &FinalizersDescriptor)
    , MetaEtc_(this, &MetaEtcDescriptor)
    , Spec_(this)
    , Status_(this)
{
}

////////////////////////////////////////////////////////////////////////////////

NYT::NOrm::NClient::NObjects::TObjectKey TBufferedTimestampId::GetKey() const
{
    return NYT::NOrm::NServer::NObjects::TObjectKey(
        GetI64Id(),
        GetUi64Id());
}

void TBufferedTimestampId::ScheduleUuidLoad() const
{
    MetaEtc().ScheduleLoad();
}

NYT::NOrm::NClient::NObjects::TObjectId TBufferedTimestampId::GetUuid(
    std::source_location location) const
{
    return MetaEtc().Load(location).uuid();
}

TString TBufferedTimestampId::GetName(
    std::source_location location) const
{
    return MetaEtc().Load(location).name();
}

////////////////////////////////////////////////////////////////////////////////

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, TString>
TCat::BreedDescriptor{
    &CatsTable.Fields.MetaBreed,
    [] (TCat* obj) { return &obj->Breed(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, ui64>
TCat::FinalizationStartTimeDescriptor{
    &CatsTable.Fields.MetaFinalizationStartTime,
    [] (TCat* obj) { return &obj->FinalizationStartTime(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, THashMap<TString, NYT::NOrm::NDataModel::TFinalizer>>
TCat::FinalizersDescriptor{
    &CatsTable.Fields.MetaFinalizers,
    [] (TCat* obj) { return &obj->Finalizers(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, TCat::TMetaEtc>
TCat::MetaEtcDescriptor{
    &CatsTable.Fields.MetaEtc,
    [] (TCat* object) { return &object->MetaEtc(); }
};

////////////////////////////////////////////////////////////////////////////////

TCat::TSpec::TSpec(TCat* object)
    : LastSleepDuration_(object, &LastSleepDurationDescriptor)
    , FavouriteFood_(object, &FavouriteFoodDescriptor)
    , FavouriteToy_(object, &FavouriteToyDescriptor)
    , Revision_(object, &RevisionDescriptor)
    , Mood_(object, &MoodDescriptor)
    , HealthCondition_(object, &HealthConditionDescriptor)
    , EyeColorWithDefaultYsonStorageType_(object, &EyeColorWithDefaultYsonStorageTypeDescriptor)
    , FriendCatsCount_(object, &FriendCatsCountDescriptor)
    , StatisticsForDaysOfYear_(object, &StatisticsForDaysOfYearDescriptor)
    , Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, i64>
TCat::TSpec::LastSleepDurationDescriptor{
    &CatsTable.Fields.SpecLastSleepDuration,
    [] (TCat* obj) { return &obj->Spec().LastSleepDuration(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, TString>
TCat::TSpec::FavouriteFoodDescriptor{
    &CatsTable.Fields.SpecFavouriteFood,
    [] (TCat* obj) { return &obj->Spec().FavouriteFood(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, TString>
TCat::TSpec::FavouriteToyDescriptor{
    &CatsTable.Fields.SpecFavouriteToy,
    [] (TCat* obj) { return &obj->Spec().FavouriteToy(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, ui64>
TCat::TSpec::RevisionDescriptor{
    &CatsTable.Fields.SpecRevision,
    [] (TCat* obj) { return &obj->Spec().Revision(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, EMood>
TCat::TSpec::MoodDescriptor{
    &CatsTable.Fields.SpecMood,
    [] (TCat* obj) { return &obj->Spec().Mood(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, EHealthCondition>
TCat::TSpec::HealthConditionDescriptor{
    &CatsTable.Fields.SpecHealthCondition,
    [] (TCat* obj) { return &obj->Spec().HealthCondition(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, EEyeColor>
TCat::TSpec::EyeColorWithDefaultYsonStorageTypeDescriptor{
    &CatsTable.Fields.SpecEyeColorWithDefaultYsonStorageType,
    [] (TCat* obj) { return &obj->Spec().EyeColorWithDefaultYsonStorageType(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAggregatedAttributeDescriptor<TCat, i64>
TCat::TSpec::FriendCatsCountDescriptor{
    &CatsTable.Fields.SpecFriendCatsCount,
    [] (TCat* obj) { return &obj->Spec().FriendCatsCount(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAggregatedAttributeDescriptor<TCat, THashMap<ui32, NYT::NOrm::NExample::NClient::NProto::NDataModel::TCatSpec::DayStatistics>>
TCat::TSpec::StatisticsForDaysOfYearDescriptor{
    &CatsTable.Fields.SpecStatisticsForDaysOfYear,
    [] (TCat* obj) { return &obj->Spec().StatisticsForDaysOfYear(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, TCat::TSpec::TEtc>
TCat::TSpec::EtcDescriptor{
    &CatsTable.Fields.SpecEtc,
    [] (TCat* obj) { return &obj->Spec().Etc(); }
};

TCat::TStatus::TStatus(TCat* object)
    : UpdatableNested_(object)
    , ReadOnlyNested_(object)
    , OpaqueRoNested_(object)
    , OpaqueNested_(object)
    , Etc_(object, &EtcDescriptor)
{ }

TCat::TStatus::TUpdatableNested::TUpdatableNested(TCat* object)
    : InnerFirst_(object, &InnerFirstDescriptor)
    , InnerSecond_(object, &InnerSecondDescriptor)
    , InnerThird_(object, &InnerThirdDescriptor)
    , Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, i64>
TCat::TStatus::TUpdatableNested::InnerFirstDescriptor{
    &CatsTable.Fields.StatusUpdatableNestedInnerFirst,
    [] (TCat* obj) { return &obj->Status().UpdatableNested().InnerFirst(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, i64>
TCat::TStatus::TUpdatableNested::InnerSecondDescriptor{
    &CatsTable.Fields.StatusUpdatableNestedInnerSecond,
    [] (TCat* obj) { return &obj->Status().UpdatableNested().InnerSecond(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, i64>
TCat::TStatus::TUpdatableNested::InnerThirdDescriptor{
    &CatsTable.Fields.StatusUpdatableNestedInnerThird,
    [] (TCat* obj) { return &obj->Status().UpdatableNested().InnerThird(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, TCat::TStatus::TUpdatableNested::TEtc>
TCat::TStatus::TUpdatableNested::EtcDescriptor{
    &CatsTable.Fields.StatusUpdatableNestedEtc,
    [] (TCat* obj) { return &obj->Status().UpdatableNested().Etc(); }
};

TCat::TStatus::TReadOnlyNested::TReadOnlyNested(TCat* object)
    : InnerFirst_(object, &InnerFirstDescriptor)
    , InnerSecond_(object, &InnerSecondDescriptor)
    , InnerThird_(object, &InnerThirdDescriptor)
    , Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, i64>
TCat::TStatus::TReadOnlyNested::InnerFirstDescriptor{
    &CatsTable.Fields.StatusReadOnlyNestedInnerFirst,
    [] (TCat* obj) { return &obj->Status().ReadOnlyNested().InnerFirst(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, i64>
TCat::TStatus::TReadOnlyNested::InnerSecondDescriptor{
    &CatsTable.Fields.StatusReadOnlyNestedInnerSecond,
    [] (TCat* obj) { return &obj->Status().ReadOnlyNested().InnerSecond(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, i64>
TCat::TStatus::TReadOnlyNested::InnerThirdDescriptor{
    &CatsTable.Fields.StatusReadOnlyNestedInnerThird,
    [] (TCat* obj) { return &obj->Status().ReadOnlyNested().InnerThird(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, TCat::TStatus::TReadOnlyNested::TEtc>
TCat::TStatus::TReadOnlyNested::EtcDescriptor{
    &CatsTable.Fields.StatusReadOnlyNestedEtc,
    [] (TCat* obj) { return &obj->Status().ReadOnlyNested().Etc(); }
};

TCat::TStatus::TOpaqueRoNested::TOpaqueRoNested(TCat* object)
    : InnerFirst_(object, &InnerFirstDescriptor)
    , InnerSecond_(object, &InnerSecondDescriptor)
    , InnerThird_(object, &InnerThirdDescriptor)
    , Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, i64>
TCat::TStatus::TOpaqueRoNested::InnerFirstDescriptor{
    &CatsTable.Fields.StatusOpaqueRoNestedInnerFirst,
    [] (TCat* obj) { return &obj->Status().OpaqueRoNested().InnerFirst(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, i64>
TCat::TStatus::TOpaqueRoNested::InnerSecondDescriptor{
    &CatsTable.Fields.StatusOpaqueRoNestedInnerSecond,
    [] (TCat* obj) { return &obj->Status().OpaqueRoNested().InnerSecond(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, i64>
TCat::TStatus::TOpaqueRoNested::InnerThirdDescriptor{
    &CatsTable.Fields.StatusOpaqueRoNestedInnerThird,
    [] (TCat* obj) { return &obj->Status().OpaqueRoNested().InnerThird(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, TCat::TStatus::TOpaqueRoNested::TEtc>
TCat::TStatus::TOpaqueRoNested::EtcDescriptor{
    &CatsTable.Fields.StatusOpaqueRoNestedEtc,
    [] (TCat* obj) { return &obj->Status().OpaqueRoNested().Etc(); }
};

TCat::TStatus::TOpaqueNested::TOpaqueNested(TCat* object)
    : InnerFirst_(object, &InnerFirstDescriptor)
    , InnerSecond_(object, &InnerSecondDescriptor)
    , InnerThird_(object, &InnerThirdDescriptor)
    , Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, i64>
TCat::TStatus::TOpaqueNested::InnerFirstDescriptor{
    &CatsTable.Fields.StatusOpaqueNestedInnerFirst,
    [] (TCat* obj) { return &obj->Status().OpaqueNested().InnerFirst(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, i64>
TCat::TStatus::TOpaqueNested::InnerSecondDescriptor{
    &CatsTable.Fields.StatusOpaqueNestedInnerSecond,
    [] (TCat* obj) { return &obj->Status().OpaqueNested().InnerSecond(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, i64>
TCat::TStatus::TOpaqueNested::InnerThirdDescriptor{
    &CatsTable.Fields.StatusOpaqueNestedInnerThird,
    [] (TCat* obj) { return &obj->Status().OpaqueNested().InnerThird(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, TCat::TStatus::TOpaqueNested::TEtc>
TCat::TStatus::TOpaqueNested::EtcDescriptor{
    &CatsTable.Fields.StatusOpaqueNestedEtc,
    [] (TCat* obj) { return &obj->Status().OpaqueNested().Etc(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, TCat::TStatus::TEtc>
TCat::TStatus::EtcDescriptor{
    &CatsTable.Fields.StatusEtc,
    [] (TCat* obj) { return &obj->Status().Etc(); }
};

////////////////////////////////////////////////////////////////////////////////

TCat::TCat(
    const i64& id,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TDataModelObject(typeHandler, session)
    , Id_(id)
    , Breed_(this, &BreedDescriptor)
    , FinalizationStartTime_(this, &FinalizationStartTimeDescriptor)
    , Finalizers_(this, &FinalizersDescriptor)
    , MetaEtc_(this, &MetaEtcDescriptor)
    , Spec_(this)
    , Status_(this)
{
    RegisterScalarAttributeIndex(
        "cats_by_names_with_predicate",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndex>(
            typeHandler->GetIndexDescriptorOrThrow("cats_by_names_with_predicate"),
            this,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
                NYT::NOrm::NServer::NObjects::TIndexAttribute(
                    &Spec().Etc(),
                    "/name",
                    /*repeated*/ false),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
                NYT::NOrm::NServer::NObjects::TIndexAttribute(
                    &Spec().LastSleepDuration(),
                    "",
                    /*repeated*/ false),
            },
            /*unique*/ false));
}

////////////////////////////////////////////////////////////////////////////////

NYT::NOrm::NClient::NObjects::TObjectKey TCat::GetKey() const
{
    return NYT::NOrm::NServer::NObjects::TObjectKey(
        GetId());
}

void TCat::ScheduleUuidLoad() const
{
    MetaEtc().ScheduleLoad();
}

NYT::NOrm::NClient::NObjects::TObjectId TCat::GetUuid(
    std::source_location location) const
{
    return MetaEtc().Load(location).uuid();
}

TString TCat::GetName(
    std::source_location location) const
{
    return MetaEtc().Load(location).name();
}

////////////////////////////////////////////////////////////////////////////////

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TEditor, ui64>
TEditor::FinalizationStartTimeDescriptor{
    &EditorsTable.Fields.MetaFinalizationStartTime,
    [] (TEditor* obj) { return &obj->FinalizationStartTime(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TEditor, THashMap<TString, NYT::NOrm::NDataModel::TFinalizer>>
TEditor::FinalizersDescriptor{
    &EditorsTable.Fields.MetaFinalizers,
    [] (TEditor* obj) { return &obj->Finalizers(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TEditor, TEditor::TMetaEtc>
TEditor::MetaEtcDescriptor{
    &EditorsTable.Fields.MetaEtc,
    [] (TEditor* object) { return &object->MetaEtc(); }
};

////////////////////////////////////////////////////////////////////////////////

TEditor::TSpec::TSpec(TEditor* object)
    : Achievements_(object, &AchievementsDescriptor)
    , PhoneNumber_(object, &PhoneNumberDescriptor)
    , Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TEditor, std::vector<TString>>
TEditor::TSpec::AchievementsDescriptor{
    &EditorsTable.Fields.SpecAchievements,
    [] (TEditor* obj) { return &obj->Spec().Achievements(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TEditor, TString>
TEditor::TSpec::PhoneNumberDescriptor{
    &EditorsTable.Fields.SpecPhoneNumber,
    [] (TEditor* obj) { return &obj->Spec().PhoneNumber(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TEditor, TEditor::TSpec::TEtc>
TEditor::TSpec::EtcDescriptor{
    &EditorsTable.Fields.SpecEtc,
    [] (TEditor* obj) { return &obj->Spec().Etc(); }
};

TEditor::TStatus::TStatus(TEditor* object)
    : Books_(object, &BooksDescriptor)
    , Publishers_(object, &PublishersDescriptor)
    , Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TOneToManyAttributeDescriptor<NYT::NOrm::NExample::NServer::NLibrary::TEditor, NYT::NOrm::NExample::NServer::NLibrary::TBook> TEditor::TStatus::BooksDescriptor {
    &EditorToBooksTable,
    { &EditorToBooksTable.Fields.EditorId },
    { &EditorToBooksTable.Fields.BookId, &EditorToBooksTable.Fields.BookId2 },
    [] (NYT::NOrm::NExample::NServer::NLibrary::TEditor* obj) { return &obj->Status().Books(); },
    [] (NYT::NOrm::NExample::NServer::NLibrary::TBook* obj) { return &obj->Spec().EditorId(); },
    /*foreignObjectTableKey*/ true,
    /*forbidNonEmptyRemoval*/ false
};

const NYT::NOrm::NServer::NObjects::TOneToManyAttributeDescriptor<NYT::NOrm::NExample::NServer::NLibrary::TEditor, NYT::NOrm::NExample::NServer::NLibrary::TPublisher> TEditor::TStatus::PublishersDescriptor {
    &EditorToPublishersTable,
    { &EditorToPublishersTable.Fields.EditorId },
    { &EditorToPublishersTable.Fields.PublisherId },
    [] (NYT::NOrm::NExample::NServer::NLibrary::TEditor* obj) { return &obj->Status().Publishers(); },
    [] (NYT::NOrm::NExample::NServer::NLibrary::TPublisher* obj) { return &obj->Spec().EditorInChief(); },
    /*foreignObjectTableKey*/ false,
    /*forbidNonEmptyRemoval*/ false
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TEditor, TEditor::TStatus::TEtc>
TEditor::TStatus::EtcDescriptor{
    &EditorsTable.Fields.StatusEtc,
    [] (TEditor* obj) { return &obj->Status().Etc(); }
};

////////////////////////////////////////////////////////////////////////////////

TEditor::TEditor(
    const TString& id,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TDataModelObject(typeHandler, session)
    , Id_(id)
    , FinalizationStartTime_(this, &FinalizationStartTimeDescriptor)
    , Finalizers_(this, &FinalizersDescriptor)
    , MetaEtc_(this, &MetaEtcDescriptor)
    , Spec_(this)
    , Status_(this)
{
    RegisterScalarAttributeIndex(
        "editors_by_achievements",
        std::make_unique<NYT::NOrm::NServer::NObjects::TRepeatedScalarAttributeIndex>(
            typeHandler->GetIndexDescriptorOrThrow("editors_by_achievements"),
            this,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
                NYT::NOrm::NServer::NObjects::TIndexAttribute(
                    &Spec().Achievements(),
                    "",
                    /*repeated*/ true),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
            }));
    RegisterScalarAttributeIndex(
        "editors_by_name",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndex>(
            typeHandler->GetIndexDescriptorOrThrow("editors_by_name"),
            this,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
                NYT::NOrm::NServer::NObjects::TIndexAttribute(
                    &Spec().Etc(),
                    "/name",
                    /*repeated*/ false),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
            },
            /*unique*/ false));
    RegisterScalarAttributeIndex(
        "editors_by_phone_number",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndex>(
            typeHandler->GetIndexDescriptorOrThrow("editors_by_phone_number"),
            this,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
                NYT::NOrm::NServer::NObjects::TIndexAttribute(
                    &Spec().PhoneNumber(),
                    "",
                    /*repeated*/ false),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
            },
            /*unique*/ false));
    RegisterScalarAttributeIndex(
        "editors_by_post",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndex>(
            typeHandler->GetIndexDescriptorOrThrow("editors_by_post"),
            this,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
                NYT::NOrm::NServer::NObjects::TIndexAttribute(
                    &Spec().Etc(),
                    "/post",
                    /*repeated*/ false),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
            },
            /*unique*/ false));
}

////////////////////////////////////////////////////////////////////////////////

NYT::NOrm::NClient::NObjects::TObjectKey TEditor::GetKey() const
{
    return NYT::NOrm::NServer::NObjects::TObjectKey(
        GetId());
}

void TEditor::ScheduleUuidLoad() const
{
    MetaEtc().ScheduleLoad();
}

NYT::NOrm::NClient::NObjects::TObjectId TEditor::GetUuid(
    std::source_location location) const
{
    return MetaEtc().Load(location).uuid();
}

TString TEditor::GetName(
    std::source_location location) const
{
    return MetaEtc().Load(location).name();
}

////////////////////////////////////////////////////////////////////////////////

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TEmployer, TString>
TEmployer::EmailDescriptor{
    &EmployersTable.Fields.MetaEmail,
    [] (TEmployer* obj) { return &obj->Email(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TEmployer, ui64>
TEmployer::FinalizationStartTimeDescriptor{
    &EmployersTable.Fields.MetaFinalizationStartTime,
    [] (TEmployer* obj) { return &obj->FinalizationStartTime(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TEmployer, THashMap<TString, NYT::NOrm::NDataModel::TFinalizer>>
TEmployer::FinalizersDescriptor{
    &EmployersTable.Fields.MetaFinalizers,
    [] (TEmployer* obj) { return &obj->Finalizers(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TEmployer, TEmployer::TMetaEtc>
TEmployer::MetaEtcDescriptor{
    &EmployersTable.Fields.MetaEtc,
    [] (TEmployer* object) { return &object->MetaEtc(); }
};

////////////////////////////////////////////////////////////////////////////////

TEmployer::TSpec::TSpec(TEmployer* object)
    : Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TEmployer, TEmployer::TSpec::TEtc>
TEmployer::TSpec::EtcDescriptor{
    &EmployersTable.Fields.Spec,
    [] (TEmployer* obj) { return &obj->Spec().Etc(); }
};

TEmployer::TStatus::TStatus(TEmployer* object)
    : Salary_(object, &SalaryDescriptor)
    , Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TEmployer, i64>
TEmployer::TStatus::SalaryDescriptor{
    &EmployersTable.Fields.StatusSalary,
    [] (TEmployer* obj) { return &obj->Status().Salary(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TEmployer, TEmployer::TStatus::TEtc>
TEmployer::TStatus::EtcDescriptor{
    &EmployersTable.Fields.Status,
    [] (TEmployer* obj) { return &obj->Status().Etc(); }
};

////////////////////////////////////////////////////////////////////////////////

TEmployer::TEmployer(
    const TString& id,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TDataModelObject(typeHandler, session)
    , Id_(id)
    , Email_(this, &EmailDescriptor)
    , FinalizationStartTime_(this, &FinalizationStartTimeDescriptor)
    , Finalizers_(this, &FinalizersDescriptor)
    , MetaEtc_(this, &MetaEtcDescriptor)
    , Spec_(this)
    , Status_(this)
{
}

////////////////////////////////////////////////////////////////////////////////

NYT::NOrm::NClient::NObjects::TObjectKey TEmployer::GetKey() const
{
    return NYT::NOrm::NServer::NObjects::TObjectKey(
        GetId());
}

void TEmployer::ScheduleUuidLoad() const
{
    MetaEtc().ScheduleLoad();
}

NYT::NOrm::NClient::NObjects::TObjectId TEmployer::GetUuid(
    std::source_location location) const
{
    return MetaEtc().Load(location).uuid();
}

TString TEmployer::GetName(
    std::source_location location) const
{
    return MetaEtc().Load(location).name();
}

////////////////////////////////////////////////////////////////////////////////

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TExecutor, ui64>
TExecutor::FinalizationStartTimeDescriptor{
    &ExecutorsTable.Fields.MetaFinalizationStartTime,
    [] (TExecutor* obj) { return &obj->FinalizationStartTime(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TExecutor, THashMap<TString, NYT::NOrm::NDataModel::TFinalizer>>
TExecutor::FinalizersDescriptor{
    &ExecutorsTable.Fields.MetaFinalizers,
    [] (TExecutor* obj) { return &obj->Finalizers(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TExecutor, TExecutor::TMetaEtc>
TExecutor::MetaEtcDescriptor{
    &ExecutorsTable.Fields.MetaEtc,
    [] (TExecutor* object) { return &object->MetaEtc(); }
};

////////////////////////////////////////////////////////////////////////////////

TExecutor::TSpec::TSpec(TExecutor* object)
    : Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TExecutor, TExecutor::TSpec::TEtc>
TExecutor::TSpec::EtcDescriptor{
    &ExecutorsTable.Fields.SpecEtc,
    [] (TExecutor* obj) { return &obj->Spec().Etc(); }
};

TExecutor::TStatus::TStatus(TExecutor* object)
    : MotherShips_(object, &MotherShipsDescriptor)
    , Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TOneToManyAttributeDescriptor<NYT::NOrm::NExample::NServer::NLibrary::TExecutor, NYT::NOrm::NExample::NServer::NLibrary::TMotherShip> TExecutor::TStatus::MotherShipsDescriptor {
    &ExecutorToMotherShipsTable,
    { &ExecutorToMotherShipsTable.Fields.ExecutorId },
    { &ExecutorToMotherShipsTable.Fields.MotherShipId },
    [] (NYT::NOrm::NExample::NServer::NLibrary::TExecutor* obj) { return &obj->Status().MotherShips(); },
    [] (NYT::NOrm::NExample::NServer::NLibrary::TMotherShip* obj) { return &obj->Spec().ExecutorId(); },
    /*foreignObjectTableKey*/ false,
    /*forbidNonEmptyRemoval*/ false
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TExecutor, TExecutor::TStatus::TEtc>
TExecutor::TStatus::EtcDescriptor{
    &ExecutorsTable.Fields.StatusEtc,
    [] (TExecutor* obj) { return &obj->Status().Etc(); }
};

////////////////////////////////////////////////////////////////////////////////

TExecutor::TExecutor(
    const i64& id,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TDataModelObject(typeHandler, session)
    , Id_(id)
    , FinalizationStartTime_(this, &FinalizationStartTimeDescriptor)
    , Finalizers_(this, &FinalizersDescriptor)
    , MetaEtc_(this, &MetaEtcDescriptor)
    , Spec_(this)
    , Status_(this)
{
}

////////////////////////////////////////////////////////////////////////////////

NYT::NOrm::NClient::NObjects::TObjectKey TExecutor::GetKey() const
{
    return NYT::NOrm::NServer::NObjects::TObjectKey(
        GetId());
}

void TExecutor::ScheduleUuidLoad() const
{
    MetaEtc().ScheduleLoad();
}

NYT::NOrm::NClient::NObjects::TObjectId TExecutor::GetUuid(
    std::source_location location) const
{
    return MetaEtc().Load(location).uuid();
}

TString TExecutor::GetName(
    std::source_location location) const
{
    return MetaEtc().Load(location).name();
}

////////////////////////////////////////////////////////////////////////////////

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TGenre, ui64>
TGenre::FinalizationStartTimeDescriptor{
    &GenresTable.Fields.MetaFinalizationStartTime,
    [] (TGenre* obj) { return &obj->FinalizationStartTime(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TGenre, THashMap<TString, NYT::NOrm::NDataModel::TFinalizer>>
TGenre::FinalizersDescriptor{
    &GenresTable.Fields.MetaFinalizers,
    [] (TGenre* obj) { return &obj->Finalizers(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TGenre, TGenre::TMetaEtc>
TGenre::MetaEtcDescriptor{
    &GenresTable.Fields.MetaEtc,
    [] (TGenre* object) { return &object->MetaEtc(); }
};

////////////////////////////////////////////////////////////////////////////////

TGenre::TSpec::TSpec(TGenre* object)
    : Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TGenre, TGenre::TSpec::TEtc>
TGenre::TSpec::EtcDescriptor{
    &GenresTable.Fields.SpecEtc,
    [] (TGenre* obj) { return &obj->Spec().Etc(); }
};

TGenre::TStatus::TStatus(TGenre* object)
    : Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TGenre, TGenre::TStatus::TEtc>
TGenre::TStatus::EtcDescriptor{
    &GenresTable.Fields.StatusEtc,
    [] (TGenre* obj) { return &obj->Status().Etc(); }
};

////////////////////////////////////////////////////////////////////////////////

TGenre::TGenre(
    const TString& id,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TDataModelObject(typeHandler, session)
    , Id_(id)
    , FinalizationStartTime_(this, &FinalizationStartTimeDescriptor)
    , Finalizers_(this, &FinalizersDescriptor)
    , MetaEtc_(this, &MetaEtcDescriptor)
    , Spec_(this)
    , Status_(this)
{
    RegisterScalarAttributeIndex(
        "genre_by_name",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndex>(
            typeHandler->GetIndexDescriptorOrThrow("genre_by_name"),
            this,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
                NYT::NOrm::NServer::NObjects::TIndexAttribute(
                    &Spec().Etc(),
                    "/name",
                    /*repeated*/ false),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
            },
            /*unique*/ true));
}

////////////////////////////////////////////////////////////////////////////////

NYT::NOrm::NClient::NObjects::TObjectKey TGenre::GetKey() const
{
    return NYT::NOrm::NServer::NObjects::TObjectKey(
        GetId());
}

void TGenre::ScheduleUuidLoad() const
{
    MetaEtc().ScheduleLoad();
}

NYT::NOrm::NClient::NObjects::TObjectId TGenre::GetUuid(
    std::source_location location) const
{
    return MetaEtc().Load(location).uuid();
}

TString TGenre::GetName(
    std::source_location location) const
{
    return MetaEtc().Load(location).name();
}

////////////////////////////////////////////////////////////////////////////////

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TGroup, ui64>
TGroup::FinalizationStartTimeDescriptor{
    &GroupsTable.Fields.MetaFinalizationStartTime,
    [] (TGroup* obj) { return &obj->FinalizationStartTime(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TGroup, THashMap<TString, NYT::NOrm::NDataModel::TFinalizer>>
TGroup::FinalizersDescriptor{
    &GroupsTable.Fields.MetaFinalizers,
    [] (TGroup* obj) { return &obj->Finalizers(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TGroup, TGroup::TMetaEtc>
TGroup::MetaEtcDescriptor{
    &GroupsTable.Fields.MetaEtc,
    [] (TGroup* object) { return &object->MetaEtc(); }
};

////////////////////////////////////////////////////////////////////////////////

TGroup::TSpec::TSpec(TGroup* object)
    : Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TGroup, TGroup::TSpec::TEtc>
TGroup::TSpec::EtcDescriptor{
    &GroupsTable.Fields.Spec,
    [] (TGroup* obj) { return &obj->Spec().Etc(); }
};

TGroup::TStatus::TStatus(TGroup* object)
    : Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TGroup, TGroup::TStatus::TEtc>
TGroup::TStatus::EtcDescriptor{
    &GroupsTable.Fields.StatusEtc,
    [] (TGroup* obj) { return &obj->Status().Etc(); }
};

////////////////////////////////////////////////////////////////////////////////

TGroup::TGroup(
    const TString& id,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TDataModelObject(typeHandler, session)
    , Id_(id)
    , FinalizationStartTime_(this, &FinalizationStartTimeDescriptor)
    , Finalizers_(this, &FinalizersDescriptor)
    , MetaEtc_(this, &MetaEtcDescriptor)
    , Spec_(this)
    , Status_(this)
{
}

////////////////////////////////////////////////////////////////////////////////

NYT::NOrm::NClient::NObjects::TObjectKey TGroup::GetKey() const
{
    return NYT::NOrm::NServer::NObjects::TObjectKey(
        GetId());
}

void TGroup::ScheduleUuidLoad() const
{
    MetaEtc().ScheduleLoad();
}

NYT::NOrm::NClient::NObjects::TObjectId TGroup::GetUuid(
    std::source_location location) const
{
    return MetaEtc().Load(location).uuid();
}

TString TGroup::GetName(
    std::source_location location) const
{
    return MetaEtc().Load(location).name();
}

////////////////////////////////////////////////////////////////////////////////

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<THitchhiker, i64>
THitchhiker::FormativeBookIdDescriptor{
    &HitchhikersTable.Fields.MetaFormativeBookId,
    [] (THitchhiker* obj) { return &obj->FormativeBookId(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<THitchhiker, i64>
THitchhiker::FormativeBookId2Descriptor{
    &HitchhikersTable.Fields.MetaFormativeBookId2,
    [] (THitchhiker* obj) { return &obj->FormativeBookId2(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<THitchhiker, ui64>
THitchhiker::FinalizationStartTimeDescriptor{
    &HitchhikersTable.Fields.MetaFinalizationStartTime,
    [] (THitchhiker* obj) { return &obj->FinalizationStartTime(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<THitchhiker, THashMap<TString, NYT::NOrm::NDataModel::TFinalizer>>
THitchhiker::FinalizersDescriptor{
    &HitchhikersTable.Fields.MetaFinalizers,
    [] (THitchhiker* obj) { return &obj->Finalizers(); }
};

const NYT::NOrm::NServer::NObjects::TSingleReferenceDescriptor<THitchhiker, TBook> THitchhiker::FormativeBookDescriptor {
    .ForwardAttributeGetter = [] (THitchhiker* obj) { return &obj->FormativeBook(); },
    .InverseAttributeGetter = [] (TBook* obj) { return &obj->Status().FormedHitchhikers(); },
    .KeyStorageDescriptor = NYT::NOrm::NServer::NObjects::TColumnarKeyStorageDescriptor{
        .KeyLocators = { { &THitchhiker::FormativeBookIdDescriptor, "" }, { &THitchhiker::FormativeBookId2Descriptor, "" } },
    },
};

const NYT::NOrm::NServer::NObjects::TSingleViewDescriptor<THitchhiker, TBook> THitchhiker::FormativeBookViewDescriptor {
    .ReferenceDescriptor = THitchhiker::FormativeBookDescriptor,
    .ViewAttributeGetter = [] (THitchhiker* obj) { return &obj->FormativeBookView(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<THitchhiker, THitchhiker::TMetaEtc>
THitchhiker::MetaEtcDescriptor{
    &HitchhikersTable.Fields.MetaEtc,
    [] (THitchhiker* object) { return &object->MetaEtc(); }
};

////////////////////////////////////////////////////////////////////////////////

THitchhiker::TSpec::TSpec(THitchhiker* object)
    : HatedBooks_(object, &HatedBooksDescriptor)
    , Books_(
        object,
        BooksDescriptor,
        std::make_unique<NYT::NOrm::NServer::NObjects::TProtoMultiKeyStorageDriver>(object, BooksDescriptor.KeyStorageDescriptor))
    , BooksView_(object)
    , Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<THitchhiker, std::vector<NYT::NOrm::NExample::NClient::NProto::NDataModel::THitchhikerSpec::TBookId>>
THitchhiker::TSpec::HatedBooksDescriptor{
    &HitchhikersTable.Fields.SpecHatedBooks,
    [] (THitchhiker* obj) { return &obj->Spec().HatedBooks(); }
};

const NYT::NOrm::NServer::NObjects::TMultiReferenceDescriptor<THitchhiker, TBook> THitchhiker::TSpec::BooksDescriptor {
    .ForwardAttributeGetter = [] (THitchhiker* obj) { return &obj->Spec().Books(); },
    .InverseAttributeGetter = [] (TBook* obj) { return &obj->Status().Hitchhikers(); },
    .KeyStorageDescriptor = NYT::NOrm::NServer::NObjects::TProtoKeyStorageDescriptor{
        .KeyLocators = { { &THitchhiker::TSpec::EtcDescriptor, { "/favorite_book/id", "/favorite_book/id2" } }, { &THitchhiker::TSpec::HatedBooksDescriptor, { "/*/id", "/*/id2" } } },
    },
};

const NYT::NOrm::NServer::NObjects::TMultiViewDescriptor<THitchhiker, TBook> THitchhiker::TSpec::BooksViewDescriptor {
    .ReferenceDescriptor = THitchhiker::TSpec::BooksDescriptor,
    .ViewAttributeGetter = [] (THitchhiker* obj) { return &obj->Spec().BooksView(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<THitchhiker, THitchhiker::TSpec::TEtc>
THitchhiker::TSpec::EtcDescriptor{
    &HitchhikersTable.Fields.SpecEtc,
    [] (THitchhiker* obj) { return &obj->Spec().Etc(); }
};

THitchhiker::TStatus::TStatus(THitchhiker* object)
    : Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<THitchhiker, THitchhiker::TStatus::TEtc>
THitchhiker::TStatus::EtcDescriptor{
    &HitchhikersTable.Fields.StatusEtc,
    [] (THitchhiker* obj) { return &obj->Status().Etc(); }
};

////////////////////////////////////////////////////////////////////////////////

THitchhiker::THitchhiker(
    const i64& id,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TDataModelObject(typeHandler, session)
    , Id_(id)
    , FormativeBookId_(this, &FormativeBookIdDescriptor)
    , FormativeBookId2_(this, &FormativeBookId2Descriptor)
    , FinalizationStartTime_(this, &FinalizationStartTimeDescriptor)
    , Finalizers_(this, &FinalizersDescriptor)
    , FormativeBook_(
        this,
        FormativeBookDescriptor,
        std::make_unique<NYT::NOrm::NServer::NObjects::TColumnarSingleKeyStorageDriver>(this, FormativeBookDescriptor.KeyStorageDescriptor))
    , FormativeBookView_(this)
    , MetaEtc_(this, &MetaEtcDescriptor)
    , Spec_(this)
    , Status_(this)
{
}

////////////////////////////////////////////////////////////////////////////////

NYT::NOrm::NClient::NObjects::TObjectKey THitchhiker::GetKey() const
{
    return NYT::NOrm::NServer::NObjects::TObjectKey(
        GetId());
}

void THitchhiker::ScheduleUuidLoad() const
{
    MetaEtc().ScheduleLoad();
}

NYT::NOrm::NClient::NObjects::TObjectId THitchhiker::GetUuid(
    std::source_location location) const
{
    return MetaEtc().Load(location).uuid();
}

TString THitchhiker::GetName(
    std::source_location location) const
{
    return MetaEtc().Load(location).name();
}

////////////////////////////////////////////////////////////////////////////////

const NYT::NOrm::NServer::NObjects::TManyToOneAttributeDescriptor<NYT::NOrm::NExample::NServer::NLibrary::TIllustrator, NYT::NOrm::NExample::NServer::NLibrary::TPublisher>
    TIllustrator::PartTimeJobDescriptor {
        &IllustratorsTable.Fields.MetaPartTimeJob,
        [] (NYT::NOrm::NExample::NServer::NLibrary::TIllustrator* v) { return &v->PartTimeJob(); },
        [] (NYT::NOrm::NExample::NServer::NLibrary::TPublisher* v) { return &v->Status().Illustrators(); },
        /*forbidNonEmptyRemoval*/ false
    };

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TIllustrator, ui64>
TIllustrator::FinalizationStartTimeDescriptor{
    &IllustratorsTable.Fields.MetaFinalizationStartTime,
    [] (TIllustrator* obj) { return &obj->FinalizationStartTime(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TIllustrator, THashMap<TString, NYT::NOrm::NDataModel::TFinalizer>>
TIllustrator::FinalizersDescriptor{
    &IllustratorsTable.Fields.MetaFinalizers,
    [] (TIllustrator* obj) { return &obj->Finalizers(); }
};

const NYT::NOrm::NServer::NObjects::TManyToOneViewAttributeDescriptor<NYT::NOrm::NExample::NServer::NLibrary::TIllustrator, NYT::NOrm::NExample::NServer::NLibrary::TPublisher> TIllustrator::PartTimeJobViewDescriptor {
    TIllustrator::PartTimeJobDescriptor,
    [] (const TIllustrator* obj) { return &obj->PartTimeJobView(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TIllustrator, TIllustrator::TMetaEtc>
TIllustrator::MetaEtcDescriptor{
    &IllustratorsTable.Fields.MetaEtc,
    [] (TIllustrator* object) { return &object->MetaEtc(); }
};

////////////////////////////////////////////////////////////////////////////////

TIllustrator::TSpec::TSpec(TIllustrator* object)
    : Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TIllustrator, TIllustrator::TSpec::TEtc>
TIllustrator::TSpec::EtcDescriptor{
    &IllustratorsTable.Fields.Spec,
    [] (TIllustrator* obj) { return &obj->Spec().Etc(); }
};

TIllustrator::TStatus::TStatus(TIllustrator* object)
    : Books_(object, &BooksDescriptor)
    , BooksForCoverIllustrator_(object, &BooksForCoverIllustratorDescriptor)
    , Publishers_(object, &PublishersDescriptor)
    , PublishersForOldFeaturedIllustrators_(object, &PublishersForOldFeaturedIllustratorsDescriptor)
    , PublishersForNewFeaturedIllustrators_(object, &PublishersForNewFeaturedIllustratorsDescriptor)
    , Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TOneToManyAttributeDescriptor<NYT::NOrm::NExample::NServer::NLibrary::TIllustrator, NYT::NOrm::NExample::NServer::NLibrary::TBook> TIllustrator::TStatus::BooksDescriptor {
    &IllustratorToBooksTable,
    { &IllustratorToBooksTable.Fields.IllustratorUid },
    { &IllustratorToBooksTable.Fields.BookId, &IllustratorToBooksTable.Fields.BookId2 },
    [] (NYT::NOrm::NExample::NServer::NLibrary::TIllustrator* obj) { return &obj->Status().Books(); },
    [] (NYT::NOrm::NExample::NServer::NLibrary::TBook* obj) { return &obj->Spec().IllustratorId(); },
    /*foreignObjectTableKey*/ false,
    /*forbidNonEmptyRemoval*/ false
};

const NYT::NOrm::NServer::NObjects::TOneToManyAttributeDescriptor<NYT::NOrm::NExample::NServer::NLibrary::TIllustrator, NYT::NOrm::NExample::NServer::NLibrary::TBook> TIllustrator::TStatus::BooksForCoverIllustratorDescriptor {
    &IllustratorToBooksForCoverIllustratorTable,
    { &IllustratorToBooksForCoverIllustratorTable.Fields.IllustratorUid },
    { &IllustratorToBooksForCoverIllustratorTable.Fields.BookId, &IllustratorToBooksForCoverIllustratorTable.Fields.BookId2 },
    [] (NYT::NOrm::NExample::NServer::NLibrary::TIllustrator* obj) { return &obj->Status().BooksForCoverIllustrator(); },
    [] (NYT::NOrm::NExample::NServer::NLibrary::TBook* obj) { return &obj->Spec().CoverIllustratorId(); },
    /*foreignObjectTableKey*/ false,
    /*forbidNonEmptyRemoval*/ false
};

const NYT::NOrm::NServer::NObjects::TOneToManyAttributeDescriptor<NYT::NOrm::NExample::NServer::NLibrary::TIllustrator, NYT::NOrm::NExample::NServer::NLibrary::TPublisher> TIllustrator::TStatus::PublishersDescriptor {
    &IllustratorToPublishersTable,
    { &IllustratorToPublishersTable.Fields.IllustratorUid },
    { &IllustratorToPublishersTable.Fields.PublisherId },
    [] (NYT::NOrm::NExample::NServer::NLibrary::TIllustrator* obj) { return &obj->Status().Publishers(); },
    [] (NYT::NOrm::NExample::NServer::NLibrary::TPublisher* obj) { return &obj->Spec().IllustratorInChief(); },
    /*foreignObjectTableKey*/ false,
    /*forbidNonEmptyRemoval*/ false
};

const NYT::NOrm::NServer::NObjects::TManyToManyTabularAttributeDescriptor<NYT::NOrm::NExample::NServer::NLibrary::TIllustrator, NYT::NOrm::NExample::NServer::NLibrary::TPublisher> TIllustrator::TStatus::PublishersForOldFeaturedIllustratorsDescriptor {
    &IllustratorsToPublishersForOldFeaturedIllustratorsTable,
    { &IllustratorsToPublishersForOldFeaturedIllustratorsTable.Fields.IllustratorUid },
    { &IllustratorsToPublishersForOldFeaturedIllustratorsTable.Fields.PublisherId },
    [] (NYT::NOrm::NExample::NServer::NLibrary::TIllustrator* obj) { return &obj->Status().PublishersForOldFeaturedIllustrators(); },
    [] (NYT::NOrm::NExample::NServer::NLibrary::TPublisher* obj) { return &obj->Spec().FeaturedIllustrators(); },
    /*foreignObjectTableKey*/ false,
    /*forbidNonEmptyRemoval*/ false
};

const NYT::NOrm::NServer::NObjects::TManyToManyTabularAttributeDescriptor<NYT::NOrm::NExample::NServer::NLibrary::TIllustrator, NYT::NOrm::NExample::NServer::NLibrary::TPublisher> TIllustrator::TStatus::PublishersForNewFeaturedIllustratorsDescriptor {
    &IllustratorsToPublishersForNewFeaturedIllustratorsTable,
    { &IllustratorsToPublishersForNewFeaturedIllustratorsTable.Fields.IllustratorUid },
    { &IllustratorsToPublishersForNewFeaturedIllustratorsTable.Fields.PublisherId },
    [] (NYT::NOrm::NExample::NServer::NLibrary::TIllustrator* obj) { return &obj->Status().PublishersForNewFeaturedIllustrators(); },
    [] (NYT::NOrm::NExample::NServer::NLibrary::TPublisher* obj) { return &obj->Status().FeaturedIllustrators(); },
    /*foreignObjectTableKey*/ false,
    /*forbidNonEmptyRemoval*/ false
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TIllustrator, TIllustrator::TStatus::TEtc>
TIllustrator::TStatus::EtcDescriptor{
    &IllustratorsTable.Fields.StatusEtc,
    [] (TIllustrator* obj) { return &obj->Status().Etc(); }
};

////////////////////////////////////////////////////////////////////////////////

TIllustrator::TIllustrator(
    const i64& uid,
    const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TDataModelObject(typeHandler, session)
    , Uid_(uid)
    , ParentKeyAttribute_(this, parentKey)
    , Publisher_(this)
    , PartTimeJob_(this, &PartTimeJobDescriptor)
    , FinalizationStartTime_(this, &FinalizationStartTimeDescriptor)
    , Finalizers_(this, &FinalizersDescriptor)
    , PartTimeJobView_(this)
    , MetaEtc_(this, &MetaEtcDescriptor)
    , Spec_(this)
    , Status_(this)
{
}

////////////////////////////////////////////////////////////////////////////////

NYT::NOrm::NClient::NObjects::TObjectKey TIllustrator::GetKey() const
{
    return NYT::NOrm::NServer::NObjects::TObjectKey(
        GetUid());
}

NYT::NOrm::NClient::NObjects::TObjectKey TIllustrator::GetParentKey(
    std::source_location location) const
{
    return ParentKeyAttribute_.GetKey(location);
}

NYT::NOrm::NServer::NObjects::TParentKeyAttribute* TIllustrator::GetParentKeyAttribute()
{
    return &ParentKeyAttribute_;
}

i64 TIllustrator::PublisherId(std::source_location location) const
{
    return GetParentKey(location).GetWithDefault<i64>(0);
}

void TIllustrator::ScheduleUuidLoad() const
{
    MetaEtc().ScheduleLoad();
}

NYT::NOrm::NClient::NObjects::TObjectId TIllustrator::GetUuid(
    std::source_location location) const
{
    return MetaEtc().Load(location).uuid();
}

TString TIllustrator::GetName(
    std::source_location location) const
{
    return MetaEtc().Load(location).name();
}

////////////////////////////////////////////////////////////////////////////////

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TIndexedIncrementId, ui64>
TIndexedIncrementId::FinalizationStartTimeDescriptor{
    &IndexedIncrementIdsTable.Fields.MetaFinalizationStartTime,
    [] (TIndexedIncrementId* obj) { return &obj->FinalizationStartTime(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TIndexedIncrementId, THashMap<TString, NYT::NOrm::NDataModel::TFinalizer>>
TIndexedIncrementId::FinalizersDescriptor{
    &IndexedIncrementIdsTable.Fields.MetaFinalizers,
    [] (TIndexedIncrementId* obj) { return &obj->Finalizers(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TIndexedIncrementId, TIndexedIncrementId::TMetaEtc>
TIndexedIncrementId::MetaEtcDescriptor{
    &IndexedIncrementIdsTable.Fields.MetaEtc,
    [] (TIndexedIncrementId* object) { return &object->MetaEtc(); }
};

////////////////////////////////////////////////////////////////////////////////

TIndexedIncrementId::TSpec::TSpec(TIndexedIncrementId* object)
    : Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TIndexedIncrementId, TIndexedIncrementId::TSpec::TEtc>
TIndexedIncrementId::TSpec::EtcDescriptor{
    &IndexedIncrementIdsTable.Fields.SpecEtc,
    [] (TIndexedIncrementId* obj) { return &obj->Spec().Etc(); }
};

TIndexedIncrementId::TStatus::TStatus(TIndexedIncrementId* object)
    : Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TIndexedIncrementId, TIndexedIncrementId::TStatus::TEtc>
TIndexedIncrementId::TStatus::EtcDescriptor{
    &IndexedIncrementIdsTable.Fields.StatusEtc,
    [] (TIndexedIncrementId* obj) { return &obj->Status().Etc(); }
};

////////////////////////////////////////////////////////////////////////////////

TIndexedIncrementId::TIndexedIncrementId(
    const i64& i64Id,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TDataModelObject(typeHandler, session)
    , I64Id_(i64Id)
    , FinalizationStartTime_(this, &FinalizationStartTimeDescriptor)
    , Finalizers_(this, &FinalizersDescriptor)
    , MetaEtc_(this, &MetaEtcDescriptor)
    , Spec_(this)
    , Status_(this)
{
}

////////////////////////////////////////////////////////////////////////////////

NYT::NOrm::NClient::NObjects::TObjectKey TIndexedIncrementId::GetKey() const
{
    return NYT::NOrm::NServer::NObjects::TObjectKey(
        GetI64Id());
}

void TIndexedIncrementId::ScheduleUuidLoad() const
{
    MetaEtc().ScheduleLoad();
}

NYT::NOrm::NClient::NObjects::TObjectId TIndexedIncrementId::GetUuid(
    std::source_location location) const
{
    return MetaEtc().Load(location).uuid();
}

TString TIndexedIncrementId::GetName(
    std::source_location location) const
{
    return MetaEtc().Load(location).name();
}

////////////////////////////////////////////////////////////////////////////////

const NYT::NOrm::NServer::NObjects::TOneTransitiveAttributeDescriptor<TInterceptor, TNexus> TInterceptor::NexusIdDescriptor {
    &InterceptorsTable.Fields.MetaNexusId,
    [] (TInterceptor* interceptor) {
        return &interceptor->NexusId();
    },
    [] (TInterceptor* interceptor) {
        auto setter = [interceptor] (TNexus* nexus) {
            interceptor->NexusId().StoreInitial(nexus);
        };
        interceptor->GetParentKeyAttribute()->ScheduleParentLoad();
        interceptor->GetSession()->ScheduleLoad([interceptor, setter=std::move(setter)] (auto*) {
            if (interceptor->IsRemoved()) {
                return;
            }
            auto* motherShip = interceptor
                ->GetTypeHandler()
                ->GetParent(interceptor)
                ->As<TMotherShip>();
            motherShip->GetParentKeyAttribute()->ScheduleParentLoad();
            motherShip->GetSession()->ScheduleLoad([motherShip, setter=std::move(setter)] (auto*) {
                if (motherShip->IsRemoved()) {
                    return;
                }
                auto* nexus = motherShip
                    ->GetTypeHandler()
                    ->GetParent(motherShip)
                    ->As<TNexus>();
                setter(nexus);
            });
        });
    }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TInterceptor, ui64>
TInterceptor::FinalizationStartTimeDescriptor{
    &InterceptorsTable.Fields.MetaFinalizationStartTime,
    [] (TInterceptor* obj) { return &obj->FinalizationStartTime(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TInterceptor, THashMap<TString, NYT::NOrm::NDataModel::TFinalizer>>
TInterceptor::FinalizersDescriptor{
    &InterceptorsTable.Fields.MetaFinalizers,
    [] (TInterceptor* obj) { return &obj->Finalizers(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TInterceptor, TInterceptor::TMetaEtc>
TInterceptor::MetaEtcDescriptor{
    &InterceptorsTable.Fields.MetaEtc,
    [] (TInterceptor* object) { return &object->MetaEtc(); }
};

////////////////////////////////////////////////////////////////////////////////

TInterceptor::TSpec::TSpec(TInterceptor* object)
    : Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TInterceptor, TInterceptor::TSpec::TEtc>
TInterceptor::TSpec::EtcDescriptor{
    &InterceptorsTable.Fields.SpecEtc,
    [] (TInterceptor* obj) { return &obj->Spec().Etc(); }
};

TInterceptor::TStatus::TStatus(TInterceptor* object)
    : Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TInterceptor, TInterceptor::TStatus::TEtc>
TInterceptor::TStatus::EtcDescriptor{
    &InterceptorsTable.Fields.StatusEtc,
    [] (TInterceptor* obj) { return &obj->Status().Etc(); }
};

////////////////////////////////////////////////////////////////////////////////

TInterceptor::TInterceptor(
    const i64& id,
    const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TDataModelObject(typeHandler, session)
    , Id_(id)
    , ParentKeyAttribute_(this, parentKey)
    , MotherShip_(this)
    , NexusId_(this, &NexusIdDescriptor)
    , FinalizationStartTime_(this, &FinalizationStartTimeDescriptor)
    , Finalizers_(this, &FinalizersDescriptor)
    , MetaEtc_(this, &MetaEtcDescriptor)
    , Spec_(this)
    , Status_(this)
{
}

////////////////////////////////////////////////////////////////////////////////

NYT::NOrm::NClient::NObjects::TObjectKey TInterceptor::GetKey() const
{
    return NYT::NOrm::NServer::NObjects::TObjectKey(
        GetId());
}

NYT::NOrm::NClient::NObjects::TObjectKey TInterceptor::GetParentKey(
    std::source_location location) const
{
    return ParentKeyAttribute_.GetKey(location);
}

NYT::NOrm::NServer::NObjects::TParentKeyAttribute* TInterceptor::GetParentKeyAttribute()
{
    return &ParentKeyAttribute_;
}

i64 TInterceptor::MotherShipId(std::source_location location) const
{
    return GetParentKey(location).GetWithDefault<i64>(0);
}

void TInterceptor::ScheduleUuidLoad() const
{
    MetaEtc().ScheduleLoad();
}

NYT::NOrm::NClient::NObjects::TObjectId TInterceptor::GetUuid(
    std::source_location location) const
{
    return MetaEtc().Load(location).uuid();
}

TString TInterceptor::GetName(
    std::source_location location) const
{
    return MetaEtc().Load(location).name();
}

////////////////////////////////////////////////////////////////////////////////

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TManualId, ui64>
TManualId::FinalizationStartTimeDescriptor{
    &ManualIdsTable.Fields.MetaFinalizationStartTime,
    [] (TManualId* obj) { return &obj->FinalizationStartTime(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TManualId, THashMap<TString, NYT::NOrm::NDataModel::TFinalizer>>
TManualId::FinalizersDescriptor{
    &ManualIdsTable.Fields.MetaFinalizers,
    [] (TManualId* obj) { return &obj->Finalizers(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TManualId, TManualId::TMetaEtc>
TManualId::MetaEtcDescriptor{
    &ManualIdsTable.Fields.MetaEtc,
    [] (TManualId* object) { return &object->MetaEtc(); }
};

////////////////////////////////////////////////////////////////////////////////

TManualId::TSpec::TSpec(TManualId* object)
    : StrValue_(object, &StrValueDescriptor)
    , I32Value_(object, &I32ValueDescriptor)
    , Ui32Value_(object, &Ui32ValueDescriptor)
    , Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TManualId, TString>
TManualId::TSpec::StrValueDescriptor{
    &ManualIdsTable.Fields.SpecStrValue,
    [] (TManualId* obj) { return &obj->Spec().StrValue(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TManualId, i32>
TManualId::TSpec::I32ValueDescriptor{
    &ManualIdsTable.Fields.SpecI32Value,
    [] (TManualId* obj) { return &obj->Spec().I32Value(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TManualId, ui32>
TManualId::TSpec::Ui32ValueDescriptor{
    &ManualIdsTable.Fields.SpecUi32Value,
    [] (TManualId* obj) { return &obj->Spec().Ui32Value(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TManualId, TManualId::TSpec::TEtc>
TManualId::TSpec::EtcDescriptor{
    &ManualIdsTable.Fields.SpecEtc,
    [] (TManualId* obj) { return &obj->Spec().Etc(); }
};

TManualId::TStatus::TStatus(TManualId* object)
    : Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TManualId, TManualId::TStatus::TEtc>
TManualId::TStatus::EtcDescriptor{
    &ManualIdsTable.Fields.StatusEtc,
    [] (TManualId* obj) { return &obj->Status().Etc(); }
};

////////////////////////////////////////////////////////////////////////////////

TManualId::TManualId(
    const TString& strId,
    const i64& i64Id,
    const ui64& ui64Id,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TDataModelObject(typeHandler, session)
    , StrId_(strId)
    , I64Id_(i64Id)
    , Ui64Id_(ui64Id)
    , FinalizationStartTime_(this, &FinalizationStartTimeDescriptor)
    , Finalizers_(this, &FinalizersDescriptor)
    , MetaEtc_(this, &MetaEtcDescriptor)
    , Spec_(this)
    , Status_(this)
{
}

////////////////////////////////////////////////////////////////////////////////

NYT::NOrm::NClient::NObjects::TObjectKey TManualId::GetKey() const
{
    return NYT::NOrm::NServer::NObjects::TObjectKey(
        GetStrId(),
        GetI64Id(),
        GetUi64Id());
}

void TManualId::ScheduleUuidLoad() const
{
    MetaEtc().ScheduleLoad();
}

NYT::NOrm::NClient::NObjects::TObjectId TManualId::GetUuid(
    std::source_location location) const
{
    return MetaEtc().Load(location).uuid();
}

TString TManualId::GetName(
    std::source_location location) const
{
    return MetaEtc().Load(location).name();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NExample::NServer::NLibrary
