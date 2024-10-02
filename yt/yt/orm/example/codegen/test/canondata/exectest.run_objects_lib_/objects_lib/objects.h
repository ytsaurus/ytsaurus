// AUTOMATICALLY GENERATED. DO NOT EDIT!

#pragma once

#include "db_schema.h"
#include "object_detail.h"

#include <yt/yt/orm/example/client/proto/data_model/autogen/schema.pb.h>
#include <yt/yt/orm/example/server/proto/autogen/etc.pb.h>

// TODO(bulatman): Include schema_transitive.h instead.

#include <yt_proto/yt/orm/data_model/finalizers.pb.h>
#include <yt_proto/yt/orm/data_model/semaphore.pb.h>
#include <yt_proto/yt/orm/data_model/semaphore_set.pb.h>

#include <yt/yt/orm/server/objects/data_model_object.h>
#include <yt/yt/orm/server/objects/reference_attribute.h>
#include <yt/yt/orm/server/objects/semaphore_detail.h>
#include <yt/yt/orm/server/objects/semaphore_set_detail.h>
#include <yt/yt/orm/library/mpl/types.h>

#include <library/cpp/yt/memory/ref_tracked.h>

#include <util/generic/string.h>
#include <util/system/types.h>

#include <vector>

namespace NYT::NOrm::NExample::NServer::NPlugins {

////////////////////////////////////////////////////////////////////////////////
class TMotherShip;

////////////////////////////////////////////////////////////////////////////////

} // NYT::NOrm::NExample::NServer::NPlugins

// NYT::NOrm::NServer::NObjects namespace is intentional: there are a bunch of template specializations within it (e.g.: `TObjectKeyTraits`),
// which are required by templated classes within the namespace (e.g.: `TAttributeSchema::SetAttribute(TManyToOneAttributeDescriptor)`).
namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

template <>
struct TObjectKeyTraits<NYT::NOrm::NExample::NServer::NLibrary::TAuthor>
{
    using TTypes = std::tuple<
        TString
    >;
};

template <>
struct TObjectKeyTraits<NYT::NOrm::NExample::NServer::NLibrary::TBook>
{
    using TTypes = std::tuple<
        i64,
        i64
    >;
};

template <>
struct TObjectKeyTraits<NYT::NOrm::NExample::NServer::NLibrary::TBufferedTimestampId>
{
    using TTypes = std::tuple<
        i64,
        ui64
    >;
};

template <>
struct TObjectKeyTraits<NYT::NOrm::NExample::NServer::NLibrary::TCat>
{
    using TTypes = std::tuple<
        i64
    >;
};

template <>
struct TObjectKeyTraits<NYT::NOrm::NExample::NServer::NLibrary::TEditor>
{
    using TTypes = std::tuple<
        TString
    >;
};

template <>
struct TObjectKeyTraits<NYT::NOrm::NExample::NServer::NLibrary::TEmployer>
{
    using TTypes = std::tuple<
        TString
    >;
};

template <>
struct TObjectKeyTraits<NYT::NOrm::NExample::NServer::NLibrary::TExecutor>
{
    using TTypes = std::tuple<
        i64
    >;
};

template <>
struct TObjectKeyTraits<NYT::NOrm::NExample::NServer::NLibrary::TGenre>
{
    using TTypes = std::tuple<
        TString
    >;
};

template <>
struct TObjectKeyTraits<NYT::NOrm::NExample::NServer::NLibrary::TGroup>
{
    using TTypes = std::tuple<
        TString
    >;
};

template <>
struct TObjectKeyTraits<NYT::NOrm::NExample::NServer::NLibrary::THitchhiker>
{
    using TTypes = std::tuple<
        i64
    >;
};

template <>
struct TObjectKeyTraits<NYT::NOrm::NExample::NServer::NLibrary::TIllustrator>
{
    using TTypes = std::tuple<
        i64
    >;
};

template <>
struct TObjectKeyTraits<NYT::NOrm::NExample::NServer::NLibrary::TIndexedIncrementId>
{
    using TTypes = std::tuple<
        i64
    >;
};

template <>
struct TObjectKeyTraits<NYT::NOrm::NExample::NServer::NLibrary::TInterceptor>
{
    using TTypes = std::tuple<
        i64
    >;
};

template <>
struct TObjectKeyTraits<NYT::NOrm::NExample::NServer::NLibrary::TManualId>
{
    using TTypes = std::tuple<
        TString,
        i64,
        ui64
    >;
};

template <>
struct TObjectKeyTraits<NYT::NOrm::NExample::NServer::NLibrary::TMotherShip>
{
    using TTypes = std::tuple<
        i64
    >;
};

template <>
struct TObjectKeyTraits<NYT::NOrm::NExample::NServer::NLibrary::TMultipolicyId>
{
    using TTypes = std::tuple<
        TString,
        i64,
        ui64,
        ui64
    >;
};

template <>
struct TObjectKeyTraits<NYT::NOrm::NExample::NServer::NLibrary::TNestedColumns>
{
    using TTypes = std::tuple<
        i64
    >;
};

template <>
struct TObjectKeyTraits<NYT::NOrm::NExample::NServer::NLibrary::TNexus>
{
    using TTypes = std::tuple<
        i64
    >;
};

template <>
struct TObjectKeyTraits<NYT::NOrm::NExample::NServer::NLibrary::TNirvanaDMProcessInstance>
{
    using TTypes = std::tuple<
        TString
    >;
};

template <>
struct TObjectKeyTraits<NYT::NOrm::NExample::NServer::NLibrary::TPublisher>
{
    using TTypes = std::tuple<
        i64
    >;
};

template <>
struct TObjectKeyTraits<NYT::NOrm::NExample::NServer::NLibrary::TRandomId>
{
    using TTypes = std::tuple<
        TString,
        i64,
        ui64
    >;
};

template <>
struct TObjectKeyTraits<NYT::NOrm::NExample::NServer::NLibrary::TSchema>
{
    using TTypes = std::tuple<
        TString
    >;
};

template <>
struct TObjectKeyTraits<NYT::NOrm::NExample::NServer::NLibrary::TSemaphore>
{
    using TTypes = std::tuple<
        TString
    >;
};

template <>
struct TObjectKeyTraits<NYT::NOrm::NExample::NServer::NLibrary::TSemaphoreSet>
{
    using TTypes = std::tuple<
        TString
    >;
};

template <>
struct TObjectKeyTraits<NYT::NOrm::NExample::NServer::NLibrary::TTimestampId>
{
    using TTypes = std::tuple<
        i64,
        ui64
    >;
};

template <>
struct TObjectKeyTraits<NYT::NOrm::NExample::NServer::NLibrary::TTypographer>
{
    using TTypes = std::tuple<
        i64
    >;
};

template <>
struct TObjectKeyTraits<NYT::NOrm::NExample::NServer::NLibrary::TUser>
{
    using TTypes = std::tuple<
        TString
    >;
};

template <>
struct TObjectKeyTraits<NYT::NOrm::NExample::NServer::NLibrary::TWatchLogConsumer>
{
    using TTypes = std::tuple<
        TString
    >;
};

////////////////////////////////////////////////////////////////////////////////

template <>
struct TObjectPluginTraits<NYT::NOrm::NExample::NServer::NLibrary::TMotherShip>
{
    using TType = NYT::NOrm::NExample::NServer::NPlugins::TMotherShip;

    static TType* Downcast(NYT::NOrm::NExample::NServer::NLibrary::TMotherShip* object);
};

template <>
struct TObjectPluginTraits<NYT::NOrm::NExample::NServer::NPlugins::TMotherShip>
{
    static NYT::NOrm::NExample::NServer::NLibrary::TMotherShip* Upcast(
        NYT::NOrm::NExample::NServer::NPlugins::TMotherShip* object);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects

namespace NYT::NOrm::NExample::NServer::NLibrary {

////////////////////////////////////////////////////////////////////////////////

class TAuthor
    : public TDataModelObject
    , public NYT::TRefTracked<TAuthor>
{
public:
    static constexpr NYT::NOrm::NClient::NObjects::TObjectTypeValue Type =
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::Author);

    TAuthor(
        const TString& id,
        NYT::NOrm::NServer::NObjects::IObjectTypeHandler* typeHandler,
        NYT::NOrm::NServer::NObjects::ISession* session);

    virtual NYT::NOrm::NClient::NObjects::TObjectTypeValue GetType() const override
    {
        return Type;
    }

    virtual NYT::NOrm::NClient::NObjects::TObjectKey GetKey() const override;

protected:
    const TString Id_;

public:
    const TString& GetId() const
    {
        return Id_;
    }

    using TFinalizationStartTime_ = ui64;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TAuthor, TFinalizationStartTime_> FinalizationStartTimeDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT_OVERRIDE(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFinalizationStartTime_>, FinalizationStartTime);

    using TFinalizers_ = THashMap<TString, NYT::NOrm::NDataModel::TFinalizer>;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TAuthor, TFinalizers_> FinalizersDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT_OVERRIDE(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFinalizers_>, Finalizers);

    using TMetaEtc = typename NYT::NOrm::NExample::NServer::NProto::NAutogen::TAuthorMetaEtc;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TAuthor, TMetaEtc> MetaEtcDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TMetaEtc>, MetaEtc);

    void ScheduleUuidLoad() const override;
    NYT::NOrm::NClient::NObjects::TObjectId GetUuid(std::source_location location = std::source_location::current()) const override;
    TString GetName(std::source_location location = std::source_location::current()) const override;

    class TSpec
    {
    public:
        explicit TSpec(TAuthor* object);

        using TName_ = TString;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TAuthor, TName_> NameDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TName_>, Name);

        using TAge_ = ui64;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TAuthor, TAge_> AgeDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TAge_>, Age);

        using TEtc = NYT::NOrm::NExample::NServer::NProto::NAutogen::TAuthorSpecEtc;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TAuthor, TEtc> EtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TSpec, Spec);

    class TStatus
    {
    public:
        explicit TStatus(TAuthor* object);

        using TBooksForPeerReviewersAttribute =
            NYT::NOrm::NServer::NObjects::TManyToManyTabularAttribute<NYT::NOrm::NExample::NServer::NLibrary::TAuthor, NYT::NOrm::NExample::NServer::NLibrary::TBook>;
        static const TBooksForPeerReviewersAttribute::TDescriptor BooksForPeerReviewersDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TBooksForPeerReviewersAttribute, BooksForPeerReviewers);

        using TBookRefsAttribute =
            NYT::NOrm::NServer::NObjects::TMultiReferenceAttribute<TAuthor, TBook>;
        static const NYT::NOrm::NServer::NObjects::TMultiReferenceDescriptor<TAuthor, TBook> BookRefsDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TBookRefsAttribute, BookRefs);

        using TEtc = NYT::NOrm::NExample::NClient::NProto::NDataModel::TAuthorStatus;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TAuthor, TEtc> EtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TStatus, Status);
};

////////////////////////////////////////////////////////////////////////////////

class TBook
    : public TDataModelObject
    , public NYT::TRefTracked<TBook>
{
public:
    static constexpr NYT::NOrm::NClient::NObjects::TObjectTypeValue Type =
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::Book);

    TBook(
        const i64& id,
        const i64& id2,
        const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
        NYT::NOrm::NServer::NObjects::IObjectTypeHandler* typeHandler,
        NYT::NOrm::NServer::NObjects::ISession* session);

    virtual NYT::NOrm::NClient::NObjects::TObjectTypeValue GetType() const override
    {
        return Type;
    }

    virtual NYT::NOrm::NClient::NObjects::TObjectKey GetKey() const override;

protected:
    const i64 Id_;

public:
    const i64& GetId() const
    {
        return Id_;
    }

protected:
    const i64 Id2_;

public:
    const i64& GetId2() const
    {
        return Id2_;
    }

private:
    NYT::NOrm::NServer::NObjects::TParentKeyAttribute ParentKeyAttribute_;

public:
    NYT::NOrm::NClient::NObjects::TObjectKey GetParentKey(std::source_location location = std::source_location::current()) const override;
    NYT::NOrm::NServer::NObjects::TParentKeyAttribute* GetParentKeyAttribute() override;
    i64 PublisherId(std::source_location location = std::source_location::current()) const;

    using TPublisherAttribute =
        NYT::NOrm::NServer::NObjects::TParentAttribute<TPublisher>;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TPublisherAttribute, Publisher);

    using TIsbn_ = TString;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TBook, TIsbn_> IsbnDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TIsbn_>, Isbn);

    using TFinalizationStartTime_ = ui64;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TBook, TFinalizationStartTime_> FinalizationStartTimeDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT_OVERRIDE(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFinalizationStartTime_>, FinalizationStartTime);

    using TFinalizers_ = THashMap<TString, NYT::NOrm::NDataModel::TFinalizer>;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TBook, TFinalizers_> FinalizersDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT_OVERRIDE(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFinalizers_>, Finalizers);

    using TMetaEtc = typename NYT::NOrm::NExample::NServer::NProto::NAutogen::TBookMetaEtc;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TBook, TMetaEtc> MetaEtcDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TMetaEtc>, MetaEtc);

    void ScheduleUuidLoad() const override;
    NYT::NOrm::NClient::NObjects::TObjectId GetUuid(std::source_location location = std::source_location::current()) const override;
    TString GetName(std::source_location location = std::source_location::current()) const override;

    class TSpec
    {
    public:
        explicit TSpec(TBook* object);

        class TPeerReview
        {
        public:
            explicit TPeerReview(TBook* object);

            using TReviewerIdsAttribute =
                NYT::NOrm::NServer::NObjects::TManyToManyInlineAttribute<NYT::NOrm::NExample::NServer::NLibrary::TBook, NYT::NOrm::NExample::NServer::NLibrary::TAuthor>;
            static const TReviewerIdsAttribute::TDescriptor ReviewerIdsDescriptor;
            DEFINE_BYREF_RW_PROPERTY_NO_INIT(TReviewerIdsAttribute, ReviewerIds);

            using TReviewersAttributeDescriptor =
                NYT::NOrm::NServer::NObjects::TManyToManyInlineViewAttributeDescriptor<NYT::NOrm::NExample::NServer::NLibrary::TBook, NYT::NOrm::NExample::NServer::NLibrary::TAuthor>;
            static const TReviewersAttributeDescriptor ReviewersDescriptor;
            DEFINE_BYREF_RW_PROPERTY_NO_INIT(
                NYT::NOrm::NServer::NObjects::TManyToManyInlineViewAttribute,
                Reviewers);

            using TEtc = NYT::NOrm::NExample::NServer::NProto::NAutogen::TBookDescriptionPeerReviewInfoEtc;
            static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TBook, TEtc> EtcDescriptor;
            DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
        };

        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TPeerReview, PeerReview);

        using TName_ = TString;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TBook, TName_> NameDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TName_>, Name);

        using TYear_ = i32;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TBook, TYear_> YearDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TYear_>, Year);

        using TFont_ = TString;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TBook, TFont_> FontDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFont_>, Font);

        using TGenres_ = std::vector<TString>;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TBook, TGenres_> GenresDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TGenres_>, Genres);

        using TKeywords_ = std::vector<TString>;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TBook, TKeywords_> KeywordsDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TKeywords_>, Keywords);

        using TEditorIdAttribute =
            NYT::NOrm::NServer::NObjects::TManyToOneAttribute<NYT::NOrm::NExample::NServer::NLibrary::TBook, NYT::NOrm::NExample::NServer::NLibrary::TEditor>;
        static const TEditorIdAttribute::TDescriptor EditorIdDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TEditorIdAttribute, EditorId);

        using TDigitalData_ = NYT::NOrm::NExample::NClient::NProto::NDataModel::TBookDescription::TDigitalData;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TBook, TDigitalData_> DigitalDataDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TDigitalData_>, DigitalData);

        using TAuthorIds_ = std::vector<TString>;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TBook, TAuthorIds_> AuthorIdsDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TAuthorIds_>, AuthorIds);

        using TIllustratorIdAttribute =
            NYT::NOrm::NServer::NObjects::TManyToOneAttribute<NYT::NOrm::NExample::NServer::NLibrary::TBook, NYT::NOrm::NExample::NServer::NLibrary::TIllustrator>;
        static const TIllustratorIdAttribute::TDescriptor IllustratorIdDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TIllustratorIdAttribute, IllustratorId);

        using TAlternativePublisherIdsAttribute =
            NYT::NOrm::NServer::NObjects::TManyToManyInlineAttribute<NYT::NOrm::NExample::NServer::NLibrary::TBook, NYT::NOrm::NExample::NServer::NLibrary::TPublisher>;
        static const TAlternativePublisherIdsAttribute::TDescriptor AlternativePublisherIdsDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TAlternativePublisherIdsAttribute, AlternativePublisherIds);

        using TChapterDescriptions_ = std::vector<NYT::NOrm::NExample::NClient::NProto::NDataModel::TBookDescription::TChapterDescription>;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TBook, TChapterDescriptions_> ChapterDescriptionsDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TChapterDescriptions_>, ChapterDescriptions);

        using TCoverIllustratorIdAttribute =
            NYT::NOrm::NServer::NObjects::TManyToOneAttribute<NYT::NOrm::NExample::NServer::NLibrary::TBook, NYT::NOrm::NExample::NServer::NLibrary::TIllustrator>;
        static const TCoverIllustratorIdAttribute::TDescriptor CoverIllustratorIdDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TCoverIllustratorIdAttribute, CoverIllustratorId);

        using TApiRevision_ = ui64;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TBook, TApiRevision_> ApiRevisionDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TApiRevision_>, ApiRevision);

        using TAuthorRefsAttribute =
            NYT::NOrm::NServer::NObjects::TMultiReferenceAttribute<TBook, TAuthor>;
        static const NYT::NOrm::NServer::NObjects::TMultiReferenceDescriptor<TBook, TAuthor> AuthorRefsDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TAuthorRefsAttribute, AuthorRefs);

        using TEditorAttributeDescriptor =
            NYT::NOrm::NServer::NObjects::TManyToOneViewAttributeDescriptor<NYT::NOrm::NExample::NServer::NLibrary::TBook, NYT::NOrm::NExample::NServer::NLibrary::TEditor>;
        static const TEditorAttributeDescriptor EditorDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(
            NYT::NOrm::NServer::NObjects::TManyToOneViewAttribute,
            Editor);

        using TIllustratorAttributeDescriptor =
            NYT::NOrm::NServer::NObjects::TManyToOneViewAttributeDescriptor<NYT::NOrm::NExample::NServer::NLibrary::TBook, NYT::NOrm::NExample::NServer::NLibrary::TIllustrator>;
        static const TIllustratorAttributeDescriptor IllustratorDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(
            NYT::NOrm::NServer::NObjects::TManyToOneViewAttribute,
            Illustrator);

        using TPublishersAttributeDescriptor =
            NYT::NOrm::NServer::NObjects::TManyToManyInlineViewAttributeDescriptor<NYT::NOrm::NExample::NServer::NLibrary::TBook, NYT::NOrm::NExample::NServer::NLibrary::TPublisher>;
        static const TPublishersAttributeDescriptor PublishersDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(
            NYT::NOrm::NServer::NObjects::TManyToManyInlineViewAttribute,
            Publishers);

        using TCoverIllustratorAttributeDescriptor =
            NYT::NOrm::NServer::NObjects::TManyToOneViewAttributeDescriptor<NYT::NOrm::NExample::NServer::NLibrary::TBook, NYT::NOrm::NExample::NServer::NLibrary::TIllustrator>;
        static const TCoverIllustratorAttributeDescriptor CoverIllustratorDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(
            NYT::NOrm::NServer::NObjects::TManyToOneViewAttribute,
            CoverIllustrator);

        static const NYT::NOrm::NServer::NObjects::TMultiViewDescriptor<TBook, TAuthor> AuthorsDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(
            NYT::NOrm::NServer::NObjects::TViewAttributeBase,
            Authors);

        using TEtc = NYT::NOrm::NExample::NServer::NProto::NAutogen::TBookDescriptionEtc;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TBook, TEtc> EtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);

        using TWarehouseEtc = NYT::NOrm::NExample::NServer::NProto::NAutogen::TBookDescriptionWarehouseEtc;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TBook, TWarehouseEtc> WarehouseEtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TWarehouseEtc>, WarehouseEtc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TSpec, Spec);

    class TStatus
    {
    public:
        explicit TStatus(TBook* object);

        using TReleased_ = bool;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TBook, TReleased_> ReleasedDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TReleased_>, Released);

        using THitchhikersAttribute =
            NYT::NOrm::NServer::NObjects::TMultiReferenceAttribute<TBook, THitchhiker>;
        static const NYT::NOrm::NServer::NObjects::TMultiReferenceDescriptor<TBook, THitchhiker> HitchhikersDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(THitchhikersAttribute, Hitchhikers);

        using TFormedHitchhikersAttribute =
            NYT::NOrm::NServer::NObjects::TMultiReferenceAttribute<TBook, THitchhiker>;
        static const NYT::NOrm::NServer::NObjects::TMultiReferenceDescriptor<TBook, THitchhiker> FormedHitchhikersDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TFormedHitchhikersAttribute, FormedHitchhikers);

        static const NYT::NOrm::NServer::NObjects::TMultiViewDescriptor<TBook, THitchhiker> HitchhikersViewDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(
            NYT::NOrm::NServer::NObjects::TViewAttributeBase,
            HitchhikersView);

        static const NYT::NOrm::NServer::NObjects::TMultiViewDescriptor<TBook, THitchhiker> FormedHitchhikersViewDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(
            NYT::NOrm::NServer::NObjects::TViewAttributeBase,
            FormedHitchhikersView);

        using TEtc = NYT::NOrm::NExample::NServer::NProto::NAutogen::TBookStatusEtc;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TBook, TEtc> EtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TStatus, Status);
};

////////////////////////////////////////////////////////////////////////////////

class TBufferedTimestampId
    : public TDataModelObject
    , public NYT::TRefTracked<TBufferedTimestampId>
{
public:
    static constexpr NYT::NOrm::NClient::NObjects::TObjectTypeValue Type =
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::BufferedTimestampId);

    TBufferedTimestampId(
        const i64& i64Id,
        const ui64& ui64Id,
        NYT::NOrm::NServer::NObjects::IObjectTypeHandler* typeHandler,
        NYT::NOrm::NServer::NObjects::ISession* session);

    virtual NYT::NOrm::NClient::NObjects::TObjectTypeValue GetType() const override
    {
        return Type;
    }

    virtual NYT::NOrm::NClient::NObjects::TObjectKey GetKey() const override;

protected:
    const i64 I64Id_;

public:
    const i64& GetI64Id() const
    {
        return I64Id_;
    }

protected:
    const ui64 Ui64Id_;

public:
    const ui64& GetUi64Id() const
    {
        return Ui64Id_;
    }

    using TFinalizationStartTime_ = ui64;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TBufferedTimestampId, TFinalizationStartTime_> FinalizationStartTimeDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT_OVERRIDE(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFinalizationStartTime_>, FinalizationStartTime);

    using TFinalizers_ = THashMap<TString, NYT::NOrm::NDataModel::TFinalizer>;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TBufferedTimestampId, TFinalizers_> FinalizersDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT_OVERRIDE(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFinalizers_>, Finalizers);

    using TMetaEtc = typename NYT::NOrm::NExample::NServer::NProto::NAutogen::TBufferedTimestampIdMetaEtc;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TBufferedTimestampId, TMetaEtc> MetaEtcDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TMetaEtc>, MetaEtc);

    void ScheduleUuidLoad() const override;
    NYT::NOrm::NClient::NObjects::TObjectId GetUuid(std::source_location location = std::source_location::current()) const override;
    TString GetName(std::source_location location = std::source_location::current()) const override;

    class TSpec
    {
    public:
        explicit TSpec(TBufferedTimestampId* object);

        using TI64Value_ = i64;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TBufferedTimestampId, TI64Value_> I64ValueDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TI64Value_>, I64Value);

        using TEtc = NYT::NOrm::NExample::NServer::NProto::NAutogen::TBufferedTimestampIdSpecEtc;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TBufferedTimestampId, TEtc> EtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TSpec, Spec);

    class TStatus
    {
    public:
        explicit TStatus(TBufferedTimestampId* object);

        using TEtc = NYT::NOrm::NExample::NClient::NProto::NDataModel::TBufferedTimestampIdStatus;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TBufferedTimestampId, TEtc> EtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TStatus, Status);
};

////////////////////////////////////////////////////////////////////////////////

class TCat
    : public TDataModelObject
    , public NYT::TRefTracked<TCat>
{
public:
    static constexpr NYT::NOrm::NClient::NObjects::TObjectTypeValue Type =
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::Cat);

    TCat(
        const i64& id,
        NYT::NOrm::NServer::NObjects::IObjectTypeHandler* typeHandler,
        NYT::NOrm::NServer::NObjects::ISession* session);

    virtual NYT::NOrm::NClient::NObjects::TObjectTypeValue GetType() const override
    {
        return Type;
    }

    virtual NYT::NOrm::NClient::NObjects::TObjectKey GetKey() const override;

protected:
    const i64 Id_;

public:
    const i64& GetId() const
    {
        return Id_;
    }

    using TBreed_ = TString;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, TBreed_> BreedDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TBreed_>, Breed);

    using TFinalizationStartTime_ = ui64;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, TFinalizationStartTime_> FinalizationStartTimeDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT_OVERRIDE(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFinalizationStartTime_>, FinalizationStartTime);

    using TFinalizers_ = THashMap<TString, NYT::NOrm::NDataModel::TFinalizer>;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, TFinalizers_> FinalizersDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT_OVERRIDE(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFinalizers_>, Finalizers);

    using TMetaEtc = typename NYT::NOrm::NExample::NServer::NProto::NAutogen::TCatMetaEtc;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, TMetaEtc> MetaEtcDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TMetaEtc>, MetaEtc);

    void ScheduleUuidLoad() const override;
    NYT::NOrm::NClient::NObjects::TObjectId GetUuid(std::source_location location = std::source_location::current()) const override;
    TString GetName(std::source_location location = std::source_location::current()) const override;

    class TSpec
    {
    public:
        explicit TSpec(TCat* object);

        using TLastSleepDuration_ = i64;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, TLastSleepDuration_> LastSleepDurationDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TLastSleepDuration_>, LastSleepDuration);

        using TFavouriteFood_ = TString;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, TFavouriteFood_> FavouriteFoodDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFavouriteFood_>, FavouriteFood);

        using TFavouriteToy_ = TString;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, TFavouriteToy_> FavouriteToyDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFavouriteToy_>, FavouriteToy);

        using TRevision_ = ui64;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, TRevision_> RevisionDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TRevision_>, Revision);

        using TMood_ = EMood;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, TMood_> MoodDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TMood_>, Mood);

        using THealthCondition_ = EHealthCondition;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, THealthCondition_> HealthConditionDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<THealthCondition_>, HealthCondition);

        using TEyeColorWithDefaultYsonStorageType_ = EEyeColor;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, TEyeColorWithDefaultYsonStorageType_> EyeColorWithDefaultYsonStorageTypeDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEyeColorWithDefaultYsonStorageType_>, EyeColorWithDefaultYsonStorageType);

        using TFriendCatsCount_ = i64;
        static const NYT::NOrm::NServer::NObjects::TScalarAggregatedAttributeDescriptor<TCat, TFriendCatsCount_> FriendCatsCountDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAggregatedAttribute<TFriendCatsCount_>, FriendCatsCount);

        using TStatisticsForDaysOfYear_ = THashMap<ui32, NYT::NOrm::NExample::NClient::NProto::NDataModel::TCatSpec::DayStatistics>;
        static const NYT::NOrm::NServer::NObjects::TScalarAggregatedAttributeDescriptor<TCat, TStatisticsForDaysOfYear_> StatisticsForDaysOfYearDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAggregatedAttribute<TStatisticsForDaysOfYear_>, StatisticsForDaysOfYear);

        using TEtc = NYT::NOrm::NExample::NServer::NProto::NAutogen::TCatSpecEtc;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, TEtc> EtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TSpec, Spec);

    class TStatus
    {
    public:
        explicit TStatus(TCat* object);

        class TUpdatableNested
        {
        public:
            explicit TUpdatableNested(TCat* object);

            using TInnerFirst_ = i64;
            static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, TInnerFirst_> InnerFirstDescriptor;
            DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TInnerFirst_>, InnerFirst);

            using TInnerSecond_ = i64;
            static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, TInnerSecond_> InnerSecondDescriptor;
            DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TInnerSecond_>, InnerSecond);

            using TInnerThird_ = i64;
            static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, TInnerThird_> InnerThirdDescriptor;
            DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TInnerThird_>, InnerThird);

            using TEtc = NYT::NOrm::NExample::NServer::NProto::NAutogen::TCatStatusInnerEtc;
            static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, TEtc> EtcDescriptor;
            DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
        };

        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TUpdatableNested, UpdatableNested);

        class TReadOnlyNested
        {
        public:
            explicit TReadOnlyNested(TCat* object);

            using TInnerFirst_ = i64;
            static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, TInnerFirst_> InnerFirstDescriptor;
            DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TInnerFirst_>, InnerFirst);

            using TInnerSecond_ = i64;
            static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, TInnerSecond_> InnerSecondDescriptor;
            DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TInnerSecond_>, InnerSecond);

            using TInnerThird_ = i64;
            static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, TInnerThird_> InnerThirdDescriptor;
            DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TInnerThird_>, InnerThird);

            using TEtc = NYT::NOrm::NExample::NServer::NProto::NAutogen::TCatStatusInnerEtc;
            static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, TEtc> EtcDescriptor;
            DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
        };

        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TReadOnlyNested, ReadOnlyNested);

        class TOpaqueRoNested
        {
        public:
            explicit TOpaqueRoNested(TCat* object);

            using TInnerFirst_ = i64;
            static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, TInnerFirst_> InnerFirstDescriptor;
            DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TInnerFirst_>, InnerFirst);

            using TInnerSecond_ = i64;
            static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, TInnerSecond_> InnerSecondDescriptor;
            DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TInnerSecond_>, InnerSecond);

            using TInnerThird_ = i64;
            static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, TInnerThird_> InnerThirdDescriptor;
            DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TInnerThird_>, InnerThird);

            using TEtc = NYT::NOrm::NExample::NServer::NProto::NAutogen::TCatStatusInnerEtc;
            static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, TEtc> EtcDescriptor;
            DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
        };

        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TOpaqueRoNested, OpaqueRoNested);

        class TOpaqueNested
        {
        public:
            explicit TOpaqueNested(TCat* object);

            using TInnerFirst_ = i64;
            static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, TInnerFirst_> InnerFirstDescriptor;
            DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TInnerFirst_>, InnerFirst);

            using TInnerSecond_ = i64;
            static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, TInnerSecond_> InnerSecondDescriptor;
            DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TInnerSecond_>, InnerSecond);

            using TInnerThird_ = i64;
            static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, TInnerThird_> InnerThirdDescriptor;
            DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TInnerThird_>, InnerThird);

            using TEtc = NYT::NOrm::NExample::NServer::NProto::NAutogen::TCatStatusInnerEtc;
            static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, TEtc> EtcDescriptor;
            DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
        };

        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TOpaqueNested, OpaqueNested);

        using TEtc = NYT::NOrm::NExample::NServer::NProto::NAutogen::TCatStatusEtc;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TCat, TEtc> EtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TStatus, Status);
};

////////////////////////////////////////////////////////////////////////////////

class TEditor
    : public TDataModelObject
    , public NYT::TRefTracked<TEditor>
{
public:
    static constexpr NYT::NOrm::NClient::NObjects::TObjectTypeValue Type =
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::Editor);

    TEditor(
        const TString& id,
        NYT::NOrm::NServer::NObjects::IObjectTypeHandler* typeHandler,
        NYT::NOrm::NServer::NObjects::ISession* session);

    virtual NYT::NOrm::NClient::NObjects::TObjectTypeValue GetType() const override
    {
        return Type;
    }

    virtual NYT::NOrm::NClient::NObjects::TObjectKey GetKey() const override;

protected:
    const TString Id_;

public:
    const TString& GetId() const
    {
        return Id_;
    }

    using TFinalizationStartTime_ = ui64;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TEditor, TFinalizationStartTime_> FinalizationStartTimeDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT_OVERRIDE(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFinalizationStartTime_>, FinalizationStartTime);

    using TFinalizers_ = THashMap<TString, NYT::NOrm::NDataModel::TFinalizer>;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TEditor, TFinalizers_> FinalizersDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT_OVERRIDE(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFinalizers_>, Finalizers);

    using TMetaEtc = typename NYT::NOrm::NExample::NServer::NProto::NAutogen::TSomeMetaEtc;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TEditor, TMetaEtc> MetaEtcDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TMetaEtc>, MetaEtc);

    void ScheduleUuidLoad() const override;
    NYT::NOrm::NClient::NObjects::TObjectId GetUuid(std::source_location location = std::source_location::current()) const override;
    TString GetName(std::source_location location = std::source_location::current()) const override;

    class TSpec
    {
    public:
        explicit TSpec(TEditor* object);

        using TAchievements_ = std::vector<TString>;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TEditor, TAchievements_> AchievementsDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TAchievements_>, Achievements);

        using TPhoneNumber_ = TString;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TEditor, TPhoneNumber_> PhoneNumberDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TPhoneNumber_>, PhoneNumber);

        using TEtc = NYT::NOrm::NExample::NServer::NProto::NAutogen::TSomeSpecEtc;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TEditor, TEtc> EtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TSpec, Spec);

    class TStatus
    {
    public:
        explicit TStatus(TEditor* object);

        using TBooksAttribute =
            NYT::NOrm::NServer::NObjects::TOneToManyAttribute<NYT::NOrm::NExample::NServer::NLibrary::TEditor, NYT::NOrm::NExample::NServer::NLibrary::TBook>;
        static const TBooksAttribute::TDescriptor BooksDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TBooksAttribute, Books);

        using TPublishersAttribute =
            NYT::NOrm::NServer::NObjects::TOneToManyAttribute<NYT::NOrm::NExample::NServer::NLibrary::TEditor, NYT::NOrm::NExample::NServer::NLibrary::TPublisher>;
        static const TPublishersAttribute::TDescriptor PublishersDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TPublishersAttribute, Publishers);

        using TEtc = NYT::NOrm::NExample::NClient::NProto::NDataModel::TSomeStatus;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TEditor, TEtc> EtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TStatus, Status);
};

////////////////////////////////////////////////////////////////////////////////

class TEmployer
    : public TDataModelObject
    , public NYT::TRefTracked<TEmployer>
{
public:
    static constexpr NYT::NOrm::NClient::NObjects::TObjectTypeValue Type =
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::Employer);

    TEmployer(
        const TString& id,
        NYT::NOrm::NServer::NObjects::IObjectTypeHandler* typeHandler,
        NYT::NOrm::NServer::NObjects::ISession* session);

    virtual NYT::NOrm::NClient::NObjects::TObjectTypeValue GetType() const override
    {
        return Type;
    }

    virtual NYT::NOrm::NClient::NObjects::TObjectKey GetKey() const override;

protected:
    const TString Id_;

public:
    const TString& GetId() const
    {
        return Id_;
    }

    using TEmail_ = TString;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TEmployer, TEmail_> EmailDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEmail_>, Email);

    using TFinalizationStartTime_ = ui64;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TEmployer, TFinalizationStartTime_> FinalizationStartTimeDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT_OVERRIDE(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFinalizationStartTime_>, FinalizationStartTime);

    using TFinalizers_ = THashMap<TString, NYT::NOrm::NDataModel::TFinalizer>;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TEmployer, TFinalizers_> FinalizersDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT_OVERRIDE(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFinalizers_>, Finalizers);

    using TMetaEtc = typename NYT::NOrm::NExample::NServer::NProto::NAutogen::TEmployerMetaEtc;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TEmployer, TMetaEtc> MetaEtcDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TMetaEtc>, MetaEtc);

    void ScheduleUuidLoad() const override;
    NYT::NOrm::NClient::NObjects::TObjectId GetUuid(std::source_location location = std::source_location::current()) const override;
    TString GetName(std::source_location location = std::source_location::current()) const override;

    class TSpec
    {
    public:
        explicit TSpec(TEmployer* object);

        using TEtc = NYT::NOrm::NExample::NClient::NProto::NDataModel::TEmployer_TSpec;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TEmployer, TEtc> EtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TSpec, Spec);

    class TStatus
    {
    public:
        explicit TStatus(TEmployer* object);

        using TSalary_ = i64;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TEmployer, TSalary_> SalaryDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TSalary_>, Salary);

        using TEtc = NYT::NOrm::NExample::NServer::NProto::NAutogen::TEmployerStatusEtc;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TEmployer, TEtc> EtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TStatus, Status);
};

////////////////////////////////////////////////////////////////////////////////

class TExecutor
    : public TDataModelObject
    , public NYT::TRefTracked<TExecutor>
{
public:
    static constexpr NYT::NOrm::NClient::NObjects::TObjectTypeValue Type =
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::Executor);

    TExecutor(
        const i64& id,
        NYT::NOrm::NServer::NObjects::IObjectTypeHandler* typeHandler,
        NYT::NOrm::NServer::NObjects::ISession* session);

    virtual NYT::NOrm::NClient::NObjects::TObjectTypeValue GetType() const override
    {
        return Type;
    }

    virtual NYT::NOrm::NClient::NObjects::TObjectKey GetKey() const override;

protected:
    const i64 Id_;

public:
    const i64& GetId() const
    {
        return Id_;
    }

    using TFinalizationStartTime_ = ui64;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TExecutor, TFinalizationStartTime_> FinalizationStartTimeDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT_OVERRIDE(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFinalizationStartTime_>, FinalizationStartTime);

    using TFinalizers_ = THashMap<TString, NYT::NOrm::NDataModel::TFinalizer>;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TExecutor, TFinalizers_> FinalizersDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT_OVERRIDE(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFinalizers_>, Finalizers);

    using TMetaEtc = typename NYT::NOrm::NExample::NServer::NProto::NAutogen::TExecutorMetaEtc;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TExecutor, TMetaEtc> MetaEtcDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TMetaEtc>, MetaEtc);

    void ScheduleUuidLoad() const override;
    NYT::NOrm::NClient::NObjects::TObjectId GetUuid(std::source_location location = std::source_location::current()) const override;
    TString GetName(std::source_location location = std::source_location::current()) const override;

    class TSpec
    {
    public:
        explicit TSpec(TExecutor* object);

        using TEtc = NYT::NOrm::NExample::NClient::NProto::NDataModel::TExecutorSpec;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TExecutor, TEtc> EtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TSpec, Spec);

    class TStatus
    {
    public:
        explicit TStatus(TExecutor* object);

        using TMotherShipsAttribute =
            NYT::NOrm::NServer::NObjects::TOneToManyAttribute<NYT::NOrm::NExample::NServer::NLibrary::TExecutor, NYT::NOrm::NExample::NServer::NLibrary::TMotherShip>;
        static const TMotherShipsAttribute::TDescriptor MotherShipsDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TMotherShipsAttribute, MotherShips);

        using TEtc = NYT::NOrm::NExample::NClient::NProto::NDataModel::TExecutorStatus;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TExecutor, TEtc> EtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TStatus, Status);
};

////////////////////////////////////////////////////////////////////////////////

class TGenre
    : public TDataModelObject
    , public NYT::TRefTracked<TGenre>
{
public:
    static constexpr NYT::NOrm::NClient::NObjects::TObjectTypeValue Type =
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::Genre);

    TGenre(
        const TString& id,
        NYT::NOrm::NServer::NObjects::IObjectTypeHandler* typeHandler,
        NYT::NOrm::NServer::NObjects::ISession* session);

    virtual NYT::NOrm::NClient::NObjects::TObjectTypeValue GetType() const override
    {
        return Type;
    }

    virtual NYT::NOrm::NClient::NObjects::TObjectKey GetKey() const override;

protected:
    const TString Id_;

public:
    const TString& GetId() const
    {
        return Id_;
    }

    using TFinalizationStartTime_ = ui64;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TGenre, TFinalizationStartTime_> FinalizationStartTimeDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT_OVERRIDE(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFinalizationStartTime_>, FinalizationStartTime);

    using TFinalizers_ = THashMap<TString, NYT::NOrm::NDataModel::TFinalizer>;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TGenre, TFinalizers_> FinalizersDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT_OVERRIDE(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFinalizers_>, Finalizers);

    using TMetaEtc = typename NYT::NOrm::NExample::NServer::NProto::NAutogen::TGenreMetaEtc;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TGenre, TMetaEtc> MetaEtcDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TMetaEtc>, MetaEtc);

    void ScheduleUuidLoad() const override;
    NYT::NOrm::NClient::NObjects::TObjectId GetUuid(std::source_location location = std::source_location::current()) const override;
    TString GetName(std::source_location location = std::source_location::current()) const override;

    class TSpec
    {
    public:
        explicit TSpec(TGenre* object);

        using TEtc = NYT::NOrm::NExample::NClient::NProto::NDataModel::TGenreSpec;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TGenre, TEtc> EtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TSpec, Spec);

    class TStatus
    {
    public:
        explicit TStatus(TGenre* object);

        using TEtc = NYT::NOrm::NExample::NClient::NProto::NDataModel::TGenreStatus;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TGenre, TEtc> EtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TStatus, Status);
};

////////////////////////////////////////////////////////////////////////////////

class TGroup
    : public TDataModelObject
    , public NYT::TRefTracked<TGroup>
{
public:
    static constexpr NYT::NOrm::NClient::NObjects::TObjectTypeValue Type =
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::Group);

    TGroup(
        const TString& id,
        NYT::NOrm::NServer::NObjects::IObjectTypeHandler* typeHandler,
        NYT::NOrm::NServer::NObjects::ISession* session);

    virtual NYT::NOrm::NClient::NObjects::TObjectTypeValue GetType() const override
    {
        return Type;
    }

    virtual NYT::NOrm::NClient::NObjects::TObjectKey GetKey() const override;

protected:
    const TString Id_;

public:
    const TString& GetId() const
    {
        return Id_;
    }

    using TFinalizationStartTime_ = ui64;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TGroup, TFinalizationStartTime_> FinalizationStartTimeDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT_OVERRIDE(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFinalizationStartTime_>, FinalizationStartTime);

    using TFinalizers_ = THashMap<TString, NYT::NOrm::NDataModel::TFinalizer>;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TGroup, TFinalizers_> FinalizersDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT_OVERRIDE(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFinalizers_>, Finalizers);

    using TMetaEtc = typename NYT::NOrm::NExample::NServer::NProto::NAutogen::TGroupMetaEtc;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TGroup, TMetaEtc> MetaEtcDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TMetaEtc>, MetaEtc);

    void ScheduleUuidLoad() const override;
    NYT::NOrm::NClient::NObjects::TObjectId GetUuid(std::source_location location = std::source_location::current()) const override;
    TString GetName(std::source_location location = std::source_location::current()) const override;

    class TSpec
    {
    public:
        explicit TSpec(TGroup* object);

        using TEtc = NYT::NOrm::NExample::NClient::NProto::NDataModel::TGroupSpec;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TGroup, TEtc> EtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TSpec, Spec);

    class TStatus
    {
    public:
        explicit TStatus(TGroup* object);

        using TEtc = NYT::NOrm::NExample::NClient::NProto::NDataModel::TGroupStatus;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TGroup, TEtc> EtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TStatus, Status);
};

////////////////////////////////////////////////////////////////////////////////

class THitchhiker
    : public TDataModelObject
    , public NYT::TRefTracked<THitchhiker>
{
public:
    static constexpr NYT::NOrm::NClient::NObjects::TObjectTypeValue Type =
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::Hitchhiker);

    THitchhiker(
        const i64& id,
        NYT::NOrm::NServer::NObjects::IObjectTypeHandler* typeHandler,
        NYT::NOrm::NServer::NObjects::ISession* session);

    virtual NYT::NOrm::NClient::NObjects::TObjectTypeValue GetType() const override
    {
        return Type;
    }

    virtual NYT::NOrm::NClient::NObjects::TObjectKey GetKey() const override;

protected:
    const i64 Id_;

public:
    const i64& GetId() const
    {
        return Id_;
    }

    using TFormativeBookId_ = i64;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<THitchhiker, TFormativeBookId_> FormativeBookIdDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFormativeBookId_>, FormativeBookId);

    using TFormativeBookId2_ = i64;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<THitchhiker, TFormativeBookId2_> FormativeBookId2Descriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFormativeBookId2_>, FormativeBookId2);

    using TFinalizationStartTime_ = ui64;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<THitchhiker, TFinalizationStartTime_> FinalizationStartTimeDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT_OVERRIDE(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFinalizationStartTime_>, FinalizationStartTime);

    using TFinalizers_ = THashMap<TString, NYT::NOrm::NDataModel::TFinalizer>;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<THitchhiker, TFinalizers_> FinalizersDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT_OVERRIDE(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFinalizers_>, Finalizers);
    using TFormativeBookAttribute =
        NYT::NOrm::NServer::NObjects::TSingleReferenceAttribute<THitchhiker, TBook>;
    static const NYT::NOrm::NServer::NObjects::TSingleReferenceDescriptor<THitchhiker, TBook> FormativeBookDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TFormativeBookAttribute, FormativeBook);
    static const NYT::NOrm::NServer::NObjects::TSingleViewDescriptor<THitchhiker, TBook> FormativeBookViewDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(
        NYT::NOrm::NServer::NObjects::TViewAttributeBase,
        FormativeBookView);

    using TMetaEtc = typename NYT::NOrm::NExample::NServer::NProto::NAutogen::THitchhikerMetaEtc;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<THitchhiker, TMetaEtc> MetaEtcDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TMetaEtc>, MetaEtc);

    void ScheduleUuidLoad() const override;
    NYT::NOrm::NClient::NObjects::TObjectId GetUuid(std::source_location location = std::source_location::current()) const override;
    TString GetName(std::source_location location = std::source_location::current()) const override;

    class TSpec
    {
    public:
        explicit TSpec(THitchhiker* object);

        using THatedBooks_ = std::vector<NYT::NOrm::NExample::NClient::NProto::NDataModel::THitchhikerSpec::TBookId>;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<THitchhiker, THatedBooks_> HatedBooksDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<THatedBooks_>, HatedBooks);

        using TBooksAttribute =
            NYT::NOrm::NServer::NObjects::TMultiReferenceAttribute<THitchhiker, TBook>;
        static const NYT::NOrm::NServer::NObjects::TMultiReferenceDescriptor<THitchhiker, TBook> BooksDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TBooksAttribute, Books);

        static const NYT::NOrm::NServer::NObjects::TMultiViewDescriptor<THitchhiker, TBook> BooksViewDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(
            NYT::NOrm::NServer::NObjects::TViewAttributeBase,
            BooksView);

        using TEtc = NYT::NOrm::NExample::NServer::NProto::NAutogen::THitchhikerSpecEtc;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<THitchhiker, TEtc> EtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TSpec, Spec);

    class TStatus
    {
    public:
        explicit TStatus(THitchhiker* object);

        using TEtc = NYT::NOrm::NExample::NClient::NProto::NDataModel::THitchhikerStatus;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<THitchhiker, TEtc> EtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TStatus, Status);
};

////////////////////////////////////////////////////////////////////////////////

class TIllustrator
    : public TDataModelObject
    , public NYT::TRefTracked<TIllustrator>
{
public:
    static constexpr NYT::NOrm::NClient::NObjects::TObjectTypeValue Type =
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::Illustrator);

    TIllustrator(
        const i64& uid,
        const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
        NYT::NOrm::NServer::NObjects::IObjectTypeHandler* typeHandler,
        NYT::NOrm::NServer::NObjects::ISession* session);

    virtual NYT::NOrm::NClient::NObjects::TObjectTypeValue GetType() const override
    {
        return Type;
    }

    virtual NYT::NOrm::NClient::NObjects::TObjectKey GetKey() const override;

protected:
    const i64 Uid_;

public:
    const i64& GetUid() const
    {
        return Uid_;
    }

private:
    NYT::NOrm::NServer::NObjects::TParentKeyAttribute ParentKeyAttribute_;

public:
    NYT::NOrm::NClient::NObjects::TObjectKey GetParentKey(std::source_location location = std::source_location::current()) const override;
    NYT::NOrm::NServer::NObjects::TParentKeyAttribute* GetParentKeyAttribute() override;
    i64 PublisherId(std::source_location location = std::source_location::current()) const;

    using TPublisherAttribute =
        NYT::NOrm::NServer::NObjects::TParentAttribute<TPublisher>;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TPublisherAttribute, Publisher);

    using TPartTimeJobAttribute =
        NYT::NOrm::NServer::NObjects::TManyToOneAttribute<NYT::NOrm::NExample::NServer::NLibrary::TIllustrator, NYT::NOrm::NExample::NServer::NLibrary::TPublisher>;
    static const TPartTimeJobAttribute::TDescriptor PartTimeJobDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TPartTimeJobAttribute, PartTimeJob);

    using TFinalizationStartTime_ = ui64;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TIllustrator, TFinalizationStartTime_> FinalizationStartTimeDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT_OVERRIDE(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFinalizationStartTime_>, FinalizationStartTime);

    using TFinalizers_ = THashMap<TString, NYT::NOrm::NDataModel::TFinalizer>;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TIllustrator, TFinalizers_> FinalizersDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT_OVERRIDE(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFinalizers_>, Finalizers);
    using TPartTimeJobViewAttributeDescriptor =
    NYT::NOrm::NServer::NObjects::TManyToOneViewAttributeDescriptor<NYT::NOrm::NExample::NServer::NLibrary::TIllustrator, NYT::NOrm::NExample::NServer::NLibrary::TPublisher>;
    static const TPartTimeJobViewAttributeDescriptor PartTimeJobViewDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(
        NYT::NOrm::NServer::NObjects::TManyToOneViewAttribute,
        PartTimeJobView);

    using TMetaEtc = typename NYT::NOrm::NExample::NServer::NProto::NAutogen::TIllustratorMetaEtc;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TIllustrator, TMetaEtc> MetaEtcDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TMetaEtc>, MetaEtc);

    void ScheduleUuidLoad() const override;
    NYT::NOrm::NClient::NObjects::TObjectId GetUuid(std::source_location location = std::source_location::current()) const override;
    TString GetName(std::source_location location = std::source_location::current()) const override;

    class TSpec
    {
    public:
        explicit TSpec(TIllustrator* object);

        using TEtc = NYT::NOrm::NExample::NClient::NProto::NDataModel::TIllustratorSpec;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TIllustrator, TEtc> EtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TSpec, Spec);

    class TStatus
    {
    public:
        explicit TStatus(TIllustrator* object);

        using TBooksAttribute =
            NYT::NOrm::NServer::NObjects::TOneToManyAttribute<NYT::NOrm::NExample::NServer::NLibrary::TIllustrator, NYT::NOrm::NExample::NServer::NLibrary::TBook>;
        static const TBooksAttribute::TDescriptor BooksDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TBooksAttribute, Books);

        using TBooksForCoverIllustratorAttribute =
            NYT::NOrm::NServer::NObjects::TOneToManyAttribute<NYT::NOrm::NExample::NServer::NLibrary::TIllustrator, NYT::NOrm::NExample::NServer::NLibrary::TBook>;
        static const TBooksForCoverIllustratorAttribute::TDescriptor BooksForCoverIllustratorDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TBooksForCoverIllustratorAttribute, BooksForCoverIllustrator);

        using TPublishersAttribute =
            NYT::NOrm::NServer::NObjects::TOneToManyAttribute<NYT::NOrm::NExample::NServer::NLibrary::TIllustrator, NYT::NOrm::NExample::NServer::NLibrary::TPublisher>;
        static const TPublishersAttribute::TDescriptor PublishersDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TPublishersAttribute, Publishers);

        using TPublishersForOldFeaturedIllustratorsAttribute =
            NYT::NOrm::NServer::NObjects::TManyToManyTabularAttribute<NYT::NOrm::NExample::NServer::NLibrary::TIllustrator, NYT::NOrm::NExample::NServer::NLibrary::TPublisher>;
        static const TPublishersForOldFeaturedIllustratorsAttribute::TDescriptor PublishersForOldFeaturedIllustratorsDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TPublishersForOldFeaturedIllustratorsAttribute, PublishersForOldFeaturedIllustrators);

        using TPublishersForNewFeaturedIllustratorsAttribute =
            NYT::NOrm::NServer::NObjects::TManyToManyTabularAttribute<NYT::NOrm::NExample::NServer::NLibrary::TIllustrator, NYT::NOrm::NExample::NServer::NLibrary::TPublisher>;
        static const TPublishersForNewFeaturedIllustratorsAttribute::TDescriptor PublishersForNewFeaturedIllustratorsDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TPublishersForNewFeaturedIllustratorsAttribute, PublishersForNewFeaturedIllustrators);

        using TEtc = NYT::NOrm::NExample::NClient::NProto::NDataModel::TIllustratorStatus;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TIllustrator, TEtc> EtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TStatus, Status);
};

////////////////////////////////////////////////////////////////////////////////

class TIndexedIncrementId
    : public TDataModelObject
    , public NYT::TRefTracked<TIndexedIncrementId>
{
public:
    static constexpr NYT::NOrm::NClient::NObjects::TObjectTypeValue Type =
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::IndexedIncrementId);

    TIndexedIncrementId(
        const i64& i64Id,
        NYT::NOrm::NServer::NObjects::IObjectTypeHandler* typeHandler,
        NYT::NOrm::NServer::NObjects::ISession* session);

    virtual NYT::NOrm::NClient::NObjects::TObjectTypeValue GetType() const override
    {
        return Type;
    }

    virtual NYT::NOrm::NClient::NObjects::TObjectKey GetKey() const override;

protected:
    const i64 I64Id_;

public:
    const i64& GetI64Id() const
    {
        return I64Id_;
    }

    using TFinalizationStartTime_ = ui64;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TIndexedIncrementId, TFinalizationStartTime_> FinalizationStartTimeDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT_OVERRIDE(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFinalizationStartTime_>, FinalizationStartTime);

    using TFinalizers_ = THashMap<TString, NYT::NOrm::NDataModel::TFinalizer>;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TIndexedIncrementId, TFinalizers_> FinalizersDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT_OVERRIDE(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFinalizers_>, Finalizers);

    using TMetaEtc = typename NYT::NOrm::NExample::NServer::NProto::NAutogen::TIndexedIncrementIdMetaEtc;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TIndexedIncrementId, TMetaEtc> MetaEtcDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TMetaEtc>, MetaEtc);

    void ScheduleUuidLoad() const override;
    NYT::NOrm::NClient::NObjects::TObjectId GetUuid(std::source_location location = std::source_location::current()) const override;
    TString GetName(std::source_location location = std::source_location::current()) const override;

    class TSpec
    {
    public:
        explicit TSpec(TIndexedIncrementId* object);

        using TEtc = NYT::NOrm::NExample::NClient::NProto::NDataModel::TIndexedIncrementIdSpec;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TIndexedIncrementId, TEtc> EtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TSpec, Spec);

    class TStatus
    {
    public:
        explicit TStatus(TIndexedIncrementId* object);

        using TEtc = NYT::NOrm::NExample::NClient::NProto::NDataModel::TIndexedIncrementIdStatus;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TIndexedIncrementId, TEtc> EtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TStatus, Status);
};

////////////////////////////////////////////////////////////////////////////////

class TInterceptor
    : public TDataModelObject
    , public NYT::TRefTracked<TInterceptor>
{
public:
    static constexpr NYT::NOrm::NClient::NObjects::TObjectTypeValue Type =
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::Interceptor);

    TInterceptor(
        const i64& id,
        const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
        NYT::NOrm::NServer::NObjects::IObjectTypeHandler* typeHandler,
        NYT::NOrm::NServer::NObjects::ISession* session);

    virtual NYT::NOrm::NClient::NObjects::TObjectTypeValue GetType() const override
    {
        return Type;
    }

    virtual NYT::NOrm::NClient::NObjects::TObjectKey GetKey() const override;

protected:
    const i64 Id_;

public:
    const i64& GetId() const
    {
        return Id_;
    }

private:
    NYT::NOrm::NServer::NObjects::TParentKeyAttribute ParentKeyAttribute_;

public:
    NYT::NOrm::NClient::NObjects::TObjectKey GetParentKey(std::source_location location = std::source_location::current()) const override;
    NYT::NOrm::NServer::NObjects::TParentKeyAttribute* GetParentKeyAttribute() override;
    i64 MotherShipId(std::source_location location = std::source_location::current()) const;

    using TMotherShipAttribute =
        NYT::NOrm::NServer::NObjects::TParentAttribute<TMotherShip>;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TMotherShipAttribute, MotherShip);

    using TNexusIdAttribute =
        NYT::NOrm::NServer::NObjects::TOneTransitiveAttribute<TInterceptor, TNexus>;
    static const TNexusIdAttribute::TDescriptor NexusIdDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TNexusIdAttribute, NexusId);

    using TFinalizationStartTime_ = ui64;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TInterceptor, TFinalizationStartTime_> FinalizationStartTimeDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT_OVERRIDE(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFinalizationStartTime_>, FinalizationStartTime);

    using TFinalizers_ = THashMap<TString, NYT::NOrm::NDataModel::TFinalizer>;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TInterceptor, TFinalizers_> FinalizersDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT_OVERRIDE(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFinalizers_>, Finalizers);

    using TMetaEtc = typename NYT::NOrm::NExample::NServer::NProto::NAutogen::TInterceptorMetaEtc;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TInterceptor, TMetaEtc> MetaEtcDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TMetaEtc>, MetaEtc);

    void ScheduleUuidLoad() const override;
    NYT::NOrm::NClient::NObjects::TObjectId GetUuid(std::source_location location = std::source_location::current()) const override;
    TString GetName(std::source_location location = std::source_location::current()) const override;

    class TSpec
    {
    public:
        explicit TSpec(TInterceptor* object);

        using TEtc = NYT::NOrm::NExample::NClient::NProto::NDataModel::TInterceptorSpec;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TInterceptor, TEtc> EtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TSpec, Spec);

    class TStatus
    {
    public:
        explicit TStatus(TInterceptor* object);

        using TEtc = NYT::NOrm::NExample::NClient::NProto::NDataModel::TInterceptorStatus;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TInterceptor, TEtc> EtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TStatus, Status);
};

////////////////////////////////////////////////////////////////////////////////

class TManualId
    : public TDataModelObject
    , public NYT::TRefTracked<TManualId>
{
public:
    static constexpr NYT::NOrm::NClient::NObjects::TObjectTypeValue Type =
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::ManualId);

    TManualId(
        const TString& strId,
        const i64& i64Id,
        const ui64& ui64Id,
        NYT::NOrm::NServer::NObjects::IObjectTypeHandler* typeHandler,
        NYT::NOrm::NServer::NObjects::ISession* session);

    virtual NYT::NOrm::NClient::NObjects::TObjectTypeValue GetType() const override
    {
        return Type;
    }

    virtual NYT::NOrm::NClient::NObjects::TObjectKey GetKey() const override;

protected:
    const TString StrId_;

public:
    const TString& GetStrId() const
    {
        return StrId_;
    }

protected:
    const i64 I64Id_;

public:
    const i64& GetI64Id() const
    {
        return I64Id_;
    }

protected:
    const ui64 Ui64Id_;

public:
    const ui64& GetUi64Id() const
    {
        return Ui64Id_;
    }

    using TFinalizationStartTime_ = ui64;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TManualId, TFinalizationStartTime_> FinalizationStartTimeDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT_OVERRIDE(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFinalizationStartTime_>, FinalizationStartTime);

    using TFinalizers_ = THashMap<TString, NYT::NOrm::NDataModel::TFinalizer>;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TManualId, TFinalizers_> FinalizersDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT_OVERRIDE(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFinalizers_>, Finalizers);

    using TMetaEtc = typename NYT::NOrm::NExample::NServer::NProto::NAutogen::TManualIdMetaEtc;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TManualId, TMetaEtc> MetaEtcDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TMetaEtc>, MetaEtc);

    void ScheduleUuidLoad() const override;
    NYT::NOrm::NClient::NObjects::TObjectId GetUuid(std::source_location location = std::source_location::current()) const override;
    TString GetName(std::source_location location = std::source_location::current()) const override;

    class TSpec
    {
    public:
        explicit TSpec(TManualId* object);

        using TStrValue_ = TString;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TManualId, TStrValue_> StrValueDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TStrValue_>, StrValue);

        using TI32Value_ = i32;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TManualId, TI32Value_> I32ValueDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TI32Value_>, I32Value);

        using TUi32Value_ = ui32;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TManualId, TUi32Value_> Ui32ValueDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TUi32Value_>, Ui32Value);

        using TEtc = NYT::NOrm::NExample::NServer::NProto::NAutogen::TManualIdSpecEtc;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TManualId, TEtc> EtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TSpec, Spec);

    class TStatus
    {
    public:
        explicit TStatus(TManualId* object);

        using TEtc = NYT::NOrm::NExample::NClient::NProto::NDataModel::TManualIdStatus;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TManualId, TEtc> EtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TStatus, Status);
};

////////////////////////////////////////////////////////////////////////////////

class TMotherShip
    : public TDataModelObject
    , public NYT::TRefTracked<TMotherShip>
{
public:
    static constexpr NYT::NOrm::NClient::NObjects::TObjectTypeValue Type =
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::MotherShip);

    TMotherShip(
        const i64& id,
        const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
        NYT::NOrm::NServer::NObjects::IObjectTypeHandler* typeHandler,
        NYT::NOrm::NServer::NObjects::ISession* session);
    virtual ~TMotherShip() = 0;

    virtual NYT::NOrm::NClient::NObjects::TObjectTypeValue GetType() const override
    {
        return Type;
    }

    virtual NYT::NOrm::NClient::NObjects::TObjectKey GetKey() const override;

protected:
    const i64 Id_;

public:
    const i64& GetId() const
    {
        return Id_;
    }

private:
    NYT::NOrm::NServer::NObjects::TParentKeyAttribute ParentKeyAttribute_;

public:
    NYT::NOrm::NClient::NObjects::TObjectKey GetParentKey(std::source_location location = std::source_location::current()) const override;
    NYT::NOrm::NServer::NObjects::TParentKeyAttribute* GetParentKeyAttribute() override;
    i64 NexusId(std::source_location location = std::source_location::current()) const;

    using TNexusAttribute =
        NYT::NOrm::NServer::NObjects::TParentAttribute<TNexus>;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TNexusAttribute, Nexus);

    using TInterceptorAttribute =
        NYT::NOrm::NServer::NObjects::TChildrenAttribute<TInterceptor>;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TInterceptorAttribute, Interceptors);

    using TRevision_ = ui64;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TMotherShip, TRevision_> RevisionDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TRevision_>, Revision);

    using TReleaseYear_ = i64;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TMotherShip, TReleaseYear_> ReleaseYearDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TReleaseYear_>, ReleaseYear);

    using TFinalizationStartTime_ = ui64;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TMotherShip, TFinalizationStartTime_> FinalizationStartTimeDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT_OVERRIDE(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFinalizationStartTime_>, FinalizationStartTime);

    using TFinalizers_ = THashMap<TString, NYT::NOrm::NDataModel::TFinalizer>;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TMotherShip, TFinalizers_> FinalizersDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT_OVERRIDE(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFinalizers_>, Finalizers);

    using TMetaEtc = typename NYT::NOrm::NExample::NServer::NProto::NAutogen::TMotherShipMetaEtc;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TMotherShip, TMetaEtc> MetaEtcDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TMetaEtc>, MetaEtc);

    void ScheduleUuidLoad() const override;
    NYT::NOrm::NClient::NObjects::TObjectId GetUuid(std::source_location location = std::source_location::current()) const override;
    TString GetName(std::source_location location = std::source_location::current()) const override;

    class TSpec
    {
    public:
        explicit TSpec(TMotherShip* object);

        using TExecutorIdAttribute =
            NYT::NOrm::NServer::NObjects::TManyToOneAttribute<NYT::NOrm::NExample::NServer::NLibrary::TMotherShip, NYT::NOrm::NExample::NServer::NLibrary::TExecutor>;
        static const TExecutorIdAttribute::TDescriptor ExecutorIdDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TExecutorIdAttribute, ExecutorId);

        using TRevision_ = ui64;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TMotherShip, TRevision_> RevisionDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TRevision_>, Revision);

        using TSectorNames_ = std::vector<TString>;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TMotherShip, TSectorNames_> SectorNamesDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TSectorNames_>, SectorNames);

        using TPrice_ = i64;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TMotherShip, TPrice_> PriceDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TPrice_>, Price);

        using TEtc = NYT::NOrm::NExample::NServer::NProto::NAutogen::TMotherShipSpecEtc;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TMotherShip, TEtc> EtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TSpec, Spec);

    class TStatus
    {
    public:
        explicit TStatus(TMotherShip* object);

        using TEtc = NYT::NOrm::NExample::NClient::NProto::NDataModel::TMotherShipStatus;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TMotherShip, TEtc> EtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TStatus, Status);
};

////////////////////////////////////////////////////////////////////////////////

class TMultipolicyId
    : public TDataModelObject
    , public NYT::TRefTracked<TMultipolicyId>
{
public:
    static constexpr NYT::NOrm::NClient::NObjects::TObjectTypeValue Type =
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::MultipolicyId);

    TMultipolicyId(
        const TString& strId,
        const i64& i64Id,
        const ui64& ui64Id,
        const ui64& anotherUi64Id,
        NYT::NOrm::NServer::NObjects::IObjectTypeHandler* typeHandler,
        NYT::NOrm::NServer::NObjects::ISession* session);

    virtual NYT::NOrm::NClient::NObjects::TObjectTypeValue GetType() const override
    {
        return Type;
    }

    virtual NYT::NOrm::NClient::NObjects::TObjectKey GetKey() const override;

protected:
    const TString StrId_;

public:
    const TString& GetStrId() const
    {
        return StrId_;
    }

protected:
    const i64 I64Id_;

public:
    const i64& GetI64Id() const
    {
        return I64Id_;
    }

protected:
    const ui64 Ui64Id_;

public:
    const ui64& GetUi64Id() const
    {
        return Ui64Id_;
    }

protected:
    const ui64 AnotherUi64Id_;

public:
    const ui64& GetAnotherUi64Id() const
    {
        return AnotherUi64Id_;
    }

    using TFinalizationStartTime_ = ui64;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TMultipolicyId, TFinalizationStartTime_> FinalizationStartTimeDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT_OVERRIDE(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFinalizationStartTime_>, FinalizationStartTime);

    using TFinalizers_ = THashMap<TString, NYT::NOrm::NDataModel::TFinalizer>;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TMultipolicyId, TFinalizers_> FinalizersDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT_OVERRIDE(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFinalizers_>, Finalizers);

    using TMetaEtc = typename NYT::NOrm::NExample::NServer::NProto::NAutogen::TMultipolicyIdMetaEtc;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TMultipolicyId, TMetaEtc> MetaEtcDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TMetaEtc>, MetaEtc);

    void ScheduleUuidLoad() const override;
    NYT::NOrm::NClient::NObjects::TObjectId GetUuid(std::source_location location = std::source_location::current()) const override;
    TString GetName(std::source_location location = std::source_location::current()) const override;

    class TSpec
    {
    public:
        explicit TSpec(TMultipolicyId* object);

        using TStrValue_ = TString;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TMultipolicyId, TStrValue_> StrValueDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TStrValue_>, StrValue);

        using TI64Value_ = i64;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TMultipolicyId, TI64Value_> I64ValueDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TI64Value_>, I64Value);

        using TUi64Value_ = ui64;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TMultipolicyId, TUi64Value_> Ui64ValueDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TUi64Value_>, Ui64Value);

        using TAnotherUi64Value_ = ui64;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TMultipolicyId, TAnotherUi64Value_> AnotherUi64ValueDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TAnotherUi64Value_>, AnotherUi64Value);

        using TEtc = NYT::NOrm::NExample::NServer::NProto::NAutogen::TMultipolicyIdSpecEtc;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TMultipolicyId, TEtc> EtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TSpec, Spec);

    class TStatus
    {
    public:
        explicit TStatus(TMultipolicyId* object);

        using TEtc = NYT::NOrm::NExample::NClient::NProto::NDataModel::TMultipolicyIdStatus;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TMultipolicyId, TEtc> EtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TStatus, Status);
};

////////////////////////////////////////////////////////////////////////////////

class TNestedColumns
    : public TDataModelObject
    , public NYT::TRefTracked<TNestedColumns>
{
public:
    static constexpr NYT::NOrm::NClient::NObjects::TObjectTypeValue Type =
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::NestedColumns);

    TNestedColumns(
        const i64& id,
        NYT::NOrm::NServer::NObjects::IObjectTypeHandler* typeHandler,
        NYT::NOrm::NServer::NObjects::ISession* session);

    virtual NYT::NOrm::NClient::NObjects::TObjectTypeValue GetType() const override
    {
        return Type;
    }

    virtual NYT::NOrm::NClient::NObjects::TObjectKey GetKey() const override;

protected:
    const i64 Id_;

public:
    const i64& GetId() const
    {
        return Id_;
    }

    using TFinalizationStartTime_ = ui64;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TNestedColumns, TFinalizationStartTime_> FinalizationStartTimeDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT_OVERRIDE(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFinalizationStartTime_>, FinalizationStartTime);

    using TFinalizers_ = THashMap<TString, NYT::NOrm::NDataModel::TFinalizer>;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TNestedColumns, TFinalizers_> FinalizersDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT_OVERRIDE(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFinalizers_>, Finalizers);

    using TMetaEtc = typename NYT::NOrm::NExample::NServer::NProto::NAutogen::TNestedColumnsMetaEtc;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TNestedColumns, TMetaEtc> MetaEtcDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TMetaEtc>, MetaEtc);

    void ScheduleUuidLoad() const override;
    NYT::NOrm::NClient::NObjects::TObjectId GetUuid(std::source_location location = std::source_location::current()) const override;
    TString GetName(std::source_location location = std::source_location::current()) const override;

    class TSpec
    {
    public:
        explicit TSpec(TNestedColumns* object);

        class TCompositeSingular
        {
        public:
            explicit TCompositeSingular(TNestedColumns* object);

            using TColumnSingular_ = NYT::NOrm::NExample::NClient::NProto::NDataModel::TNestedColumnsSpec::TSimple;
            static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TNestedColumns, TColumnSingular_> ColumnSingularDescriptor;
            DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TColumnSingular_>, ColumnSingular);

            using TColumnRepeated_ = std::vector<NYT::NOrm::NExample::NClient::NProto::NDataModel::TNestedColumnsSpec::TSimple>;
            static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TNestedColumns, TColumnRepeated_> ColumnRepeatedDescriptor;
            DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TColumnRepeated_>, ColumnRepeated);

            using TColumnMap_ = THashMap<TString, NYT::NOrm::NExample::NClient::NProto::NDataModel::TNestedColumnsSpec::TSimple>;
            static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TNestedColumns, TColumnMap_> ColumnMapDescriptor;
            DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TColumnMap_>, ColumnMap);

            using TEtc = NYT::NOrm::NExample::NServer::NProto::NAutogen::TNestedColumnsSpecInnerEtc;
            static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TNestedColumns, TEtc> EtcDescriptor;
            DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
        };

        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TCompositeSingular, CompositeSingular);

        using TColumnSingular_ = NYT::NOrm::NExample::NClient::NProto::NDataModel::TNestedColumnsSpec::TSimple;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TNestedColumns, TColumnSingular_> ColumnSingularDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TColumnSingular_>, ColumnSingular);

        using TColumnRepeated_ = std::vector<NYT::NOrm::NExample::NClient::NProto::NDataModel::TNestedColumnsSpec::TSimple>;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TNestedColumns, TColumnRepeated_> ColumnRepeatedDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TColumnRepeated_>, ColumnRepeated);

        using TColumnMap_ = THashMap<TString, NYT::NOrm::NExample::NClient::NProto::NDataModel::TNestedColumnsSpec::TSimple>;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TNestedColumns, TColumnMap_> ColumnMapDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TColumnMap_>, ColumnMap);

        using TEtc = NYT::NOrm::NExample::NServer::NProto::NAutogen::TNestedColumnsSpecEtc;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TNestedColumns, TEtc> EtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TSpec, Spec);

    class TStatus
    {
    public:
        explicit TStatus(TNestedColumns* object);

        using TEtc = NYT::NOrm::NExample::NClient::NProto::NDataModel::TNestedColumnsStatus;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TNestedColumns, TEtc> EtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TStatus, Status);
};

////////////////////////////////////////////////////////////////////////////////

class TNexus
    : public TDataModelObject
    , public NYT::TRefTracked<TNexus>
{
public:
    static constexpr NYT::NOrm::NClient::NObjects::TObjectTypeValue Type =
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::Nexus);

    TNexus(
        const i64& id,
        NYT::NOrm::NServer::NObjects::IObjectTypeHandler* typeHandler,
        NYT::NOrm::NServer::NObjects::ISession* session);

    virtual NYT::NOrm::NClient::NObjects::TObjectTypeValue GetType() const override
    {
        return Type;
    }

    virtual NYT::NOrm::NClient::NObjects::TObjectKey GetKey() const override;

protected:
    const i64 Id_;

public:
    const i64& GetId() const
    {
        return Id_;
    }

    using TMotherShipAttribute =
        NYT::NOrm::NServer::NObjects::TChildrenAttribute<TMotherShip>;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TMotherShipAttribute, MotherShips);

    using TFinalizationStartTime_ = ui64;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TNexus, TFinalizationStartTime_> FinalizationStartTimeDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT_OVERRIDE(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFinalizationStartTime_>, FinalizationStartTime);

    using TFinalizers_ = THashMap<TString, NYT::NOrm::NDataModel::TFinalizer>;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TNexus, TFinalizers_> FinalizersDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT_OVERRIDE(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFinalizers_>, Finalizers);

    using TMetaEtc = typename NYT::NOrm::NExample::NServer::NProto::NAutogen::TNexusMetaEtc;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TNexus, TMetaEtc> MetaEtcDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TMetaEtc>, MetaEtc);

    void ScheduleUuidLoad() const override;
    NYT::NOrm::NClient::NObjects::TObjectId GetUuid(std::source_location location = std::source_location::current()) const override;
    TString GetName(std::source_location location = std::source_location::current()) const override;

    class TSpec
    {
    public:
        explicit TSpec(TNexus* object);

        using TSomeMapToMessageColumn_ = THashMap<ui64, NYT::NOrm::NExample::NClient::NProto::NDataModel::TNexusSpec::NestedMessage>;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TNexus, TSomeMapToMessageColumn_> SomeMapToMessageColumnDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TSomeMapToMessageColumn_>, SomeMapToMessageColumn);

        using TEtc = NYT::NOrm::NExample::NServer::NProto::NAutogen::TNexusSpecEtc;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TNexus, TEtc> EtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TSpec, Spec);

    class TStatus
    {
    public:
        explicit TStatus(TNexus* object);

        using TColumnSemaphore_ = NYT::NOrm::NDataModel::TEmbeddedSemaphore;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TNexus, TColumnSemaphore_> ColumnSemaphoreDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TColumnSemaphore_>, ColumnSemaphore);

        using TEtc = NYT::NOrm::NExample::NServer::NProto::NAutogen::TNexusStatusEtc;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TNexus, TEtc> EtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TStatus, Status);
};

////////////////////////////////////////////////////////////////////////////////

class TNirvanaDMProcessInstance
    : public TDataModelObject
    , public NYT::TRefTracked<TNirvanaDMProcessInstance>
{
public:
    static constexpr NYT::NOrm::NClient::NObjects::TObjectTypeValue Type =
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::NirvanaDMProcessInstance);

    TNirvanaDMProcessInstance(
        const TString& id,
        NYT::NOrm::NServer::NObjects::IObjectTypeHandler* typeHandler,
        NYT::NOrm::NServer::NObjects::ISession* session);

    virtual NYT::NOrm::NClient::NObjects::TObjectTypeValue GetType() const override
    {
        return Type;
    }

    virtual NYT::NOrm::NClient::NObjects::TObjectKey GetKey() const override;

protected:
    const TString Id_;

public:
    const TString& GetId() const
    {
        return Id_;
    }

    using TFinalizationStartTime_ = ui64;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TNirvanaDMProcessInstance, TFinalizationStartTime_> FinalizationStartTimeDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT_OVERRIDE(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFinalizationStartTime_>, FinalizationStartTime);

    using TFinalizers_ = THashMap<TString, NYT::NOrm::NDataModel::TFinalizer>;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TNirvanaDMProcessInstance, TFinalizers_> FinalizersDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT_OVERRIDE(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFinalizers_>, Finalizers);

    using TMetaEtc = typename NYT::NOrm::NExample::NServer::NProto::NAutogen::TNirvanaDMProcessInstanceMetaEtc;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TNirvanaDMProcessInstance, TMetaEtc> MetaEtcDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TMetaEtc>, MetaEtc);

    void ScheduleUuidLoad() const override;
    NYT::NOrm::NClient::NObjects::TObjectId GetUuid(std::source_location location = std::source_location::current()) const override;
    TString GetName(std::source_location location = std::source_location::current()) const override;

    class TSpec
    {
    public:
        explicit TSpec(TNirvanaDMProcessInstance* object);

        using TEtc = NYT::NOrm::NExample::NClient::NProto::NDataModel::TNirvanaDMProcessInstanceSpec;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TNirvanaDMProcessInstance, TEtc> EtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TSpec, Spec);

    class TStatus
    {
    public:
        explicit TStatus(TNirvanaDMProcessInstance* object);

        using TEtc = NYT::NOrm::NExample::NClient::NProto::NDataModel::TNirvanaDMProcessInstanceStatus;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TNirvanaDMProcessInstance, TEtc> EtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TStatus, Status);
};

////////////////////////////////////////////////////////////////////////////////

class TPublisher
    : public TDataModelObject
    , public NYT::TRefTracked<TPublisher>
{
public:
    static constexpr NYT::NOrm::NClient::NObjects::TObjectTypeValue Type =
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::Publisher);

    TPublisher(
        const i64& id,
        NYT::NOrm::NServer::NObjects::IObjectTypeHandler* typeHandler,
        NYT::NOrm::NServer::NObjects::ISession* session);

    virtual NYT::NOrm::NClient::NObjects::TObjectTypeValue GetType() const override
    {
        return Type;
    }

    virtual NYT::NOrm::NClient::NObjects::TObjectKey GetKey() const override;

protected:
    const i64 Id_;

public:
    const i64& GetId() const
    {
        return Id_;
    }

    using TBookAttribute =
        NYT::NOrm::NServer::NObjects::TChildrenAttribute<TBook>;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TBookAttribute, Books);

    using TIllustratorAttribute =
        NYT::NOrm::NServer::NObjects::TChildrenAttribute<TIllustrator>;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TIllustratorAttribute, Illustrators);

    using TTypographerAttribute =
        NYT::NOrm::NServer::NObjects::TChildrenAttribute<TTypographer>;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TTypographerAttribute, Typographers);

    using TFinalizationStartTime_ = ui64;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TPublisher, TFinalizationStartTime_> FinalizationStartTimeDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT_OVERRIDE(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFinalizationStartTime_>, FinalizationStartTime);

    using TFinalizers_ = THashMap<TString, NYT::NOrm::NDataModel::TFinalizer>;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TPublisher, TFinalizers_> FinalizersDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT_OVERRIDE(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFinalizers_>, Finalizers);

    using TMetaEtc = typename NYT::NOrm::NExample::NServer::NProto::NAutogen::TPublisherMetaEtc;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TPublisher, TMetaEtc> MetaEtcDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TMetaEtc>, MetaEtc);

    void ScheduleUuidLoad() const override;
    NYT::NOrm::NClient::NObjects::TObjectId GetUuid(std::source_location location = std::source_location::current()) const override;
    TString GetName(std::source_location location = std::source_location::current()) const override;

    class TSpec
    {
    public:
        explicit TSpec(TPublisher* object);

        using TName_ = TString;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TPublisher, TName_> NameDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TName_>, Name);

        using TEditorInChiefAttribute =
            NYT::NOrm::NServer::NObjects::TManyToOneAttribute<NYT::NOrm::NExample::NServer::NLibrary::TPublisher, NYT::NOrm::NExample::NServer::NLibrary::TEditor>;
        static const TEditorInChiefAttribute::TDescriptor EditorInChiefDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TEditorInChiefAttribute, EditorInChief);

        using TIllustratorInChiefAttribute =
            NYT::NOrm::NServer::NObjects::TManyToOneAttribute<NYT::NOrm::NExample::NServer::NLibrary::TPublisher, NYT::NOrm::NExample::NServer::NLibrary::TIllustrator>;
        static const TIllustratorInChiefAttribute::TDescriptor IllustratorInChiefDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TIllustratorInChiefAttribute, IllustratorInChief);

        using TPublisherGroupAttribute =
            NYT::NOrm::NServer::NObjects::TManyToOneAttribute<NYT::NOrm::NExample::NServer::NLibrary::TPublisher, NYT::NOrm::NExample::NServer::NLibrary::TPublisher>;
        static const TPublisherGroupAttribute::TDescriptor PublisherGroupDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TPublisherGroupAttribute, PublisherGroup);

        using TColumnField_ = i64;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TPublisher, TColumnField_> ColumnFieldDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TColumnField_>, ColumnField);

        using TFeaturedIllustratorsAttribute =
            NYT::NOrm::NServer::NObjects::TManyToManyInlineAttribute<NYT::NOrm::NExample::NServer::NLibrary::TPublisher, NYT::NOrm::NExample::NServer::NLibrary::TIllustrator>;
        static const TFeaturedIllustratorsAttribute::TDescriptor FeaturedIllustratorsDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TFeaturedIllustratorsAttribute, FeaturedIllustrators);

        using TEditorInChiefViewAttributeDescriptor =
            NYT::NOrm::NServer::NObjects::TManyToOneViewAttributeDescriptor<NYT::NOrm::NExample::NServer::NLibrary::TPublisher, NYT::NOrm::NExample::NServer::NLibrary::TEditor>;
        static const TEditorInChiefViewAttributeDescriptor EditorInChiefViewDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(
            NYT::NOrm::NServer::NObjects::TManyToOneViewAttribute,
            EditorInChiefView);

        using TIllustratorInChiefViewAttributeDescriptor =
            NYT::NOrm::NServer::NObjects::TManyToOneViewAttributeDescriptor<NYT::NOrm::NExample::NServer::NLibrary::TPublisher, NYT::NOrm::NExample::NServer::NLibrary::TIllustrator>;
        static const TIllustratorInChiefViewAttributeDescriptor IllustratorInChiefViewDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(
            NYT::NOrm::NServer::NObjects::TManyToOneViewAttribute,
            IllustratorInChiefView);

        using TPublisherGroupViewAttributeDescriptor =
            NYT::NOrm::NServer::NObjects::TManyToOneViewAttributeDescriptor<NYT::NOrm::NExample::NServer::NLibrary::TPublisher, NYT::NOrm::NExample::NServer::NLibrary::TPublisher>;
        static const TPublisherGroupViewAttributeDescriptor PublisherGroupViewDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(
            NYT::NOrm::NServer::NObjects::TManyToOneViewAttribute,
            PublisherGroupView);

        using TFeaturedIllustratorsViewAttributeDescriptor =
            NYT::NOrm::NServer::NObjects::TManyToManyInlineViewAttributeDescriptor<NYT::NOrm::NExample::NServer::NLibrary::TPublisher, NYT::NOrm::NExample::NServer::NLibrary::TIllustrator>;
        static const TFeaturedIllustratorsViewAttributeDescriptor FeaturedIllustratorsViewDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(
            NYT::NOrm::NServer::NObjects::TManyToManyInlineViewAttribute,
            FeaturedIllustratorsView);

        using TEtc = NYT::NOrm::NExample::NServer::NProto::NAutogen::TPublisherSpecEtc;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TPublisher, TEtc> EtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TSpec, Spec);

    class TStatus
    {
    public:
        explicit TStatus(TPublisher* object);

        using TColumnField_ = i64;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TPublisher, TColumnField_> ColumnFieldDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TColumnField_>, ColumnField);

        using TColumnList_ = std::vector<i64>;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TPublisher, TColumnList_> ColumnListDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TColumnList_>, ColumnList);

        using TFeaturedIllustratorsAttribute =
            NYT::NOrm::NServer::NObjects::TManyToManyInlineAttribute<NYT::NOrm::NExample::NServer::NLibrary::TPublisher, NYT::NOrm::NExample::NServer::NLibrary::TIllustrator>;
        static const TFeaturedIllustratorsAttribute::TDescriptor FeaturedIllustratorsDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TFeaturedIllustratorsAttribute, FeaturedIllustrators);

        using TBooksAttribute =
            NYT::NOrm::NServer::NObjects::TManyToManyTabularAttribute<NYT::NOrm::NExample::NServer::NLibrary::TPublisher, NYT::NOrm::NExample::NServer::NLibrary::TBook>;
        static const TBooksAttribute::TDescriptor BooksDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TBooksAttribute, Books);

        using TIllustratorsAttribute =
            NYT::NOrm::NServer::NObjects::TOneToManyAttribute<NYT::NOrm::NExample::NServer::NLibrary::TPublisher, NYT::NOrm::NExample::NServer::NLibrary::TIllustrator>;
        static const TIllustratorsAttribute::TDescriptor IllustratorsDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TIllustratorsAttribute, Illustrators);

        using TPublishersAttribute =
            NYT::NOrm::NServer::NObjects::TOneToManyAttribute<NYT::NOrm::NExample::NServer::NLibrary::TPublisher, NYT::NOrm::NExample::NServer::NLibrary::TPublisher>;
        static const TPublishersAttribute::TDescriptor PublishersDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TPublishersAttribute, Publishers);

        using TFeaturedIllustratorsViewAttributeDescriptor =
            NYT::NOrm::NServer::NObjects::TManyToManyInlineViewAttributeDescriptor<NYT::NOrm::NExample::NServer::NLibrary::TPublisher, NYT::NOrm::NExample::NServer::NLibrary::TIllustrator>;
        static const TFeaturedIllustratorsViewAttributeDescriptor FeaturedIllustratorsViewDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(
            NYT::NOrm::NServer::NObjects::TManyToManyInlineViewAttribute,
            FeaturedIllustratorsView);

        using TEtc = NYT::NOrm::NExample::NServer::NProto::NAutogen::TPublisherStatusEtc;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TPublisher, TEtc> EtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TStatus, Status);

    enum class EAttributeMigrations : int {
        SpecAddressToStatusAddress, // /spec/address to /status/address
        SpecNumberOfAwardsToStatusNumberOfAwards, // /spec/number_of_awards to /status/number_of_awards
        SpecNonColumnFieldToSpecColumnField, // /spec/non_column_field to /spec/column_field
        StatusColumnFieldToStatusNonColumnField, // /status/column_field to /status/non_column_field
        SpecFeaturedIllustratorsToStatusFeaturedIllustrators, // /spec/featured_illustrators to /status/featured_illustrators
        StatusColumnListToStatusNonColumnList, // /status/column_list to /status/non_column_list
    };
};

////////////////////////////////////////////////////////////////////////////////

class TRandomId
    : public TDataModelObject
    , public NYT::TRefTracked<TRandomId>
{
public:
    static constexpr NYT::NOrm::NClient::NObjects::TObjectTypeValue Type =
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::RandomId);

    TRandomId(
        const TString& strId,
        const i64& i64Id,
        const ui64& ui64Id,
        NYT::NOrm::NServer::NObjects::IObjectTypeHandler* typeHandler,
        NYT::NOrm::NServer::NObjects::ISession* session);

    virtual NYT::NOrm::NClient::NObjects::TObjectTypeValue GetType() const override
    {
        return Type;
    }

    virtual NYT::NOrm::NClient::NObjects::TObjectKey GetKey() const override;

protected:
    const TString StrId_;

public:
    const TString& GetStrId() const
    {
        return StrId_;
    }

protected:
    const i64 I64Id_;

public:
    const i64& GetI64Id() const
    {
        return I64Id_;
    }

protected:
    const ui64 Ui64Id_;

public:
    const ui64& GetUi64Id() const
    {
        return Ui64Id_;
    }

    using TFinalizationStartTime_ = ui64;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TRandomId, TFinalizationStartTime_> FinalizationStartTimeDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT_OVERRIDE(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFinalizationStartTime_>, FinalizationStartTime);

    using TFinalizers_ = THashMap<TString, NYT::NOrm::NDataModel::TFinalizer>;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TRandomId, TFinalizers_> FinalizersDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT_OVERRIDE(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFinalizers_>, Finalizers);

    using TMetaEtc = typename NYT::NOrm::NExample::NServer::NProto::NAutogen::TRandomIdMetaEtc;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TRandomId, TMetaEtc> MetaEtcDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TMetaEtc>, MetaEtc);

    void ScheduleUuidLoad() const override;
    NYT::NOrm::NClient::NObjects::TObjectId GetUuid(std::source_location location = std::source_location::current()) const override;
    TString GetName(std::source_location location = std::source_location::current()) const override;

    class TSpec
    {
    public:
        explicit TSpec(TRandomId* object);

        using TStrValue_ = TString;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TRandomId, TStrValue_> StrValueDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TStrValue_>, StrValue);

        using TEtc = NYT::NOrm::NExample::NServer::NProto::NAutogen::TRandomIdSpecEtc;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TRandomId, TEtc> EtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TSpec, Spec);

    class TStatus
    {
    public:
        explicit TStatus(TRandomId* object);

        using TEtc = NYT::NOrm::NExample::NClient::NProto::NDataModel::TRandomIdStatus;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TRandomId, TEtc> EtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TStatus, Status);
};

////////////////////////////////////////////////////////////////////////////////

class TSchema
    : public TDataModelObject
    , public NYT::TRefTracked<TSchema>
{
public:
    static constexpr NYT::NOrm::NClient::NObjects::TObjectTypeValue Type =
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::Schema);

    TSchema(
        const TString& id,
        NYT::NOrm::NServer::NObjects::IObjectTypeHandler* typeHandler,
        NYT::NOrm::NServer::NObjects::ISession* session);

    virtual NYT::NOrm::NClient::NObjects::TObjectTypeValue GetType() const override
    {
        return Type;
    }

    virtual NYT::NOrm::NClient::NObjects::TObjectKey GetKey() const override;

protected:
    const TString Id_;

public:
    const TString& GetId() const
    {
        return Id_;
    }

    using TFinalizationStartTime_ = ui64;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TSchema, TFinalizationStartTime_> FinalizationStartTimeDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT_OVERRIDE(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFinalizationStartTime_>, FinalizationStartTime);

    using TFinalizers_ = THashMap<TString, NYT::NOrm::NDataModel::TFinalizer>;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TSchema, TFinalizers_> FinalizersDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT_OVERRIDE(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFinalizers_>, Finalizers);

    using TMetaEtc = typename NYT::NOrm::NExample::NServer::NProto::NAutogen::TSchemaMetaEtc;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TSchema, TMetaEtc> MetaEtcDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TMetaEtc>, MetaEtc);

    void ScheduleUuidLoad() const override;
    NYT::NOrm::NClient::NObjects::TObjectId GetUuid(std::source_location location = std::source_location::current()) const override;
    TString GetName(std::source_location location = std::source_location::current()) const override;

    class TSpec
    {
    public:
        explicit TSpec(TSchema* object);

        using TEtc = NYT::NOrm::NExample::NClient::NProto::NDataModel::TSchemaSpec;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TSchema, TEtc> EtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TSpec, Spec);

    class TStatus
    {
    public:
        explicit TStatus(TSchema* object);

        using TEtc = NYT::NOrm::NExample::NClient::NProto::NDataModel::TSchemaStatus;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TSchema, TEtc> EtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TStatus, Status);
};

////////////////////////////////////////////////////////////////////////////////

class TSemaphore
    : public TDataModelObject
    , public NYT::TRefTracked<TSemaphore>
{
public:
    static constexpr NYT::NOrm::NClient::NObjects::TObjectTypeValue Type =
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::Semaphore);

    TSemaphore(
        const TString& id,
        const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
        NYT::NOrm::NServer::NObjects::IObjectTypeHandler* typeHandler,
        NYT::NOrm::NServer::NObjects::ISession* session);

    virtual NYT::NOrm::NClient::NObjects::TObjectTypeValue GetType() const override
    {
        return Type;
    }

    virtual NYT::NOrm::NClient::NObjects::TObjectKey GetKey() const override;

protected:
    const TString Id_;

public:
    const TString& GetId() const
    {
        return Id_;
    }

private:
    NYT::NOrm::NServer::NObjects::TParentKeyAttribute ParentKeyAttribute_;

public:
    NYT::NOrm::NClient::NObjects::TObjectKey GetParentKey(std::source_location location = std::source_location::current()) const override;
    NYT::NOrm::NServer::NObjects::TParentKeyAttribute* GetParentKeyAttribute() override;
    TString SemaphoreSetId(std::source_location location = std::source_location::current()) const;

    using TSemaphoreSetAttribute =
        NYT::NOrm::NServer::NObjects::TParentAttribute<TSemaphoreSet>;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TSemaphoreSetAttribute, SemaphoreSet);

    using TFinalizationStartTime_ = ui64;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TSemaphore, TFinalizationStartTime_> FinalizationStartTimeDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT_OVERRIDE(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFinalizationStartTime_>, FinalizationStartTime);

    using TFinalizers_ = THashMap<TString, NYT::NOrm::NDataModel::TFinalizer>;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TSemaphore, TFinalizers_> FinalizersDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT_OVERRIDE(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFinalizers_>, Finalizers);

    using TMetaEtc = typename NYT::NOrm::NExample::NServer::NProto::NAutogen::TSemaphoreMetaEtc;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TSemaphore, TMetaEtc> MetaEtcDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TMetaEtc>, MetaEtc);

    void ScheduleUuidLoad() const override;
    NYT::NOrm::NClient::NObjects::TObjectId GetUuid(std::source_location location = std::source_location::current()) const override;
    TString GetName(std::source_location location = std::source_location::current()) const override;

    class TSpec
    {
    public:
        explicit TSpec(TSemaphore* object);

        using TEtc = NYT::NOrm::NDataModel::TSemaphoreSpec;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TSemaphore, TEtc> EtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TSpec, Spec);

    class TStatus
    {
    public:
        explicit TStatus(TSemaphore* object);

        using TEtc = NYT::NOrm::NDataModel::TSemaphoreStatus;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TSemaphore, TEtc> EtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TStatus, Status);
};

////////////////////////////////////////////////////////////////////////////////

class TSemaphoreSet
    : public TDataModelObject
    , public NYT::TRefTracked<TSemaphoreSet>
{
public:
    static constexpr NYT::NOrm::NClient::NObjects::TObjectTypeValue Type =
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::SemaphoreSet);

    TSemaphoreSet(
        const TString& id,
        NYT::NOrm::NServer::NObjects::IObjectTypeHandler* typeHandler,
        NYT::NOrm::NServer::NObjects::ISession* session);

    virtual NYT::NOrm::NClient::NObjects::TObjectTypeValue GetType() const override
    {
        return Type;
    }

    virtual NYT::NOrm::NClient::NObjects::TObjectKey GetKey() const override;

protected:
    const TString Id_;

public:
    const TString& GetId() const
    {
        return Id_;
    }

    using TSemaphoreAttribute =
        NYT::NOrm::NServer::NObjects::TChildrenAttribute<TSemaphore>;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TSemaphoreAttribute, Semaphores);

    using TFinalizationStartTime_ = ui64;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TSemaphoreSet, TFinalizationStartTime_> FinalizationStartTimeDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT_OVERRIDE(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFinalizationStartTime_>, FinalizationStartTime);

    using TFinalizers_ = THashMap<TString, NYT::NOrm::NDataModel::TFinalizer>;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TSemaphoreSet, TFinalizers_> FinalizersDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT_OVERRIDE(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFinalizers_>, Finalizers);

    using TMetaEtc = typename NYT::NOrm::NExample::NServer::NProto::NAutogen::TSemaphoreSetMetaEtc;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TSemaphoreSet, TMetaEtc> MetaEtcDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TMetaEtc>, MetaEtc);

    void ScheduleUuidLoad() const override;
    NYT::NOrm::NClient::NObjects::TObjectId GetUuid(std::source_location location = std::source_location::current()) const override;
    TString GetName(std::source_location location = std::source_location::current()) const override;

    class TSpec
    {
    public:
        explicit TSpec(TSemaphoreSet* object);

        using TEtc = NYT::NOrm::NDataModel::TSemaphoreSetSpec;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TSemaphoreSet, TEtc> EtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TSpec, Spec);

    class TStatus
    {
    public:
        explicit TStatus(TSemaphoreSet* object);

        using TEtc = NYT::NOrm::NExample::NClient::NProto::NDataModel::TSemaphoreSetStatus;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TSemaphoreSet, TEtc> EtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TStatus, Status);
};

////////////////////////////////////////////////////////////////////////////////

class TTimestampId
    : public TDataModelObject
    , public NYT::TRefTracked<TTimestampId>
{
public:
    static constexpr NYT::NOrm::NClient::NObjects::TObjectTypeValue Type =
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::TimestampId);

    TTimestampId(
        const i64& i64Id,
        const ui64& ui64Id,
        NYT::NOrm::NServer::NObjects::IObjectTypeHandler* typeHandler,
        NYT::NOrm::NServer::NObjects::ISession* session);

    virtual NYT::NOrm::NClient::NObjects::TObjectTypeValue GetType() const override
    {
        return Type;
    }

    virtual NYT::NOrm::NClient::NObjects::TObjectKey GetKey() const override;

protected:
    const i64 I64Id_;

public:
    const i64& GetI64Id() const
    {
        return I64Id_;
    }

protected:
    const ui64 Ui64Id_;

public:
    const ui64& GetUi64Id() const
    {
        return Ui64Id_;
    }

    using TFinalizationStartTime_ = ui64;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TTimestampId, TFinalizationStartTime_> FinalizationStartTimeDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT_OVERRIDE(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFinalizationStartTime_>, FinalizationStartTime);

    using TFinalizers_ = THashMap<TString, NYT::NOrm::NDataModel::TFinalizer>;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TTimestampId, TFinalizers_> FinalizersDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT_OVERRIDE(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFinalizers_>, Finalizers);

    using TMetaEtc = typename NYT::NOrm::NExample::NServer::NProto::NAutogen::TTimestampIdMetaEtc;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TTimestampId, TMetaEtc> MetaEtcDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TMetaEtc>, MetaEtc);

    void ScheduleUuidLoad() const override;
    NYT::NOrm::NClient::NObjects::TObjectId GetUuid(std::source_location location = std::source_location::current()) const override;
    TString GetName(std::source_location location = std::source_location::current()) const override;

    class TSpec
    {
    public:
        explicit TSpec(TTimestampId* object);

        using TUi64Value_ = ui64;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TTimestampId, TUi64Value_> Ui64ValueDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TUi64Value_>, Ui64Value);

        using TEtc = NYT::NOrm::NExample::NServer::NProto::NAutogen::TTimestampIdSpecEtc;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TTimestampId, TEtc> EtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TSpec, Spec);

    class TStatus
    {
    public:
        explicit TStatus(TTimestampId* object);

        using TEtc = NYT::NOrm::NExample::NClient::NProto::NDataModel::TTimestampIdStatus;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TTimestampId, TEtc> EtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TStatus, Status);
};

////////////////////////////////////////////////////////////////////////////////

class TTypographer
    : public TDataModelObject
    , public NYT::TRefTracked<TTypographer>
{
public:
    static constexpr NYT::NOrm::NClient::NObjects::TObjectTypeValue Type =
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::Typographer);

    TTypographer(
        const i64& id,
        const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
        NYT::NOrm::NServer::NObjects::IObjectTypeHandler* typeHandler,
        NYT::NOrm::NServer::NObjects::ISession* session);

    virtual NYT::NOrm::NClient::NObjects::TObjectTypeValue GetType() const override
    {
        return Type;
    }

    virtual NYT::NOrm::NClient::NObjects::TObjectKey GetKey() const override;

protected:
    const i64 Id_;

public:
    const i64& GetId() const
    {
        return Id_;
    }

private:
    NYT::NOrm::NServer::NObjects::TParentKeyAttribute ParentKeyAttribute_;

public:
    NYT::NOrm::NClient::NObjects::TObjectKey GetParentKey(std::source_location location = std::source_location::current()) const override;
    NYT::NOrm::NServer::NObjects::TParentKeyAttribute* GetParentKeyAttribute() override;
    i64 PublisherId(std::source_location location = std::source_location::current()) const;

    using TPublisherAttribute =
        NYT::NOrm::NServer::NObjects::TParentAttribute<TPublisher>;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TPublisherAttribute, Publisher);

    using TLogin_ = TString;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TTypographer, TLogin_> LoginDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TLogin_>, Login);

    using TFinalizationStartTime_ = ui64;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TTypographer, TFinalizationStartTime_> FinalizationStartTimeDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT_OVERRIDE(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFinalizationStartTime_>, FinalizationStartTime);

    using TFinalizers_ = THashMap<TString, NYT::NOrm::NDataModel::TFinalizer>;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TTypographer, TFinalizers_> FinalizersDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT_OVERRIDE(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFinalizers_>, Finalizers);

    using TMetaEtc = typename NYT::NOrm::NExample::NServer::NProto::NAutogen::TTypographerMetaEtc;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TTypographer, TMetaEtc> MetaEtcDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TMetaEtc>, MetaEtc);

    void ScheduleUuidLoad() const override;
    NYT::NOrm::NClient::NObjects::TObjectId GetUuid(std::source_location location = std::source_location::current()) const override;
    TString GetName(std::source_location location = std::source_location::current()) const override;

    class TSpec
    {
    public:
        explicit TSpec(TTypographer* object);

        using TTestMandatoryColumnField_ = TString;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TTypographer, TTestMandatoryColumnField_> TestMandatoryColumnFieldDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TTestMandatoryColumnField_>, TestMandatoryColumnField);

        using TEtc = NYT::NOrm::NExample::NServer::NProto::NAutogen::TTypographerSpecEtc;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TTypographer, TEtc> EtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TSpec, Spec);

    class TStatus
    {
    public:
        explicit TStatus(TTypographer* object);

        using TEtc = NYT::NOrm::NExample::NClient::NProto::NDataModel::TTypographerStatus;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TTypographer, TEtc> EtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TStatus, Status);
};

////////////////////////////////////////////////////////////////////////////////

class TUser
    : public TDataModelObject
    , public NYT::TRefTracked<TUser>
{
public:
    static constexpr NYT::NOrm::NClient::NObjects::TObjectTypeValue Type =
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::User);

    TUser(
        const TString& id,
        NYT::NOrm::NServer::NObjects::IObjectTypeHandler* typeHandler,
        NYT::NOrm::NServer::NObjects::ISession* session);

    virtual NYT::NOrm::NClient::NObjects::TObjectTypeValue GetType() const override
    {
        return Type;
    }

    virtual NYT::NOrm::NClient::NObjects::TObjectKey GetKey() const override;

protected:
    const TString Id_;

public:
    const TString& GetId() const
    {
        return Id_;
    }

    using TFinalizationStartTime_ = ui64;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TUser, TFinalizationStartTime_> FinalizationStartTimeDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT_OVERRIDE(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFinalizationStartTime_>, FinalizationStartTime);

    using TFinalizers_ = THashMap<TString, NYT::NOrm::NDataModel::TFinalizer>;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TUser, TFinalizers_> FinalizersDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT_OVERRIDE(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFinalizers_>, Finalizers);

    using TMetaEtc = typename NYT::NOrm::NExample::NServer::NProto::NAutogen::TUserMetaEtc;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TUser, TMetaEtc> MetaEtcDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TMetaEtc>, MetaEtc);

    void ScheduleUuidLoad() const override;
    NYT::NOrm::NClient::NObjects::TObjectId GetUuid(std::source_location location = std::source_location::current()) const override;
    TString GetName(std::source_location location = std::source_location::current()) const override;

    class TSpec
    {
    public:
        explicit TSpec(TUser* object);

        using TEtc = NYT::NOrm::NExample::NClient::NProto::NDataModel::TUserSpec;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TUser, TEtc> EtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TSpec, Spec);

    class TStatus
    {
    public:
        explicit TStatus(TUser* object);

        using TEtc = NYT::NOrm::NExample::NClient::NProto::NDataModel::TUserStatus;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TUser, TEtc> EtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TStatus, Status);
};

////////////////////////////////////////////////////////////////////////////////

class TWatchLogConsumer
    : public TDataModelObject
    , public NYT::TRefTracked<TWatchLogConsumer>
{
public:
    static constexpr NYT::NOrm::NClient::NObjects::TObjectTypeValue Type =
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::WatchLogConsumer);

    TWatchLogConsumer(
        const TString& id,
        NYT::NOrm::NServer::NObjects::IObjectTypeHandler* typeHandler,
        NYT::NOrm::NServer::NObjects::ISession* session);

    virtual NYT::NOrm::NClient::NObjects::TObjectTypeValue GetType() const override
    {
        return Type;
    }

    virtual NYT::NOrm::NClient::NObjects::TObjectKey GetKey() const override;

protected:
    const TString Id_;

public:
    const TString& GetId() const
    {
        return Id_;
    }

    using TFinalizationStartTime_ = ui64;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TWatchLogConsumer, TFinalizationStartTime_> FinalizationStartTimeDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT_OVERRIDE(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFinalizationStartTime_>, FinalizationStartTime);

    using TFinalizers_ = THashMap<TString, NYT::NOrm::NDataModel::TFinalizer>;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TWatchLogConsumer, TFinalizers_> FinalizersDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT_OVERRIDE(NYT::NOrm::NServer::NObjects::TScalarAttribute<TFinalizers_>, Finalizers);

    using TMetaEtc = typename NYT::NOrm::NExample::NServer::NProto::NAutogen::TWatchLogConsumerMetaEtc;
    static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TWatchLogConsumer, TMetaEtc> MetaEtcDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TMetaEtc>, MetaEtc);

    void ScheduleUuidLoad() const override;
    NYT::NOrm::NClient::NObjects::TObjectId GetUuid(std::source_location location = std::source_location::current()) const override;
    TString GetName(std::source_location location = std::source_location::current()) const override;

    class TSpec
    {
    public:
        explicit TSpec(TWatchLogConsumer* object);

        using TEtc = NYT::NOrm::NExample::NClient::NProto::NDataModel::TWatchLogConsumerSpec;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TWatchLogConsumer, TEtc> EtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TSpec, Spec);

    class TStatus
    {
    public:
        explicit TStatus(TWatchLogConsumer* object);

        using TEtc = NYT::NOrm::NExample::NClient::NProto::NDataModel::TWatchLogConsumerStatus;
        static const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TWatchLogConsumer, TEtc> EtcDescriptor;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(NYT::NOrm::NServer::NObjects::TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TStatus, Status);
};

////////////////////////////////////////////////////////////////////////////////

using TObjectTypes = ::NYT::NOrm::NMpl::TTypes<
    TAuthor,
    TBook,
    TBufferedTimestampId,
    TCat,
    TEditor,
    TEmployer,
    TExecutor,
    TGenre,
    TGroup,
    THitchhiker,
    TIllustrator,
    TIndexedIncrementId,
    TInterceptor,
    TManualId,
    TMotherShip,
    TMultipolicyId,
    TNestedColumns,
    TNexus,
    TNirvanaDMProcessInstance,
    TPublisher,
    TRandomId,
    TSchema,
    TSemaphore,
    TSemaphoreSet,
    TTimestampId,
    TTypographer,
    TUser,
    TWatchLogConsumer>;

template <class T>
concept CObjectType = ::NYT::NOrm::NMpl::COneOfTypes<T, TObjectTypes>;

//! Helper to get ObjectType by runtime enum type.
//! TInvokable must have signature `void <typename T>()`,
template <::NYT::NOrm::NMpl::CInvocableForEachType<TObjectTypes> TInvokable>
bool InvokeForObjectType(EObjectType objectType, TInvokable&& consumer)
{
    bool found = false;
    TObjectTypes::ForEach([&] <CObjectType T> {
        if (T::Type == static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(objectType)) {
            found = true;
            consumer.template operator()<T>();
        }
    });
    return found;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NExample::NServer::NLibrary
