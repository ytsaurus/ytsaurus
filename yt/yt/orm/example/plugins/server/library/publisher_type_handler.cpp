#include <yt/yt/orm/example/server/library/autogen/type_handler_impls.h>

#include <util/string/cast.h>

namespace NYT::NOrm::NExample::NServer::NPlugins {

////////////////////////////////////////////////////////////////////////////////

class TPublisherTypeHandler
    : public NLibrary::TPublisherTypeHandler
{
public:
    using NLibrary::TPublisherTypeHandler::TPublisherTypeHandler;

protected:

    void DoPrepareSpecNumberOfAwardsToStatusNumberOfAwardsMigration(NLibrary::TPublisher* /*object*/) override
    { }

    void DoFinalizeSpecNumberOfAwardsToStatusNumberOfAwardsMigration(NLibrary::TPublisher* object) override
    {
        i64 numberOfAwards = 0;
        if (!TryFromString(object->Spec().Etc().Load().number_of_awards(), numberOfAwards)) {
            numberOfAwards = -1;
        }
        object->Status().Etc().MutableLoad()->set_number_of_awards(numberOfAwards);
    }

    void DoReverseSpecNumberOfAwardsToStatusNumberOfAwardsMigration(NLibrary::TPublisher* object) override
    {
        i64 numberOfAwards = object->Status().Etc().Load().number_of_awards();
        object->Spec().Etc().MutableLoad()->set_number_of_awards(ToString(numberOfAwards));
    }

    void DoPrepareSpecFeaturedIllustratorsToStatusFeaturedIllustratorsMigration(NLibrary::TPublisher* /*object*/) override
    { }

    void DoFinalizeSpecFeaturedIllustratorsToStatusFeaturedIllustratorsMigration(NLibrary::TPublisher* object) override
    {
        object->Status().FeaturedIllustrators().Store(object->Spec().FeaturedIllustrators().Load());
    }

    void DoReverseSpecFeaturedIllustratorsToStatusFeaturedIllustratorsMigration(NLibrary::TPublisher* object) override
    {
        object->Spec().FeaturedIllustrators().Store(object->Status().FeaturedIllustrators().Load());
    }
};

std::unique_ptr<NYT::NOrm::NServer::NObjects::IObjectTypeHandler> CreatePublisherTypeHandler(
    NYT::NOrm::NServer::NMaster::IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
{
    return std::make_unique<TPublisherTypeHandler>(bootstrap, config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NExample::NServer::NPlugins
