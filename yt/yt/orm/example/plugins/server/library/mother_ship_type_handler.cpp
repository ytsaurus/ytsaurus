#include "mother_ship.h"

#include <yt/yt/orm/example/server/library/autogen/type_handler_impls.h>

namespace NYT::NOrm::NExample::NServer::NPlugins {

////////////////////////////////////////////////////////////////////////////////

class TMotherShipTypeHandler
    : public NLibrary::TMotherShipTypeHandler
{
public:
    using NLibrary::TMotherShipTypeHandler::TMotherShipTypeHandler;

    void Initialize() override
    {
        NLibrary::TMotherShipTypeHandler::Initialize();

        SpecAttributeSchema_->GetEtcChild()
            ->AddUpdatePrehandler<NLibrary::TMotherShip>(
                std::bind_front(&TMotherShipTypeHandler::PreloadInterceptors, this))
            ->AddValidator<TMotherShip>(
                std::bind_front(&TMotherShipTypeHandler::ValidateSpecEtc, this));
        SetAttributeSensorValueTransform("/spec/templar_count", [] (NYTree::INodePtr value) {
            if (value->GetType() == NYTree::ENodeType::Entity) {
                return "bad value";
            }
            i64 count = value->AsInt64()->GetValue();
            if (count < 0) {
                return "bad value";
            } else if (count == 0) {
                return "none";
            } else if (count < 5) {
                return "few";
            } else if (count < 10) {
                return "several";
            } else if (count < 20) {
                return "pack";
            } else if (count < 50) {
                return "lots";
            } else if (count < 100) {
                return "horde";
            } else if (count < 250) {
                return "throng";
            } else if (count < 500) {
                return "swarm";
            } else if (count < 1000) {
                return "zounds";
            } else {
                return "legion";
            }
        });
        MetaAttributeSchema_->FindChild("release_year")
            ->AddUpdateHandler<NLibrary::TMotherShip>(
                [] (NYT::NOrm::NServer::NObjects::TTransaction* /*transaction*/, NLibrary::TMotherShip* motherShip) {
                    if (motherShip->ReleaseYear() == 1777) {
                        motherShip->Spec().Price().Store(7777777);
                    } else if (motherShip->ReleaseYear() == 1888) {
                        *motherShip->Spec().Price().MutableLoad() = 8888888;
                    }
                });
    }

    void PreloadInterceptors(
        NYT::NOrm::NServer::NObjects::TTransaction* /*transaction*/,
        const NLibrary::TMotherShip* motherShip)
    {
        motherShip->As<TMotherShip>()->PreloadCheck();
    }

    void ValidateSpecEtc(
        NYT::NOrm::NServer::NObjects::TTransaction* /*transaction*/,
        const TMotherShip* motherShip)
    {
        motherShip->CheckEnoughInterceptors();
    }
};

std::unique_ptr<NYT::NOrm::NServer::NObjects::IObjectTypeHandler> CreateMotherShipTypeHandler(
    NYT::NOrm::NServer::NMaster::IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
{
    return std::make_unique<TMotherShipTypeHandler>(bootstrap, config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NExample::NServer::NPlugins
