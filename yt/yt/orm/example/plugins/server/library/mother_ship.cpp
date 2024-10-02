#include "mother_ship.h"

namespace NYT::NOrm::NExample::NServer::NPlugins {

////////////////////////////////////////////////////////////////////////////////

void TMotherShip::PreloadCheck() const
{
    Spec().Etc().ScheduleLoad();
    Interceptors().ScheduleLoad();
}

void TMotherShip::CheckEnoughInterceptors() const
{
    ui64 templarCount = Spec().Etc().Load().templar_count();
    std::vector<NLibrary::TInterceptor*> interceptors = Interceptors().Load();

    THROW_ERROR_EXCEPTION_IF(templarCount > interceptors.size(),
        NYT::NOrm::NClient::EErrorCode::InvalidObjectSpec,
        "Templar count %v out of bounds",
        templarCount);
}

std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject> CreateMotherShip(
    const i64& id,
    const NOrm::NClient::NObjects::TObjectKey& parentKey,
    NOrm::NServer::NObjects::IObjectTypeHandler* typeHandler,
    NOrm::NServer::NObjects::ISession* session)
{
    return std::make_unique<TMotherShip>(id, parentKey, typeHandler, session);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NExample::NServer::NPlugins
