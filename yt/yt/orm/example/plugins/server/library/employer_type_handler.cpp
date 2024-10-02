#include <yt/yt/orm/example/server/library/autogen/type_handler_impls.h>

#include <yt/yt/orm/server/objects/attribute_policy.h>

namespace NYT::NOrm::NExample::NServer::NPlugins {

////////////////////////////////////////////////////////////////////////////////

namespace {

using TGeneratedType = NLibrary::TEmployer::TStatus::TSalary_;

class TCustomEmployerSalaryPolicy
    : public NOrm::NServer::NObjects::IAttributePolicy<TGeneratedType>
{
public:
    NOrm::NServer::NObjects::EAttributeGenerationPolicy GetGenerationPolicy() const override {
        return NOrm::NServer::NObjects::EAttributeGenerationPolicy::Custom;
    }

    void Validate(
        const TGeneratedType& keyField, std::string_view /*title*/) const override
    {
        THROW_ERROR_EXCEPTION_UNLESS(keyField >= 0,
            "Employer salary field validation failed, salary cannot be less than zero")
    }

    TGeneratedType Generate(
        NOrm::NServer::NObjects::TTransaction* /*transaction*/, std::string_view /*title*/) const override
    {
        return 123321;
    }
};

}  // namespace

////////////////////////////////////////////////////////////////////////////////

class TEmployerTypeHandler
    : public NLibrary::TEmployerTypeHandler
{
public:
    using NLibrary::TEmployerTypeHandler::TEmployerTypeHandler;

    void Initialize() override
    {
        NLibrary::TEmployerTypeHandler::Initialize();

        StatusAttributeSchema_->FindChild("salary")->AsScalar()
            ->SetPolicy<NLibrary::TEmployer, TGeneratedType>(
                NLibrary::TEmployer::TStatus::SalaryDescriptor,
                New<TCustomEmployerSalaryPolicy>());
    }
};

std::unique_ptr<NYT::NOrm::NServer::NObjects::IObjectTypeHandler> CreateEmployerTypeHandler(
    NYT::NOrm::NServer::NMaster::IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
{
    return std::make_unique<TEmployerTypeHandler>(bootstrap, config);
}

////////////////////////////////////////////////////////////////////////////////

}  // namespace NYT::NOrm::NExample::NServer::NPlugins
