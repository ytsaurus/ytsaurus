#include <yt/yt/orm/example/server/library/autogen/type_handler_impls.h>

#include <yt/yt/orm/server/objects/attribute_policy.h>
#include <yt/yt/orm/server/objects/transaction.h>

namespace NYT::NOrm::NExample::NServer::NPlugins {

////////////////////////////////////////////////////////////////////////////////

namespace {

class TCustomGenreIdPolicy
    : public NOrm::NServer::NObjects::IAttributePolicy<TString>
{
public:
    NOrm::NServer::NObjects::EAttributeGenerationPolicy GetGenerationPolicy() const override
    {
        return NOrm::NServer::NObjects::EAttributeGenerationPolicy::Custom;
    }

    void Validate(
        const TString& keyField, std::string_view /*title*/) const override
    {
        THROW_ERROR_EXCEPTION_UNLESS(keyField.length() < 100500,
            "Genre id field validation failed, id length cannot be greater than 100500")
    }

    TString Generate(
        NOrm::NServer::NObjects::TTransaction* /*transaction*/, std::string_view /*title*/) const override
    {
        return NOrm::NServer::NObjects::RandomStringId() +
            NOrm::NServer::NObjects::RandomStringId();
    }
};

}  // namespace

////////////////////////////////////////////////////////////////////////////////

class TGenreTypeHandler
    : public NLibrary::TGenreTypeHandler
{
public:
    using NLibrary::TGenreTypeHandler::TGenreTypeHandler;

    void Initialize() override
    {
        NLibrary::TGenreTypeHandler::Initialize();

        MetaAttributeSchema_->FindChild("id")->AsScalar()
            ->SetKeyFieldPolicy<TString>(
                New<TCustomGenreIdPolicy>());

        SpecAttributeSchema_
            ->FindChild("is_name_substring")
            ->AsScalar()
            ->SetConstantChangedGetter(false)
            ->SetValueGetter<NLibrary::TGenre>(
                std::bind_front(&TGenreTypeHandler::EvaluateIsNameSubstring, this))
            ->SetPreloader<NLibrary::TGenre>(
                std::bind_front(&TGenreTypeHandler::PreloadIsNameSubstring, this));
    }

private:
    void PreloadIsNameSubstring(const NLibrary::TGenre* genre)
    {
        genre->Spec().Etc().ScheduleLoad();
    }

    void EvaluateIsNameSubstring(
        NOrm::NServer::NObjects::TTransaction* /*transaction*/,
        const NLibrary::TGenre* genre,
        NYson::IYsonConsumer* consumer,
        const NYPath::TYPath& path)
    {
        NYPath::TTokenizer tokenizer(path);
        tokenizer.Skip(NYPath::ETokenType::StartOfStream);
        tokenizer.Skip(NYPath::ETokenType::Slash);
        tokenizer.Expect(NYPath::ETokenType::Literal);
        auto string = tokenizer.GetLiteralValue();
        tokenizer.Advance();
        tokenizer.Expect(NYPath::ETokenType::EndOfStream);
        consumer->OnBooleanScalar(genre->Spec().Etc().Load().name().find(string) != TString::npos);
    }
};

std::unique_ptr<NOrm::NServer::NObjects::IObjectTypeHandler> CreateGenreTypeHandler(
    NOrm::NServer::NMaster::IBootstrap* bootstrap,
    NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
{
    return std::make_unique<TGenreTypeHandler>(bootstrap, config);
}

////////////////////////////////////////////////////////////////////////////////

}  // namespace NYT::NOrm::NExample::NServer::NPlugins
