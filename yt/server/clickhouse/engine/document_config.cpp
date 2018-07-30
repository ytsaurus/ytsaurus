#include "document_config.h"

#include "type_helpers.h"

#include <Common/Exception.h>

#include <util/string/cast.h>
#include <util/string/split.h>

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

class TDocumentConfig
    : public Poco::Util::AbstractConfiguration
{
private:
    NInterop::IDocumentPtr Document;

public:
    TDocumentConfig(NInterop::IDocumentPtr document)
        : Document(std::move(document))
    {}

    bool getRaw(const std::string& key, std::string& value) const override;
    void setRaw(const std::string& key, const std::string& value) override;
    void enumerate(const std::string& key, Keys& range) const override;

private:
    static NInterop::TDocumentPath ToPath(const std::string& key);
    static std::string RepresentAsString(const NInterop::TValue& value);
};

////////////////////////////////////////////////////////////////////////////////

NInterop::TDocumentPath TDocumentConfig::ToPath(const std::string& key)
{
    TVector<TString> keys;
    Split(ToString(key), ".", keys);
    return { keys.begin(), keys.end() };
}

std::string TDocumentConfig::RepresentAsString(const NInterop::TValue& value)
{
    switch (value.Type) {
        case NInterop::EValueType::Int:
            return std::to_string(value.Int);
        case NInterop::EValueType::UInt:
            return std::to_string(value.UInt);
        case NInterop::EValueType::Float:
            return std::to_string(value.Float);
        case NInterop::EValueType::Boolean:
            return value.Boolean ? "true" : "false";
        case NInterop::EValueType::String:
            return { value.String, value.Length };
        default:
            throw Poco::Exception(
                "Cannot represent document value as string: "
                "unsupported value type: " + ToStdString(::ToString(value.Type)));
    }

    Y_UNREACHABLE();
}

bool TDocumentConfig::getRaw(const std::string& key, std::string& value) const
{
    const auto path = ToPath(key);
    if (Document->Has(path)) {
        const auto subDocument = Document->GetSubDocument(path);
        if (subDocument->IsComposite()) {
            // workaround for hasProperty method of Poco::Util::AbstractConfiguration
            value = ToStdString(subDocument->Serialize());
        } else {
            value = RepresentAsString(subDocument->AsValue());
        }
        return true;
    }
    return false;
}

void TDocumentConfig::setRaw(const std::string& key, const std::string& value)
{
    // TODO
    throw Poco::NotImplementedException(
        "Modification of documents is not supported yet");
}

void TDocumentConfig::enumerate(const std::string& key, Keys& range) const
{
    const auto path = ToPath(key);
    if (Document->Has(path)) {
        range = ToStdString(Document->ListKeys(path));
    }
}

////////////////////////////////////////////////////////////////////////////////

Poco::AutoPtr<Poco::Util::AbstractConfiguration> CreateDocumentConfig(
    NInterop::IDocumentPtr document)
{
    return new TDocumentConfig(std::move(document));
}

} // namespace NClickHouse
} // namespace NYT
