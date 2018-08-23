#include "path.h"

#include <yt/core/misc/error.h>

#include <util/string/builder.h>

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

class TYPathService
    : public NInterop::IPathService
{
public:
    TString Join(
        const TString& base,
        const TString& relative) const override;

    TString Build(
        const TString& base,
        std::vector<TString> relativeKeys) const override;

private:
    static void ValidateBase(const TString& base);
    static void ValidateRelative(const TString& relative);
    static void ValidateKey(const TString& key);
};

////////////////////////////////////////////////////////////////////////////////

TString TYPathService::Join(const TString& base, const TString& relative) const
{
    ValidateBase(base);
    ValidateRelative(relative);

    return base + relative;
}

TString TYPathService::Build(
    const TString& base,
    std::vector<TString> relativeKeys) const
{
    ValidateBase(base);

    ::TStringBuilder builder;
    builder << base;
    for (const auto& key : relativeKeys) {
        ValidateKey(key);
        builder << "/" << key;
    }
    return builder;
}

void TYPathService::ValidateBase(const TString& base)
{
    if (base != "/" && !base.StartsWith("//")) {
        THROW_ERROR_EXCEPTION("Invalid base path")
            << TErrorAttribute("path", base);
    }
}

void TYPathService::ValidateRelative(const TString& relative)
{
    if (!relative.empty() && !relative.StartsWith("/")) {
        THROW_ERROR_EXCEPTION("Invalid relative path")
            << TErrorAttribute("path", relative);
    }
}

void TYPathService::ValidateKey(const TString& key)
{
    if (key.empty() || key.find('/') != TString::npos) {
        THROW_ERROR_EXCEPTION("Invalid path key")
            << TErrorAttribute("key", key);
    }
}

////////////////////////////////////////////////////////////////////////////////

const NInterop::IPathService* GetPathService()
{
    static TYPathService instance;
    return &instance;
}

}   // namespace NClickHouse
}   // namespace NYT
