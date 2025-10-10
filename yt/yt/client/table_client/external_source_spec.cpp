#include "external_source_spec.h"

#include <yt_proto/yt/client/table_client/proto/external_source_spec.pb.h>

#include <yt/yt/library/re2/re2.h>

namespace NYT::NTableClient {

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

void TExternalSourceSpecBase::Register(TRegistrar /*registrar*/)
{ };

void TExternalSourceSpecBase::ThrowBaseTypeUsageError()
{
    THROW_ERROR_EXCEPTION("Base external source spec cannot be interacted with, please use a derived type");
}

////////////////////////////////////////////////////////////////////////////////

void TFilesExternalSourceSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("uris", &TFilesExternalSourceSpec::Uris)
        .Default();
}

void ToProto(
    NProto::TFilesExternalSourceSpec* protoSourceSpec,
    const TFilesExternalSourceSpecPtr& sourceSpec)
{
    YT_VERIFY(sourceSpec);

    ToProto(protoSourceSpec->mutable_uris(), sourceSpec->Uris);
}

void FromProto(
    const TFilesExternalSourceSpecPtr& sourceSpec,
    const NProto::TFilesExternalSourceSpec& protoSourceSpec)
{
    YT_VERIFY(sourceSpec);

    FromProto(&sourceSpec->Uris, protoSourceSpec.uris());
}

void FormatValue(
    TStringBuilderBase* builder,
    const TFilesExternalSourceSpecPtr& sourceSpec,
    TStringBuf spec)
{
    static constexpr int MaxUrisToFormat = 10;

    FormatValue(
        builder,
        Format(
            "{Type: %lv, Uris: %v}",
            EExternalSourceSpecType::Files,
            MakeShrunkFormattableView(sourceSpec->Uris, TDefaultFormatter(), MaxUrisToFormat)),
        spec);
}

////////////////////////////////////////////////////////////////////////////////

void TPrefixExternalSourceSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("prefix_uri", &TPrefixExternalSourceSpec::PrefixUri)
        .Default();
    registrar.Parameter("recursive", &TPrefixExternalSourceSpec::Recursive)
        .Default(true);
    registrar.Parameter("include_regexes", &TPrefixExternalSourceSpec::IncludeRegexes)
        .Default();
    registrar.Parameter("exclude_regexes", &TPrefixExternalSourceSpec::ExcludeRegexes)
        .Default();

    registrar.Postprocessor([] (TPrefixExternalSourceSpec* spec) {
        auto validateRegexes = [] (const std::vector<NRe2::TRe2Ptr>& regexes, const char* name) {
            for (const auto& regex : regexes) {
                if (!regex) {
                    THROW_ERROR_EXCEPTION("Null %v regex is not allowed", name);
                }

                if (regex && !regex->ok()) {
                    THROW_ERROR_EXCEPTION("Error parsing %v regex", name)
                        << TErrorAttribute("error", regex->error());
                }
            }
        };

        validateRegexes(spec->IncludeRegexes, "include");
        validateRegexes(spec->ExcludeRegexes, "exclude");
    });
}

void ToProto(
    NProto::TPrefixExternalSourceSpec* protoSourceSpec,
    const TPrefixExternalSourceSpecPtr& sourceSpec)
{
    YT_VERIFY(sourceSpec);

    protoSourceSpec->set_prefix_uri(TString(sourceSpec->PrefixUri));
    protoSourceSpec->set_recursive(sourceSpec->Recursive);
    ToProto(protoSourceSpec->mutable_include_regexes(), sourceSpec->IncludeRegexes);
    ToProto(protoSourceSpec->mutable_exclude_regexes(), sourceSpec->ExcludeRegexes);
}

void FromProto(
    const TPrefixExternalSourceSpecPtr& sourceSpec,
    const NProto::TPrefixExternalSourceSpec& protoSourceSpec)
{
    YT_VERIFY(sourceSpec);

    sourceSpec->PrefixUri = protoSourceSpec.prefix_uri();
    sourceSpec->Recursive = protoSourceSpec.recursive();
    sourceSpec->IncludeRegexes = FromProto<std::vector<NRe2::TRe2Ptr>>(protoSourceSpec.include_regexes());
    sourceSpec->ExcludeRegexes = FromProto<std::vector<NRe2::TRe2Ptr>>(protoSourceSpec.exclude_regexes());
}

void FormatValue(
    TStringBuilderBase* builder,
    const TPrefixExternalSourceSpecPtr& sourceSpec,
    TStringBuf spec)
{
    static constexpr int MaxRegexesToFormat = 10;

    FormatValue(
        builder,
        Format(
            "{Type: %lv, PrefixUri: %v, Recursive: %v, IncludeRegexes: %v, ExcludeRegexes: %v}",
            EExternalSourceSpecType::Prefix,
            sourceSpec->PrefixUri,
            sourceSpec->Recursive,
            MakeShrunkFormattableView(sourceSpec->IncludeRegexes, TDefaultFormatter(), MaxRegexesToFormat),
            MakeShrunkFormattableView(sourceSpec->ExcludeRegexes, TDefaultFormatter(), MaxRegexesToFormat)),
        spec);
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NProto::TExternalSourceSpec* protoSourceSpec,
    const NTableClient::TExternalSourceSpec& sourceSpec)
{
    switch (sourceSpec.GetCurrentType()) {
        case NTableClient::EExternalSourceSpecType::Base: {
            TExternalSourceSpecBase::ThrowBaseTypeUsageError();
            break;
        }

        case NTableClient::EExternalSourceSpecType::Files: {
            ToProto(
                protoSourceSpec->mutable_files(),
                sourceSpec.TryGetConcrete<TFilesExternalSourceSpec>());
            break;
        }
        case NTableClient::EExternalSourceSpecType::Prefix: {
            ToProto(
                protoSourceSpec->mutable_prefix(),
                sourceSpec.TryGetConcrete<TPrefixExternalSourceSpec>());
            break;
        }

        default:
            YT_ABORT();
    }
}

void FromProto(
    NTableClient::TExternalSourceSpec* sourceSpec,
    const NProto::TExternalSourceSpec& protoSourceSpec)
{
    switch (protoSourceSpec.value_case()) {
        case NProto::TExternalSourceSpec::VALUE_NOT_SET:
            TExternalSourceSpecBase::ThrowBaseTypeUsageError();
            break;

        case NProto::TExternalSourceSpec::kFiles: {
            *sourceSpec = TExternalSourceSpec(EExternalSourceSpecType::Files);
            FromProto(
                sourceSpec->TryGetConcrete<TFilesExternalSourceSpec>(),
                protoSourceSpec.files());
            break;
        }
        case NProto::TExternalSourceSpec::kPrefix: {
            *sourceSpec = TExternalSourceSpec(EExternalSourceSpecType::Prefix);
            FromProto(
                sourceSpec->TryGetConcrete<TPrefixExternalSourceSpec>(),
                protoSourceSpec.prefix());
            break;
        }

        default:
            YT_ABORT();
    }
}

void FormatValue(
    TStringBuilderBase* builder,
    const TExternalSourceSpec& sourceSpec,
    TStringBuf spec)
{
    switch (sourceSpec.GetCurrentType()) {
        case NTableClient::EExternalSourceSpecType::Base: {
            TExternalSourceSpecBase::ThrowBaseTypeUsageError();
            break;
        }

        case NTableClient::EExternalSourceSpecType::Files: {
            FormatValue(
                builder,
                sourceSpec.TryGetConcrete<TFilesExternalSourceSpec>(),
                spec);
            break;
        }
        case NTableClient::EExternalSourceSpecType::Prefix: {
            FormatValue(
                builder,
                sourceSpec.TryGetConcrete<TPrefixExternalSourceSpec>(),
                spec);
            break;
        }

        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
