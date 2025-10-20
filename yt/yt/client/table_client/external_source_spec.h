#pragma once

#include "public.h"

#include <yt/yt/core/ytree/polymorphic_yson_struct.h>

#include <yt/yt/library/re2/public.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

//! Base type for various external source specs.
//! Must never be used directly.
struct TExternalSourceSpecBase
    : public NYTree::TYsonStruct
{
    static void ThrowBaseTypeUsageError();

    REGISTER_YSON_STRUCT(TExternalSourceSpecBase);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

//! Represents a static list of files from an offshore medium.
struct TFilesExternalSourceSpec
    : public TExternalSourceSpecBase
{
    //! List of URIs, e.g. "s3://bucket/path/to/file".
    std::vector<std::string> Uris;

    REGISTER_YSON_STRUCT(TFilesExternalSourceSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFilesExternalSourceSpec);

//! NB: Source spec must be non-null.
void ToProto(
    NProto::TFilesExternalSourceSpec* protoSourceSpec,
    const TFilesExternalSourceSpecPtr& sourceSpec);

//! NB: Source spec must be non-null.
void FromProto(
    const TFilesExternalSourceSpecPtr& sourceSpec,
    const NProto::TFilesExternalSourceSpec& protoSourceSpec);

void FormatValue(
    TStringBuilderBase* builder,
    const TFilesExternalSourceSpecPtr& sourceSpec,
    TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

//! Represents a list of files with a given prefix from an offshore medium.
//! It is pretty S3 specific at the moment, as it is the only possilbe offshore
//! medium, but this may be generalized in the future.
struct TPrefixExternalSourceSpec
    : public TExternalSourceSpecBase
{
    //! URI prefix, e.g. "s3://bucket/prefix".
    std::string PrefixUri;

    //! Whether to include files in subdirectories of the given prefix.
    //! True by default.
    bool Recursive;

    //! Filter patterns to *include* file paths in external source.
    //! Paths matching any of these patterns will be included.
    //! If empty, all files are included.
    //! Patterns apply to the relative path (key minus the prefix), e.g., `path/to/file` for key `prefix/path/to/file` under prefix `s3://bucket/prefix`.
    //! NB: Inclusion is applied before exclusion, i.e. if a path matches both an include and an exclude pattern, it will be excluded.
    std::vector<NRe2::TRe2Ptr> IncludeRegexes;

    //! Filter patterns to *exclude* file paths from external source.
    //! Paths matching any of these patterns will be excluded.
    //! If empty, no files are excluded.
    //! Patterns apply to the relative path (key minus the prefix), e.g., `path/to/file` for key `prefix/path/to/file` under prefix `s3://bucket/prefix`.
    //! NB: Exclusion is applied after inclusion, i.e. if a path matches both an include and an exclude pattern, it will be excluded.
    std::vector<NRe2::TRe2Ptr> ExcludeRegexes;

    REGISTER_YSON_STRUCT(TPrefixExternalSourceSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPrefixExternalSourceSpec);

//! NB: Source spec must be non-null.
void ToProto(
    NProto::TPrefixExternalSourceSpec* protoSourceSpec,
    const TPrefixExternalSourceSpecPtr& sourceSpec);

//! NB: Source spec must be non-null.
void FromProto(
    const TPrefixExternalSourceSpecPtr& sourceSpec,
    const NProto::TPrefixExternalSourceSpec& protoSourceSpec);

void FormatValue(
    TStringBuilderBase* builder,
    const TPrefixExternalSourceSpecPtr& sourceSpec,
    TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

//! Polymorphic wrapper around various external source specs.
//! Must never be null and must never be of base type.
DEFINE_POLYMORPHIC_YSON_STRUCT(ExternalSourceSpec,
    ((Base)   (TExternalSourceSpecBase))
    ((Files)  (TFilesExternalSourceSpec))
    ((Prefix) (TPrefixExternalSourceSpec))
);

//! NB: Source spec must be non-null and not of base type.
void ToProto(
    NProto::TExternalSourceSpec* protoSourceSpec,
    const TExternalSourceSpec& sourceSpec);

//! NB: Allocates a new source spec.
void FromProto(
    TExternalSourceSpec* sourceSpec,
    const NProto::TExternalSourceSpec& protoSourceSpec);

//! NB: Source spec must be non-null and not of base type.
//! Formatting includes the type field.
void FormatValue(
    TStringBuilderBase* builder,
    const TExternalSourceSpec& sourceSpec,
    TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
