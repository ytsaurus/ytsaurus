#pragma once

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/phoenix/context.h>
#include <yt/yt/core/phoenix/type_decl.h>

#include <yt/yt/client/security_client/acl.h>

#include <yt/yt/library/query/row_level_security_api/proto/row_level_security.pb.h>

#include <library/cpp/yt/logging/logger.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

//! A container for a prepared-to-be-compiled predicate for RLS.
//! Note that trivial allow should be encoded as `std::optional<TRlsReadSpec>()`.
class TRlsReadSpec
{
public:
    TRlsReadSpec() = default;

    static std::optional<TRlsReadSpec> BuildFromRowLevelAclAndTableSchema(
        const TTableSchemaPtr& tableSchema,
        const std::optional<std::vector<NSecurityClient::TRowLevelAccessControlEntry>>& rowLevelAcl,
        const NLogging::TLogger& logger);

    bool IsTrivialDeny() const;

    //! Prerequisite: not trivial deny.
    const std::string& GetPredicate() const;
    const TTableSchemaPtr& GetTableSchema() const;

    friend void ToProto(
        NProto::TRlsReadSpec* protoRlsReadSpec,
        const TRlsReadSpec& rlsReadSpec);

    friend void FromProto(
        TRlsReadSpec* rlsReadSpec,
        const NProto::TRlsReadSpec& protoRlsReadSpec);

private:
    struct TTrivialDeny
    {
        void Persist(const auto& /*context*/)
        { }
    };

    TTableSchemaPtr TableSchema_;
    std::variant<TTrivialDeny, std::string> PredicateOrTrivialDeny_ = TTrivialDeny{};

    PHOENIX_DECLARE_TYPE(TRlsReadSpec, 0x01215125);
};

void FormatValue(TStringBuilderBase* builder, const TRlsReadSpec& rlsReadSpec, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

struct IRlsChecker
    : public TRefCounted
{
    //! |chunkValuesPrefix| is used to cut system columns as they have
    //! reader-name-table ids and will confuse the checker.
    //! TODO(coteeq): Encode name-table-affinity of values in the input row itself.
    //! FIXME(coteeq): KeyWideningOptions break RLS.
    virtual NSecurityClient::ESecurityAction Check(
        TUnversionedRow row,
        const TRowBufferPtr& rowBuffer,
        int chunkValuesPrefix) const = 0;

    virtual bool IsColumnNeeded(int indexInChunkNameTable) const = 0;
};

DECLARE_REFCOUNTED_STRUCT(IRlsChecker)

struct IRlsCheckerFactory
    : public TRefCounted
{
    virtual IRlsCheckerPtr CreateCheckerForChunk(const TNameTablePtr& chunkNameTable) const = 0;
};

DECLARE_REFCOUNTED_STRUCT(IRlsCheckerFactory)

IRlsCheckerFactoryPtr CreateRlsCheckerFactory(
    const TRlsReadSpec& readSpec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
