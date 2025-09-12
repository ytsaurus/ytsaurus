#pragma once

#include "public.h"

#include <yt/yt/client/security_client/acl.h>

#include <yt/yt/ytlib/table_client/proto/row_level_security.pb.h>

#include <library/cpp/yt/logging/logger.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

//! A container for a prepared-to-be-compiled expression for RLS.
//! Note that trivial allow should be encoded as `std::optional<TRlsReadSpec>()`.
class TRlsReadSpec
{
public:
    TRlsReadSpec() = default;

    static std::optional<TRlsReadSpec> BuildFromRlAclAndTableSchema(
        const TTableSchemaPtr& tableSchema,
        const std::optional<std::vector<NSecurityClient::TRowLevelAccessControlEntry>>& rlAcl,
        const NLogging::TLogger& logger);

    bool IsTrivialDeny() const;

    //! Prerequisite: not trivial deny.
    const std::string& GetExpression() const;
    const TTableSchemaPtr& GetTableSchema() const;

    friend void ToProto(
        NProto::TRlsReadSpec* protoRlsReadSpec,
        const TRlsReadSpec& rlsReadSpec);

    friend void FromProto(
        TRlsReadSpec* rlsReadSpec,
        const NProto::TRlsReadSpec& protoRlsReadSpec);

private:
    struct TTrivialDeny
    { };

    TTableSchemaPtr TableSchema_;
    std::variant<TTrivialDeny, std::string> ExpressionOrTrivialDeny_ = TTrivialDeny{};
};

////////////////////////////////////////////////////////////////////////////////

struct IRlsCheckerFactory
    : public TRefCounted
{
    virtual IRlsCheckerPtr CreateCheckerForChunk(const TNameTablePtr& chunkNameTable) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IRlsCheckerFactory)

struct IRlsChecker
    : public TRefCounted
{
    virtual NSecurityClient::ESecurityAction Check(
        TUnversionedRow row,
        const TRowBufferPtr& rowBuffer) const = 0;

    virtual bool IsColumnNeeded(int indexInChunkNameTable) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IRlsChecker)

IRlsCheckerFactoryPtr CreateRlsCheckerFactory(
    const TRlsReadSpec& readSpec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
