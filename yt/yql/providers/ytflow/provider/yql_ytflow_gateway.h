#pragma once

#include "yql_ytflow_configuration.h"

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/yql_user_data.h>
#include <yql/essentials/providers/common/gateway/yql_provider_gateway.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/maybe.h>
#include <util/generic/ptr.h>


namespace NYql {

class IYtflowGateway: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IYtflowGateway>;

#define OPTION_FIELD_METHODS(type, name)                        \
    public:                                                     \
    TSelf& name(TTypeTraits<type>::TFuncParam arg##name)& {     \
        name##_ = arg##name;                                    \
        return *this;                                           \
    }                                                           \
    TSelf&& name(TTypeTraits<type>::TFuncParam arg##name)&& {   \
        name##_ = arg##name;                                    \
        return std::move(*this);                                \
    }                                                           \
    TTypeTraits<type>::TFuncParam name() const {                \
        return name##_;                                         \
    }                                                           \
    type& name() {                                              \
        return name##_;                                         \
    }

#define OPTION_FIELD(type, name)                                \
    private:                                                    \
    type name##_;                                               \
    OPTION_FIELD_METHODS(type, name)

#define OPTION_FIELD_DEFAULT(type, name, def)                   \
    private:                                                    \
    type name##_ = def;                                         \
    OPTION_FIELD_METHODS(type, name)

    struct TOpenSessionOptions {
        using TSelf = TOpenSessionOptions;

        OPTION_FIELD(TString, SessionId);
        OPTION_FIELD(TOperationProgressWriter, OperationProgressWriter);
    };

    struct TCloseSessionOptions {
        using TSelf = TCloseSessionOptions;

        OPTION_FIELD(TString, SessionId);
    };

    struct TRunOptions {
        using TSelf = TRunOptions;

        using TGetYtflowIntegration = std::function<IYtflowIntegration*(const TExprNode&)>;

        OPTION_FIELD(TString, SessionId);
        OPTION_FIELD(TMaybe<ui32>, PublicId);
        OPTION_FIELD(TYtflowSettings::TConstPtr, Config);
        OPTION_FIELD(TGetYtflowIntegration, GetYtflowIntegration);
        OPTION_FIELD(TUserDataTable, UserDataBlocks);
    };

    virtual void OpenSession(const TOpenSessionOptions& options) = 0;
    virtual NThreading::TFuture<void> CloseSession(const TCloseSessionOptions& options) = 0;

    virtual NThreading::TFuture<NCommon::TOperationResult> Run(
        const TExprNode::TPtr& node, const TRunOptions& options, TExprContext& ctx) = 0;
};

} // namespace NYql
