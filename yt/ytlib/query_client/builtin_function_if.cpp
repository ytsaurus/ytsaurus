#include "builtin_functions.h"
#include "cg_fragment_compiler.h"
#include "helpers.h"

namespace NYT {
namespace NQueryClient {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TIfFunction
    : public TTypedFunction
    , public TUniversalRangeFunction
{
public:
    TIfFunction()
        : TTypedFunction(
            "if",
            std::unordered_map<TTypeArgument, TUnionType>(),
            std::vector<TType>{EValueType::Boolean, 0, 0},
            0)
    { }

    virtual TCodegenExpression MakeCodegenExpr(
        TCodegenValue codegenFunctionContext,
        std::vector<TCodegenExpression> codegenArgs,
        std::vector<EValueType> argumentTypes,
        EValueType type,
        const Stroka& name) const override;
};

TCodegenExpression TIfFunction::MakeCodegenExpr(
    TCodegenValue codegenFunctionContext,
    std::vector<TCodegenExpression> codegenArgs,
    std::vector<EValueType> argumentTypes,
    EValueType type,
    const Stroka& name) const
{
    return [this, codegenArgs = std::move(codegenArgs), type, name] (TCGContext& builder, Value* row) {
        auto nameTwine = Twine(name.c_str());

        YCHECK(codegenArgs.size() == 3);
        auto condition = codegenArgs[0](builder, row);
        YCHECK(condition.GetStaticType() == EValueType::Boolean);

        return CodegenIf<TCGContext, TCGValue>(
            builder,
            condition.IsNull(),
            [&] (TCGContext& builder) {
                return TCGValue::CreateNull(builder, type);
            },
            [&] (TCGContext& builder) {
                return CodegenIf<TCGContext, TCGValue>(
                    builder,
                    builder.CreateICmpNE(
                        builder.CreateZExtOrBitCast(condition.GetData(), builder.getInt64Ty()),
                        builder.getInt64(0)),
                    [&] (TCGContext& builder) {
                        return codegenArgs[1](builder, row);
                    },
                    [&] (TCGContext& builder) {
                        return codegenArgs[2](builder, row);
                    });
            },
            nameTwine);
    };
}

////////////////////////////////////////////////////////////////////////////////

IFunctionDescriptorPtr CreateIfFunction()
{
    return New<TIfFunction>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
