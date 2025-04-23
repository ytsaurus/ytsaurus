#include "public.h"

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

using TTypeId = int;

class TTypingCtx
{
public:
    struct TFunctionSignature
    {
        // Result type, optional state type and argument types.
        std::vector<TTypeId> Types;
        std::vector<std::vector<TTypeId>> Constraints;
    };

    struct TFunctionSignatures
        : public std::vector<TFunctionSignature>
    { };

    TTypingCtx();

    TTypeId GetTypeId(TString name, const TLogicalTypePtr& logicalType);

    TTypeId GetTypeId(EValueType wireType);

    TTypeId GetTypeId(ESimpleLogicalValueType type);

    TTypeId GetTypeId(const TLogicalTypePtr& logicalType);

    EValueType GetWireType(TTypeId typeId);

    TLogicalTypePtr GetLogicalType(TTypeId typeId);

    bool HasImplicitCast(TTypeId sourceType, TTypeId targetType);

    void RegisterFunction(TString name, TFunctionSignatures signatures);

    void FillFunctionSignatures();

    std::vector<TFunctionSignature> GetFunctionSignatures(TStringBuf name, int /*argCount*/);

    // Additional types are 0 in case no result type.
    // 1 for result type.
    // 2 for result and state types.
    std::vector<TTypeId> InferFunctionType(
        TStringBuf name,
        std::vector<TFunctionSignature> signatures,
        TRange<TTypeId> argumentTypes,
        int additionalTypes = 1);

    // Returns result types and argument types coercions.
    std::vector<TTypeId> InferFunctionType(TStringBuf name, TRange<TTypeId> argumentTypes);
private:
    std::vector<std::pair<TString, TLogicalTypePtr>> TypeNames_;
    THashMap<TString, TTypeId> UniqueTypes_;

    THashMap<TString, TFunctionSignatures> Functions_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
