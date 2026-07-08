#include "web_assembly_caller.h"

#include "web_assembly_data_transfer.h"

#include <yt/yt/library/numeric/util.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/library/query/engine_api/evaluation_helpers.h>
#include <yt/yt/library/query/engine_api/position_independent_value_transfer.h>

#include <yt/yt/library/web_assembly/api/compartment.h>
#include <yt/yt/library/web_assembly/api/pointer.h>

#include <yt/yt/library/web_assembly/engine/wavm_private_imports.h>

#include <yt/yt/core/misc/finally.h>

#include <library/cpp/yt/memory/range.h>

namespace NYT::NQueryClient {

using namespace NWebAssembly;

////////////////////////////////////////////////////////////////////////////////

// TODO(dtorilov): PrepareFunction on compartment cloning.

void InvokeWebAssemblyAndWrapExceptions(
    IWebAssemblyCompartment* compartment,
    WAVM::Runtime::Function* function,
    const WAVM::IR::FunctionType& runtimeType,
    WAVM::IR::UntaggedValue* arguments)
{
    try {
        WAVM::Runtime::invokeFunction(
            static_cast<WAVM::Runtime::Context*>(compartment->GetContext()),
            function,
            runtimeType,
            arguments,
            /*results*/ nullptr);
    } catch (WAVM::Runtime::Exception* ex) {
        auto description = describeException(ex);
        WAVM::Runtime::destroyException(ex);
        THROW_ERROR_EXCEPTION("WAVM Runtime Exception: %Qv", description);
    }
}

template <>
void TCGWebAssemblyCaller<TCGExpressionSignature, TCGPIExpressionSignature>::Run(
    TRange<TPIValue> literalValues,
    TRange<void*> opaqueData,
    TRange<size_t> opaqueDataSizes,
    TValue* result,
    TRange<TValue> row,
    const TRowBufferPtr& buffer,
    IWebAssemblyCompartment* compartment)
{
    auto compartmentGuard = Finally([previousCompartment = GetCurrentCompartment()] {
        SetCurrentCompartment(previousCompartment);
    });

    SetCurrentCompartment(compartment);

    auto context = TExpressionContext(buffer);
    auto contextGuard = Finally([&] {
        context.ClearWebAssemblyPool();
    });

    auto literalValuesGuard = CopyIntoCompartment(literalValues, compartment);

    auto opaqueDataGuard = CopyOpaqueDataIntoCompartment(opaqueData, opaqueDataSizes, compartment);

    auto* resultOffset = BitCast<TPIValue*>(context.AllocateAligned(sizeof(TPIValue), EAddressSpace::WebAssembly));
    MakePositionIndependentFromUnversioned(PtrFromVM(compartment, resultOffset), *result);
    CapturePIValue(compartment, &context, resultOffset);
    auto finallySaveResult = Finally([&] {
        MakeUnversionedFromPositionIndependent(result, *PtrFromVM(compartment, resultOffset));
        buffer->CaptureValue(result);
    });

    auto positionIndependentRow = BorrowFromNonPI(row);
    auto copiedRow = CopyIntoCompartment(
        TRange(positionIndependentRow.Begin(), positionIndependentRow.Size()),
        compartment);

    auto arguments = std::array<WAVM::IR::UntaggedValue, 5>{
        BitCast<WAVM::Uptr>(literalValuesGuard.GetCopiedOffset()),
        BitCast<WAVM::Uptr>(opaqueDataGuard.GetCopiedOffset()),
        BitCast<WAVM::Uptr>(resultOffset),
        BitCast<WAVM::Uptr>(copiedRow.GetCopiedOffset()),
        BitCast<WAVM::Uptr>(&context),
    };

    auto runtimeType = WAVM::IR::FunctionType(WAVM::IR::FunctionType::Encoding{BitCast<WAVM::Uptr>(RuntimeType_)});

    auto* function = static_cast<WAVM::Runtime::Function*>(compartment->GetFunction(FunctionName_));

    InvokeWebAssemblyAndWrapExceptions(compartment, function, runtimeType, arguments.data());
}

template <>
void TCGWebAssemblyCaller<TCGQuerySignature, TCGPIQuerySignature>::Run(
    TRange<TPIValue> literalValues,
    TRange<void*> opaqueData,
    TRange<size_t> opaqueDataSizes,
    TExecutionContext* context,
    IWebAssemblyCompartment* compartment)
{
    auto compartmentGuard = Finally([previousCompartment = GetCurrentCompartment()] {
        SetCurrentCompartment(previousCompartment);
    });

    SetCurrentCompartment(compartment);

    auto literalValuesGuard = CopyIntoCompartment(literalValues, compartment);
    auto opaqueDataGuard = CopyOpaqueDataIntoCompartment(opaqueData, opaqueDataSizes, compartment);

    auto arguments = std::array<WAVM::IR::UntaggedValue, 3>{
        BitCast<WAVM::Uptr>(literalValuesGuard.GetCopiedOffset()),
        BitCast<WAVM::Uptr>(opaqueDataGuard.GetCopiedOffset()),
        BitCast<WAVM::Uptr>(context),
    };

    auto runtimeType = WAVM::IR::FunctionType(WAVM::IR::FunctionType::Encoding{BitCast<WAVM::Uptr>(RuntimeType_)});

    auto* function = static_cast<WAVM::Runtime::Function*>(compartment->GetFunction(FunctionName_));

    compartment->StartDeadlineTimer();

    InvokeWebAssemblyAndWrapExceptions(compartment, function, runtimeType, arguments.data());
}

template <>
void TCGWebAssemblyCaller<TCGAggregateInitSignature, TCGPIAggregateInitSignature>::Run(
    const TRowBufferPtr& buffer,
    TValue* result,
    IWebAssemblyCompartment* compartment)
{
    auto compartmentGuard = Finally([previousCompartment = GetCurrentCompartment()] {
        SetCurrentCompartment(previousCompartment);
    });

    SetCurrentCompartment(compartment);

    auto context = TExpressionContext(buffer);
    auto contextGuard = Finally([&] {
        context.ClearWebAssemblyPool();
    });

    auto* resultOffset = BitCast<TPIValue*>(context.AllocateAligned(sizeof(TPIValue), EAddressSpace::WebAssembly));
    MakePositionIndependentFromUnversioned(PtrFromVM(compartment, resultOffset), *result);
    auto finallySaveResult = Finally([&] {
        MakeUnversionedFromPositionIndependent(result, *PtrFromVM(compartment, resultOffset));
        buffer->CaptureValue(result);
    });

    auto arguments = std::array<WAVM::IR::UntaggedValue, 2>{
        BitCast<WAVM::Uptr>(&context),
        BitCast<WAVM::Uptr>(resultOffset),
    };

    auto runtimeType = WAVM::IR::FunctionType(WAVM::IR::FunctionType::Encoding{BitCast<WAVM::Uptr>(RuntimeType_)});

    auto* function = static_cast<WAVM::Runtime::Function*>(compartment->GetFunction(FunctionName_));

    InvokeWebAssemblyAndWrapExceptions(compartment, function, runtimeType, arguments.data());
}

template <>
void TCGWebAssemblyCaller<TCGAggregateUpdateSignature, TCGPIAggregateUpdateSignature>::Run(
    const TRowBufferPtr& buffer,
    TValue* result,
    TRange<TValue> input,
    IWebAssemblyCompartment* compartment)
{
    auto compartmentGuard = Finally([previousCompartment = GetCurrentCompartment()] {
        SetCurrentCompartment(previousCompartment);
    });

    SetCurrentCompartment(compartment);

    auto context = TExpressionContext(buffer);
    auto contextGuard = Finally([&] {
        context.ClearWebAssemblyPool();
    });

    auto* resultOffset = BitCast<TPIValue*>(context.AllocateAligned(sizeof(TPIValue), EAddressSpace::WebAssembly));
    MakePositionIndependentFromUnversioned(PtrFromVM(compartment, resultOffset), *result);
    CapturePIValue(compartment, &context, resultOffset);
    auto finallySaveResult = Finally([&] {
        MakeUnversionedFromPositionIndependent(result, *PtrFromVM(compartment, resultOffset));
        buffer->CaptureValue(result);
    });

    auto positionIndependentRow = BorrowFromNonPI(input);
    auto copiedRow = CopyIntoCompartment(
        TRange(positionIndependentRow.Begin(), positionIndependentRow.Size()),
        compartment);

    auto arguments = std::array<WAVM::IR::UntaggedValue, 3>{
        BitCast<WAVM::Uptr>(&context),
        BitCast<WAVM::Uptr>(resultOffset),
        BitCast<WAVM::Uptr>(copiedRow.GetCopiedOffset()),
    };

    auto runtimeType = WAVM::IR::FunctionType(WAVM::IR::FunctionType::Encoding{BitCast<WAVM::Uptr>(RuntimeType_)});

    auto* function = static_cast<WAVM::Runtime::Function*>(compartment->GetFunction(FunctionName_));

    InvokeWebAssemblyAndWrapExceptions(compartment, function, runtimeType, arguments.data());
}

template <>
void TCGWebAssemblyCaller<TCGAggregateMergeSignature, TCGPIAggregateMergeSignature>::Run(
    const TRowBufferPtr& buffer,
    TValue* result,
    const TValue* state,
    IWebAssemblyCompartment* compartment)
{
    auto compartmentGuard = Finally([previousCompartment = GetCurrentCompartment()] {
        SetCurrentCompartment(previousCompartment);
    });

    SetCurrentCompartment(compartment);

    auto context = TExpressionContext(buffer);
    auto contextGuard = Finally([&] {
        context.ClearWebAssemblyPool();
    });

    auto* resultOffset = BitCast<TPIValue*>(context.AllocateAligned(sizeof(TPIValue), EAddressSpace::WebAssembly));
    MakePositionIndependentFromUnversioned(PtrFromVM(compartment, resultOffset), *result);
    CapturePIValue(compartment, &context, resultOffset);
    auto finallySaveResult = Finally([&] {
        MakeUnversionedFromPositionIndependent(result, *PtrFromVM(compartment, resultOffset));
        buffer->CaptureValue(result);
    });

    auto* stateOffset = BitCast<TPIValue*>(context.AllocateAligned(sizeof(TPIValue), EAddressSpace::WebAssembly));
    MakePositionIndependentFromUnversioned(PtrFromVM(compartment, stateOffset), *state);
    if (IsStringLikeType(state->Type)) {
        auto* offset = BitCast<char*>(context.AllocateAligned(state->Length, EAddressSpace::WebAssembly));
        ::memcpy(PtrFromVM(compartment, offset), state->AsStringBuf().data(), state->Length);
        PtrFromVM(compartment, stateOffset)->SetStringPosition(PtrFromVM(compartment, offset));
    }

    auto arguments = std::array<WAVM::IR::UntaggedValue, 3>{
        BitCast<WAVM::Uptr>(&context),
        BitCast<WAVM::Uptr>(resultOffset),
        BitCast<WAVM::Uptr>(stateOffset),
    };

    auto runtimeType = WAVM::IR::FunctionType(WAVM::IR::FunctionType::Encoding{BitCast<WAVM::Uptr>(RuntimeType_)});

    auto* function = static_cast<WAVM::Runtime::Function*>(compartment->GetFunction(FunctionName_));

    InvokeWebAssemblyAndWrapExceptions(compartment, function, runtimeType, arguments.data());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
