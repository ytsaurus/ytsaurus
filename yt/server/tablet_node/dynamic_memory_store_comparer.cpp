#include "dynamic_memory_store_bits.h"
#include "dynamic_memory_store_comparer.h"
#include "row_comparer_generator.h"
#include "private.h"

namespace NYT {
namespace NTabletNode {

using namespace NCodegen;

////////////////////////////////////////////////////////////////////////////////

class TDynamicRowKeyComparer::TImpl
    : public TRefCounted
{
public:
#ifdef YT_USE_LLVM
    TImpl(
        int keyColumnCount,
        const TTableSchema& schema,
        TCGFunction<TDDComparerSignature> ddComparer,
        TCGFunction<TDUComparerSignature> duComparer)
        : KeyColumnCount_(keyColumnCount)
        , Schema_(schema)
        , DDComparer_(std::move(ddComparer))
        , DUComparer_(std::move(duComparer))
    { }
#endif

    TImpl(int keyColumnCount, const TTableSchema& schema)
        : KeyColumnCount_(keyColumnCount)
        , Schema_(schema)
    { }

    static TIntrusivePtr<TImpl> CreateWithoutLlvm(int keyColumnCount, const TTableSchema& schema)
    {
        return New<TImpl>(keyColumnCount, schema);
    }

    static TIntrusivePtr<TImpl> CreateWithLlvm(int keyColumnCount, const TTableSchema& schema)
    {
#ifdef YT_USE_LLVM
        TCGFunction<TDDComparerSignature> ddComparer;
        TCGFunction<TDUComparerSignature> duComparer;
        std::tie(ddComparer, duComparer) = GenerateComparers(keyColumnCount, schema);
        return New<TImpl>(
            keyColumnCount,
            schema,
            std::move(ddComparer),
            std::move(duComparer));
#else
        return CreateWithoutLlvm(keyColumnCount, schema);
#endif
    }

    int operator()(TDynamicRow lhs, TDynamicRow rhs) const
    {
        if (DDComparer_) {
            return DDComparer_(
                lhs.GetNullKeyMask(),
                lhs.BeginKeys(),
                rhs.GetNullKeyMask(),
                rhs.BeginKeys());
        } else {
            return Compare(lhs, rhs);
        }
    }

    int operator()(TDynamicRow lhs, TRowWrapper rhs) const
    {
        YASSERT(rhs.Row.GetCount() >= KeyColumnCount_);
        if (DUComparer_) {
            return DUComparer_(
                lhs.GetNullKeyMask(),
                lhs.BeginKeys(),
                rhs.Row.Begin(),
                KeyColumnCount_);
        } else {
            return Compare(lhs, rhs.Row.Begin(), KeyColumnCount_);
        }
    }

    int operator()(TDynamicRow lhs, TKeyWrapper rhs) const
    {
        if (DUComparer_) {
            return DUComparer_(
                lhs.GetNullKeyMask(),
                lhs.BeginKeys(),
                rhs.Row.Begin(),
                rhs.Row.GetCount());
        } else {
            return Compare(lhs, rhs.Row.Begin(), rhs.Row.GetCount());
        }
    }

private:
    int Compare(TDynamicRow lhs, TDynamicRow rhs) const
    {
        ui32 nullKeyBit = 1;
        ui32 lhsNullKeyMask = lhs.GetNullKeyMask();
        ui32 rhsNullKeyMask = rhs.GetNullKeyMask();
        const auto* lhsValue = lhs.BeginKeys();
        const auto* rhsValue = rhs.BeginKeys();
        auto columnIt = Schema_.Columns().begin();
        for (int index = 0;
             index < KeyColumnCount_;
             ++index, nullKeyBit <<= 1, ++lhsValue, ++rhsValue, ++columnIt)
        {
            bool lhsNull = (lhsNullKeyMask & nullKeyBit);
            bool rhsNull = (rhsNullKeyMask & nullKeyBit);
            if (lhsNull && !rhsNull) {
                return -1;
            } else if (!lhsNull && rhsNull) {
                return +1;
            } else if (lhsNull && rhsNull) {
                continue;
            }

            switch (columnIt->Type) {
                case EValueType::Int64: {
                    i64 lhsData = lhsValue->Int64;
                    i64 rhsData = rhsValue->Int64;
                    if (lhsData < rhsData) {
                        return -1;
                    } else if (lhsData > rhsData) {
                        return +1;
                    }
                    break;
                }

                case EValueType::Uint64: {
                    ui64 lhsData = lhsValue->Uint64;
                    ui64 rhsData = rhsValue->Uint64;
                    if (lhsData < rhsData) {
                        return -1;
                    } else if (lhsData > rhsData) {
                        return +1;
                    }
                    break;
                }

                case EValueType::Double: {
                    double lhsData = lhsValue->Double;
                    double rhsData = rhsValue->Double;
                    if (lhsData < rhsData) {
                        return -1;
                    } else if (lhsData > rhsData) {
                        return +1;
                    }
                    break;
                }

                case EValueType::Boolean: {
                    bool lhsData = lhsValue->Boolean;
                    bool rhsData = rhsValue->Boolean;
                    if (lhsData < rhsData) {
                        return -1;
                    } else if (lhsData > rhsData) {
                        return +1;
                    }
                    break;
                }

                case EValueType::String: {
                    size_t lhsLength = lhsValue->String->Length;
                    size_t rhsLength = rhsValue->String->Length;
                    size_t minLength = std::min(lhsLength, rhsLength);
                    int result = ::memcmp(lhsValue->String->Data, rhsValue->String->Data, minLength);
                    if (result != 0) {
                        return result;
                    } else if (lhsLength < rhsLength) {
                        return -1;
                    } else if (lhsLength > rhsLength) {
                        return +1;
                    }
                    break;
                }

                default:
                    YUNREACHABLE();
            }
        }
        return 0;
    }

    int Compare(TDynamicRow lhs, TUnversionedValue* rhsBegin, int rhsLength) const
    {
        ui32 nullKeyBit = 1;
        ui32 lhsNullKeyMask = lhs.GetNullKeyMask();
        const auto* lhsValue = lhs.BeginKeys();
        const auto* rhsValue = rhsBegin;

        auto columnIt = Schema_.Columns().begin();
        int lhsLength = KeyColumnCount_;
        int minLength = std::min(lhsLength, rhsLength);
        for (int index = 0;
             index < minLength;
             ++index, nullKeyBit <<= 1, ++lhsValue, ++rhsValue, ++columnIt)
        {
            auto lhsType = (lhsNullKeyMask & nullKeyBit) ? EValueType(EValueType::Null) : columnIt->Type;
            if (lhsType < rhsValue->Type) {
                return -1;
            } else if (lhsType > rhsValue->Type) {
                return +1;
            }

            switch (lhsType) {
                case EValueType::Int64: {
                    i64 lhsData = lhsValue->Int64;
                    i64 rhsData = rhsValue->Data.Int64;
                    if (lhsData < rhsData) {
                        return -1;
                    } else if (lhsData > rhsData) {
                        return +1;
                    }
                    break;
                }

                case EValueType::Uint64: {
                    ui64 lhsData = lhsValue->Uint64;
                    ui64 rhsData = rhsValue->Data.Uint64;
                    if (lhsData < rhsData) {
                        return -1;
                    } else if (lhsData > rhsData) {
                        return +1;
                    }
                    break;
                }

                case EValueType::Double: {
                    double lhsData = lhsValue->Double;
                    double rhsData = rhsValue->Data.Double;
                    if (lhsData < rhsData) {
                        return -1;
                    } else if (lhsData > rhsData) {
                        return +1;
                    }
                    break;
                }

                case EValueType::Boolean: {
                    bool lhsData = lhsValue->Boolean;
                    bool rhsData = rhsValue->Data.Boolean;
                    if (lhsData < rhsData) {
                        return -1;
                    } else if (lhsData > rhsData) {
                        return +1;
                    }
                    break;
                }

                case EValueType::String: {
                    size_t lhsLength = lhsValue->String->Length;
                    size_t rhsLength = rhsValue->Length;
                    size_t minLength = std::min(lhsLength, rhsLength);
                    int result = ::memcmp(lhsValue->String->Data, rhsValue->Data.String, minLength);
                    if (result != 0) {
                        return result;
                    } else if (lhsLength < rhsLength) {
                        return -1;
                    } else if (lhsLength > rhsLength) {
                        return +1;
                    }
                    break;
                }

                case EValueType::Null:
                    break;

                default:
                    YUNREACHABLE();
            }
        }
        return lhsLength - rhsLength;
    }

    const int KeyColumnCount_;
    const TTableSchema Schema_;
#ifdef YT_USE_LLVM
    TCGFunction<TDDComparerSignature> DDComparer_;
    TCGFunction<TDUComparerSignature> DUComparer_;
#endif

};

////////////////////////////////////////////////////////////////////////////////

TDynamicRowKeyComparer::TDynamicRowKeyComparer(
    int keyColumnCount,
    const TTableSchema& schema)
    : Impl_(TImpl::CreateWithLlvm(keyColumnCount, schema))
{ }

TDynamicRowKeyComparer::TDynamicRowKeyComparer(
    int keyColumnCount,
    const TTableSchema& schema,
    TNoCodegenDynamicRowKeyCompare)
    : Impl_(TImpl::CreateWithoutLlvm(keyColumnCount, schema))
{ }

TDynamicRowKeyComparer::TDynamicRowKeyComparer(const TDynamicRowKeyComparer& other) = default;
TDynamicRowKeyComparer::TDynamicRowKeyComparer(TDynamicRowKeyComparer&& other) = default;
TDynamicRowKeyComparer::TDynamicRowKeyComparer() = default;

TDynamicRowKeyComparer& TDynamicRowKeyComparer::operator=(const TDynamicRowKeyComparer& other) = default;
TDynamicRowKeyComparer& TDynamicRowKeyComparer::operator=(TDynamicRowKeyComparer&& other) = default;

TDynamicRowKeyComparer::~TDynamicRowKeyComparer() = default;

int TDynamicRowKeyComparer::operator()(TDynamicRow lhs, TDynamicRow rhs) const
{
    return Impl_->operator()(lhs, rhs);
}

int TDynamicRowKeyComparer::operator()(TDynamicRow lhs, TRowWrapper rhs) const
{
    return Impl_->operator()(lhs, rhs);
}

int TDynamicRowKeyComparer::operator()(TDynamicRow lhs, TKeyWrapper rhs) const
{
    return Impl_->operator()(lhs, rhs);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
