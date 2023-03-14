#include <yt/yt/client/complex_types/check_type_compatibility.h>
#include <yt/yt/client/table_client/logical_type.h>

#include <library/cpp/yt/misc/enum.h>
#include <yt/yt/core/ytree/convert.h>

using namespace NYT;
using namespace NYT::NTableClient;
using namespace NYT::NYTree;
using namespace NYT::NYson;
using namespace NYT::NComplexTypes;

class TFuzzLogicalTypeGenerator
{
public:
    static TLogicalTypePtr Generate(IInputStream* entropySource, int complexityLimit = 16)
    {
        YT_VERIFY(complexityLimit >= 0);
        int complexityLimitCopy = complexityLimit;
        auto generator = TFuzzLogicalTypeGenerator(entropySource);
        return generator.GenerateType(&complexityLimitCopy);
    }

private:
    explicit TFuzzLogicalTypeGenerator(IInputStream* entropySource)
        : EntropySource_(entropySource)
    { }

    TLogicalTypePtr GenerateType(int* complexityLimit)
    {
        --*complexityLimit;
        if (*complexityLimit <= 0) {
            return SimpleLogicalType(ESimpleLogicalValueType::Int64);
        } else {
            auto metatype = GenerateEnum<ELogicalMetatype>();
            switch (metatype) {
                case ELogicalMetatype::Simple:
                    return SimpleLogicalType(GenerateEnum<ESimpleLogicalValueType>());
                case ELogicalMetatype::Optional:
                    return OptionalLogicalType(GenerateType(complexityLimit));
                case ELogicalMetatype::List:
                    return ListLogicalType(GenerateType(complexityLimit));
                case ELogicalMetatype::Tuple:
                case ELogicalMetatype::VariantTuple: {
                    std::vector<TLogicalTypePtr> elements;
                    int count = GenerateByte() % 16;
                    for (int i = 0; i < count; ++i) {
                        elements.push_back(GenerateType(complexityLimit));
                    }
                    if (metatype == ELogicalMetatype::Tuple) {
                        return TupleLogicalType(elements);
                    } else {
                        return VariantTupleLogicalType(elements);
                    }
                }

                case ELogicalMetatype::Struct:
                case ELogicalMetatype::VariantStruct: {
                    std::vector<TStructField> fields;
                    int count = GenerateByte() % 16;
                    for (int i = 0; i < count; ++i) {
                        fields.push_back({GenerateName(16), GenerateType(complexityLimit)});
                    }
                    if (metatype == ELogicalMetatype::Struct) {
                        return StructLogicalType(fields);
                    } else {
                        return VariantStructLogicalType(fields);
                    }
                }

                case ELogicalMetatype::Dict:
                    return DictLogicalType(
                        GenerateType(complexityLimit),
                        GenerateType(complexityLimit));
                case ELogicalMetatype::Tagged:
                    return TaggedLogicalType(GenerateName(16), GenerateType(complexityLimit));
                case ELogicalMetatype::Decimal: {
                    int precision = GenerateByte() % 34 + 1;
                    int scale = GenerateByte() % (precision + 1);
                    return DecimalLogicalType(precision, scale);
                }
            }
        }
    }

    TString GenerateName(int maxLength = 16)
    {
        int length = GenerateByte() % maxLength;
        char res[length];
        for (int i = 0; i < length; ++i) {
            res[i] = char('a' + char(GenerateByte()) % ('z' - 'a' + 1));
        }
        return TString(res, length);
    }

    template <typename T>
    T GenerateEnum()
    {
        auto index = GenerateByte() % TEnumTraits<T>::GetDomainSize();
        return TEnumTraits<T>::GetDomainValues()[index];
    }

    int GenerateByte()
    {
        char result;
        if (EntropySource_->ReadChar(result)) {
            return static_cast<ui8>(result);
        } else {
            return 0;
        }
    }

private:
    IInputStream* const EntropySource_;
};

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size)
{
    TMemoryInput entropy(data, size);

    TLogicalTypePtr oldType = TFuzzLogicalTypeGenerator::Generate(&entropy);
    TLogicalTypePtr newType = TFuzzLogicalTypeGenerator::Generate(&entropy);

    CheckTypeCompatibility(oldType, newType);

    return 0;
}
