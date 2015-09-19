#include <yt_udf_cpp.h>

class RE2;

extern "C" RE2* RegexCreate(TUnversionedValue*);
extern "C" bool RegexFullMatch(RE2*, TUnversionedValue*);
extern "C" void RegexDestroy(RE2*);

struct TData
{
    TData(TUnversionedValue* regexp)
        : Re2(RegexCreate(regexp))
    { }

    ~TData()
    {
        RegexDestroy(Re2);
    }

    RE2* Re2;
};

extern "C" void regex_match(
    TExecutionContext* executionContext,
    NYT::NQueryClient::TFunctionContext* functonContext,
    TUnversionedValue* result,
    TUnversionedValue* regexp,
    TUnversionedValue* input)
{
    bool found;

    if (!functonContext->IsArgLiteral(0)) {
        auto data = TData(regexp);
        found = RegexFullMatch(data.Re2, input);
    } else {
        void* data = functonContext->GetPrivateData();
        if (!data) {
            data = functonContext->CreateObject<TData>(regexp);
            functonContext->SetPrivateData(data);
        }
        found = RegexFullMatch(static_cast<TData*>(data)->Re2, input);
    }

    result->Type = Boolean;
    result->Data.Boolean = found;
}
