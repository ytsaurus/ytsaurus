#pragma once

#include "property.h"
#include "public.h"

#include <yt/core/yson/public.h>

#include <yt/core/ytree/public.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Validates that a string is a correct boolean variable name.
void ValidateBooleanFormulaVariable(const TString& variable);

////////////////////////////////////////////////////////////////////////////////

class TBooleanFormula
{
public:
    TBooleanFormula();
    TBooleanFormula(const TBooleanFormula& other);
    TBooleanFormula(TBooleanFormula&& other);
    TBooleanFormula& operator=(const TBooleanFormula& other);
    TBooleanFormula& operator=(TBooleanFormula&& other);
    ~TBooleanFormula();

    bool operator==(const TBooleanFormula& other) const;

    //! Returns true if formula is empty.
    bool IsEmpty() const;

    //! Returns number of tokens in parsed formula.
    int Size() const;

    //! Returns hash based on parsed formula.
    size_t GetHash() const;

    //! Returns a human-readable representation of the formula.
    TString GetFormula() const;

    //! Check that a given set of true-variables satisfies the formula.
    bool IsSatisfiedBy(const std::vector<TString>& value) const;
    bool IsSatisfiedBy(const THashSet<TString>& value) const;

    void Save(TStreamSaveContext& context) const;
    void Load(TStreamLoadContext& context);

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl_;

    explicit TBooleanFormula(TIntrusivePtr<TImpl> impl);

    friend TIntrusivePtr<TImpl> MakeBooleanFormulaImpl(const TString& formula);
    friend TBooleanFormula MakeBooleanFormula(const TString& formula);
};

//! Parse string and return formula.
TBooleanFormula MakeBooleanFormula(const TString& formula);

void Serialize(const TBooleanFormula& booleanFormula, NYson::IYsonConsumer* consumer);
void Deserialize(TBooleanFormula& booleanFormula, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

