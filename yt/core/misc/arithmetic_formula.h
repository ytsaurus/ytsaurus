#pragma once

#include "property.h"
#include "public.h"

#include <yt/core/yson/public.h>

#include <yt/core/ytree/public.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Validates that a string is a correct formula variable name.
void ValidateFormulaVariable(const TString& variable);

////////////////////////////////////////////////////////////////////////////////

class TGenericFormulaImpl;

////////////////////////////////////////////////////////////////////////////////

class TArithmeticFormula
{
public:
    TArithmeticFormula();
    TArithmeticFormula(const TArithmeticFormula& other);
    TArithmeticFormula(TArithmeticFormula&& other);
    TArithmeticFormula& operator=(const TArithmeticFormula& other);
    TArithmeticFormula& operator=(TArithmeticFormula&& other);
    ~TArithmeticFormula();

    bool operator==(const TArithmeticFormula& other) const;

    //! Returns true if formula is empty.
    bool IsEmpty() const;

    //! Returns number of tokens in parsed formula.
    int Size() const;

    //! Returns hash based on parsed formula.
    size_t GetHash() const;

    //! Returns a human-readable representation of the formula.
    TString GetFormula() const;

    //! Evaluate the formula given values of variables.
    i64 Eval(const THashMap<TString, i64>& values) const;

    void Save(TStreamSaveContext& context) const;
    void Load(TStreamLoadContext& context);

private:
    TIntrusivePtr<TGenericFormulaImpl> Impl_;

    explicit TArithmeticFormula(TIntrusivePtr<TGenericFormulaImpl> impl);

    friend TArithmeticFormula MakeArithmeticFormula(const TString& formula);
};

//! Parse string and return arithmetic formula.
TArithmeticFormula MakeArithmeticFormula(const TString& formula);

void Serialize(const TArithmeticFormula& arithmeticFormula, NYson::IYsonConsumer* consumer);
void Deserialize(TArithmeticFormula& arithmeticFormula, NYTree::INodePtr node);

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
    TIntrusivePtr<TGenericFormulaImpl> Impl_;

    explicit TBooleanFormula(TIntrusivePtr<TGenericFormulaImpl> impl);

    friend TBooleanFormula MakeBooleanFormula(const TString& formula);
};

//! Parse string and return boolean formula.
TBooleanFormula MakeBooleanFormula(const TString& formula);

void Serialize(const TBooleanFormula& booleanFormula, NYson::IYsonConsumer* consumer);
void Deserialize(TBooleanFormula& booleanFormula, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

