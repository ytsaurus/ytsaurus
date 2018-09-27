#pragma once

#include "property.h"
#include "public.h"

#include <yt/core/yson/public.h>

#include <yt/core/ytree/public.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Validates that a string is a correct arithmetic formula variable name.
void ValidateArithmeticFormulaVariable(const TString& variable);

//! Validates that a string is a correct boolean formula variable name.
void ValidateBooleanFormulaVariable(const TString& variable);

////////////////////////////////////////////////////////////////////////////////

class TGenericFormulaImpl;

////////////////////////////////////////////////////////////////////////////////

class TArithmeticFormula
{
public:
    // TODO(ifsmirnov): remove default ctor (first must make TNullable<T> serializable
    // for non-default-constructible T)
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

    //! Returns the list of variables used in the formula.
    THashSet<TString> GetVariables() const;

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

//! Make conjunction, disjunction and negation of formulas.
TBooleanFormula operator&(const TBooleanFormula& lhs, const TBooleanFormula& rhs);
TBooleanFormula operator|(const TBooleanFormula& lhs, const TBooleanFormula& rhs);
TBooleanFormula operator!(const TBooleanFormula& formula);

void Serialize(const TBooleanFormula& booleanFormula, NYson::IYsonConsumer* consumer);
void Deserialize(TBooleanFormula& booleanFormula, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

class TTimeFormula
{
public:
    TTimeFormula();
    TTimeFormula(const TTimeFormula& other);
    TTimeFormula(TTimeFormula&& other);
    TTimeFormula& operator=(const TTimeFormula& other);
    TTimeFormula& operator=(TTimeFormula&& other);
    ~TTimeFormula();

    bool operator==(const TTimeFormula& other) const;

    //! Returns true if formula is empty.
    bool IsEmpty() const;

    //! Returns number of tokens in parsed formula.
    int Size() const;

    //! Returns hash based on parsed formula.
    size_t GetHash() const;

    //! Returns a human-readable representation of the formula.
    TString GetFormula() const;

    //! Check that given time satisfies the formula.
    bool IsSatisfiedBy(TInstant time) const;

    void Save(TStreamSaveContext& context) const;
    void Load(TStreamLoadContext& context);

private:
    TArithmeticFormula Formula_;

    explicit TTimeFormula(TArithmeticFormula&& arithmeticFormula);

    friend TTimeFormula MakeTimeFormula(const TString& formula);
};

//! Parse string and return time formula.
TTimeFormula MakeTimeFormula(const TString& formula);

void Serialize(const TTimeFormula& timeFormula, NYson::IYsonConsumer* consumer);
void Deserialize(TTimeFormula& timeFormula, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

