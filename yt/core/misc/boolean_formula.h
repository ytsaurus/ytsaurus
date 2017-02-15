#pragma once

#include "property.h"
#include "public.h"

#include <yt/core/yson/public.h>

#include <yt/core/ytree/public.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Validates that a string is a correct boolean variable name.
void ValidateBooleanFormulaVariable(const Stroka& variable);

////////////////////////////////////////////////////////////////////////////////

class TBooleanFormula
{
public:
    TBooleanFormula();
    TBooleanFormula(TBooleanFormula&& other);
    TBooleanFormula& operator=(TBooleanFormula&& other);
    ~TBooleanFormula();

    //! Returns a human-readable representation of the formula.
    Stroka GetFormula() const;

    //! Check that a given set of true-variables satisfies the formula.
    bool IsSatisfiedBy(const std::vector<Stroka>& value) const;
    bool IsSatisfiedBy(const yhash_set<Stroka>& value) const;

    void Save(TStreamSaveContext& context) const;
    void Load(TStreamLoadContext& context);

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;

    explicit TBooleanFormula(std::unique_ptr<TImpl> impl);

    friend std::unique_ptr<TImpl> MakeBooleanFormulaImpl(const Stroka& formula);
    friend TBooleanFormula MakeBooleanFormula(const Stroka& formula);
};

//! Parse string and return formula.
TBooleanFormula MakeBooleanFormula(const Stroka& formula);

void Serialize(const TBooleanFormula& booleanFormula, NYson::IYsonConsumer* consumer);
void Deserialize(TBooleanFormula& booleanFormula, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

