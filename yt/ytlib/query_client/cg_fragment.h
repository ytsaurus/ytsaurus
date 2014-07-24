#pragma once

#include "cg_types.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TCGFragment
{
public:
    TCGFragment();
    ~TCGFragment();

    llvm::Module* GetModule() const;

    llvm::Function* GetRoutine(const Stroka& symbol) const;

    void Embody(llvm::Function* body);

    TCgFunction GetCompiledBody();

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

