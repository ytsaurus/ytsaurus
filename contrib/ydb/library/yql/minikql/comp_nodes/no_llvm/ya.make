LIBRARY()

CXXFLAGS(-DMKQL_DISABLE_CODEGEN)

ADDINCL(GLOBAL contrib/ydb/library/yql/minikql/codegen/llvm_stub)

INCLUDE(../ya.make.inc)

PEERDIR(contrib/ydb/library/yql/minikql/invoke_builtins/no_llvm)

END()
