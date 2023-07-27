EXECTEST()

OWNER(
    insight
    spreis
)

LICENSE(MIT)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

RUN(fastops_test)

DEPENDS(contrib/libs/fastops/fastops/ut/bin)

SIZE(LARGE)

TAG(
    ya:fat
    ya:force_sandbox
    sb:intel_e5_2660v4
)

END()

RECURSE(
    bin
)
