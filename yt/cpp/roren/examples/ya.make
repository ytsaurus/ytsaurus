RECURSE(
    01_par_do
    02_group_by_key
    03_combine_by_key
    04_par_do_multiple_outputs
    05_co_group_by_key
)

IF (NOT OPENSOURCE)
    RECURSE(06_stateful_par_do)
ENDIF()

