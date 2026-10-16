# Deliberately overrides build/conf/docs.conf's --no-strict default so YFM
# validation diagnostics fail the build instead of just printing. Verified
# only by manual --rebuild proof; no DOCS()-target negative-build CI harness exists.
SET(_DOCS_YFM_COMMON_ARGS ${hide;requirements:"cpu:4"} -j2 --strict --allow-html --allow-custom-resources
    -e unarchive-includer
    -q
)
