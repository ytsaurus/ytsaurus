# !/usr/bin/python3

from yt.yt.orm.codegen.generator import (
    CodegenParameters,
    AuxiliaryParameters,
    dispatch,
)

if __name__ == "__main__":
    dispatch(
        obj=CodegenParameters(
            aux_parameters=AuxiliaryParameters(
                error_cpp_namespace="NYT::NOrm::NClient",
            ),
        )
    )
