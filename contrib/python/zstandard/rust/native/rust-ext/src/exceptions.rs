// Copyright (c) 2020-present, Gregory Szorc
// All rights reserved.
//
// This software may be modified and distributed under the terms
// of the BSD license. See the LICENSE file for details.

use pyo3::{create_exception, exceptions::PyException, prelude::*};

create_exception!(module, ZstdError, PyException);

pub(crate) fn init_module(py: Python, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("ZstdError", py.get_type::<ZstdError>())?;

    Ok(())
}
