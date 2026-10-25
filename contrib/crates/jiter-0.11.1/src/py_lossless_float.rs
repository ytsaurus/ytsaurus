use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::PyType;

use crate::Jiter;

#[derive(Debug, Clone, Copy)]
pub enum FloatMode {
    Float,
    Decimal,
    LosslessFloat,
}

impl Default for FloatMode {
    fn default() -> Self {
        Self::Float
    }
}

const FLOAT_ERROR: &str = "Invalid float mode, should be `'float'`, `'decimal'` or `'lossless-float'`";

impl<'py> FromPyObject<'py> for FloatMode {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        if let Ok(str_mode) = ob.extract::<&str>() {
            match str_mode {
                "float" => Ok(Self::Float),
                "decimal" => Ok(Self::Decimal),
                "lossless-float" => Ok(Self::LosslessFloat),
                _ => Err(PyValueError::new_err(FLOAT_ERROR)),
            }
        } else {
            Err(PyTypeError::new_err(FLOAT_ERROR))
        }
    }
}

/// Represents a float from JSON, by holding the underlying bytes representing a float from JSON.
#[derive(Debug, Clone)]
#[pyclass(module = "jiter")]
pub struct LosslessFloat(Vec<u8>);

impl LosslessFloat {
    pub fn new_unchecked(raw: Vec<u8>) -> Self {
        Self(raw)
    }
}

#[pymethods]
impl LosslessFloat {
    #[new]
    fn new(raw: Vec<u8>) -> PyResult<Self> {
        let s = Self(raw);
        // check the string is valid by calling `as_float`
        s.__float__()?;
        Ok(s)
    }

    fn as_decimal<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let decimal = get_decimal_type(py)?;
        let float_str = self.__str__()?;
        decimal.call1((float_str,))
    }

    fn __float__(&self) -> PyResult<f64> {
        let bytes = &self.0;
        let mut jiter = Jiter::new(bytes).with_allow_inf_nan();
        let f = jiter
            .next_float()
            .map_err(|e| PyValueError::new_err(e.description(&jiter)))?;
        jiter
            .finish()
            .map_err(|e| PyValueError::new_err(e.description(&jiter)))?;
        Ok(f)
    }

    fn __bytes__(&self) -> &[u8] {
        &self.0
    }

    fn __str__(&self) -> PyResult<&str> {
        std::str::from_utf8(&self.0).map_err(|_| PyValueError::new_err("Invalid UTF-8"))
    }

    fn __repr__(&self) -> PyResult<String> {
        self.__str__().map(|s| format!("LosslessFloat({s})"))
    }
}

static DECIMAL_TYPE: PyOnceLock<Py<PyType>> = PyOnceLock::new();

pub fn get_decimal_type(py: Python<'_>) -> PyResult<&Bound<'_, PyType>> {
    DECIMAL_TYPE.import(py, "decimal", "Decimal")
}
