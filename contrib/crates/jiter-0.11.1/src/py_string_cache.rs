use std::sync::{Mutex, MutexGuard, OnceLock};

use ahash::random_state::RandomState;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyString};

use crate::string_decoder::StringOutput;

#[derive(Debug, Clone, Copy)]
pub enum StringCacheMode {
    All,
    Keys,
    None,
}

impl Default for StringCacheMode {
    fn default() -> Self {
        Self::All
    }
}

impl<'py> FromPyObject<'py> for StringCacheMode {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<StringCacheMode> {
        if let Ok(bool_mode) = ob.cast::<PyBool>() {
            Ok(bool_mode.is_true().into())
        } else if let Ok(str_mode) = ob.extract::<&str>() {
            match str_mode {
                "all" => Ok(Self::All),
                "keys" => Ok(Self::Keys),
                "none" => Ok(Self::None),
                _ => Err(PyValueError::new_err(
                    "Invalid string cache mode, should be `'all'`, '`keys`', `'none`' or a `bool`",
                )),
            }
        } else {
            Err(PyTypeError::new_err(
                "Invalid string cache mode, should be `'all'`, '`keys`', `'none`' or a `bool`",
            ))
        }
    }
}

impl From<bool> for StringCacheMode {
    fn from(mode: bool) -> Self {
        if mode {
            Self::All
        } else {
            Self::None
        }
    }
}

pub trait StringMaybeCache {
    fn get_key<'py>(py: Python<'py>, string_output: StringOutput<'_, '_>) -> Bound<'py, PyString>;

    fn get_value<'py>(py: Python<'py>, string_output: StringOutput<'_, '_>) -> Bound<'py, PyString> {
        Self::get_key(py, string_output)
    }
}

pub struct StringCacheAll;

impl StringMaybeCache for StringCacheAll {
    fn get_key<'py>(py: Python<'py>, string_output: StringOutput<'_, '_>) -> Bound<'py, PyString> {
        // Safety: string_output carries the safety information
        unsafe { cached_py_string_maybe_ascii(py, string_output.as_str(), string_output.ascii_only()) }
    }
}

pub struct StringCacheKeys;

impl StringMaybeCache for StringCacheKeys {
    fn get_key<'py>(py: Python<'py>, string_output: StringOutput<'_, '_>) -> Bound<'py, PyString> {
        // Safety: string_output carries the safety information
        unsafe { cached_py_string_maybe_ascii(py, string_output.as_str(), string_output.ascii_only()) }
    }

    fn get_value<'py>(py: Python<'py>, string_output: StringOutput<'_, '_>) -> Bound<'py, PyString> {
        unsafe { pystring_fast_new_maybe_ascii(py, string_output.as_str(), string_output.ascii_only()) }
    }
}

pub struct StringNoCache;

impl StringMaybeCache for StringNoCache {
    fn get_key<'py>(py: Python<'py>, string_output: StringOutput<'_, '_>) -> Bound<'py, PyString> {
        unsafe { pystring_fast_new_maybe_ascii(py, string_output.as_str(), string_output.ascii_only()) }
    }
}

static STRING_CACHE: OnceLock<Mutex<PyStringCache>> = OnceLock::new();

#[inline]
fn get_string_cache() -> MutexGuard<'static, PyStringCache> {
    match STRING_CACHE.get_or_init(|| Mutex::new(PyStringCache::default())).lock() {
        Ok(cache) => cache,
        Err(poisoned) => {
            let mut cache = poisoned.into_inner();
            // worst case if we panic while the cache is held, we just clear and keep going
            cache.clear();
            cache
        }
    }
}

pub fn cache_usage() -> usize {
    get_string_cache().usage()
}

pub fn cache_clear() {
    get_string_cache().clear();
}

/// Create a cached Python `str` from a string slice
#[inline]
pub fn cached_py_string<'py>(py: Python<'py>, s: &str) -> Bound<'py, PyString> {
    // SAFETY: not setting ascii-only
    unsafe { cached_py_string_maybe_ascii(py, s, false) }
}

/// Create a cached Python `str` from a string slice.
///
/// # Safety
///
/// Caller must pass ascii-only string.
#[inline]
pub unsafe fn cached_py_string_ascii<'py>(py: Python<'py>, s: &str) -> Bound<'py, PyString> {
    // SAFETY: caller upholds invariant
    unsafe { cached_py_string_maybe_ascii(py, s, true) }
}

/// # Safety
///
/// Caller must match the ascii_only flag to the string passed in.
unsafe fn cached_py_string_maybe_ascii<'py>(py: Python<'py>, s: &str, ascii_only: bool) -> Bound<'py, PyString> {
    // from tests, 0 and 1 character strings are faster not cached
    if (2..64).contains(&s.len()) {
        get_string_cache().get_or_insert(py, s, ascii_only)
    } else {
        pystring_fast_new_maybe_ascii(py, s, ascii_only)
    }
}

// capacity should be a power of 2 so the compiler can convert `%` to a right shift below
// Using a smaller number here (e.g. 1024) seems to be faster in many cases than a larger number (like 65536)
// and also avoids stack overflow risks
const CAPACITY: usize = 16_384;
type Entry = Option<(u64, Py<PyString>)>;

/// This is a Fully associative cache with LRU replacement policy.
/// See https://en.wikipedia.org/wiki/Cache_placement_policies#Fully_associative_cache
#[derive(Debug)]
struct PyStringCache {
    entries: Box<[Entry; CAPACITY]>,
    hash_builder: RandomState,
}

const ARRAY_REPEAT_VALUE: Entry = None;

impl Default for PyStringCache {
    fn default() -> Self {
        Self {
            #[allow(clippy::large_stack_arrays)]
            entries: Box::new([ARRAY_REPEAT_VALUE; CAPACITY]),
            hash_builder: RandomState::default(),
        }
    }
}

impl PyStringCache {
    /// Lookup the cache for an entry with the given string. If it exists, return it.
    /// If it is not set or has a different string, insert it and return it.
    ///
    /// # Safety
    ///
    /// `ascii_only` must only be set to `true` if the string is guaranteed to be ASCII only.
    unsafe fn get_or_insert<'py>(&mut self, py: Python<'py>, s: &str, ascii_only: bool) -> Bound<'py, PyString> {
        let hash = self.hash_builder.hash_one(s);

        let hash_index = hash as usize % CAPACITY;

        let set_entry = |entry: &mut Entry| {
            // SAFETY: caller upholds invariant
            let py_str = unsafe { pystring_fast_new_maybe_ascii(py, s, ascii_only) };
            if let Some((_, old_py_str)) = entry.replace((hash, py_str.clone().unbind())) {
                // micro-optimization: bind the old entry before dropping it so that PyO3 can
                // fast-path the drop (Bound::drop is faster than Py::drop)
                drop(old_py_str.into_bound(py));
            }
            py_str
        };

        // we try up to 5 contiguous slots to find a match or an empty slot
        for index in hash_index..hash_index.wrapping_add(5) {
            if let Some(entry) = self.entries.get_mut(index) {
                if let Some((entry_hash, py_str_ob)) = entry {
                    // to avoid a string comparison, we first compare the hashes
                    if *entry_hash == hash {
                        // if the hashes match, we compare the strings to be absolutely sure - as a hashmap would do
                        if py_str_ob.bind(py) == s {
                            // the strings matched, return the cached string object
                            return py_str_ob.bind(py).to_owned();
                        }
                    }
                } else {
                    // we got to an empty entry, use it
                    return set_entry(entry);
                }
            } else {
                // we reached the end of entries, break
                break;
            }
        }
        // we tried all 5 slots (or got to the end of entries) without finding a match
        // or an empty slot, make this LRU by replacing the first entry
        let entry = self.entries.get_mut(hash_index).unwrap();
        set_entry(entry)
    }

    /// get the number of entries in the cache that are set
    fn usage(&self) -> usize {
        self.entries.iter().filter(|e| e.is_some()).count()
    }

    /// clear the cache by resetting all entries to `None`
    fn clear(&mut self) {
        self.entries.fill_with(|| None);
    }
}

/// Creatate a new Python `str` from a string slice, with a fast path for ASCII strings
///
/// # Safety
///
/// `ascii_only` must only be set to `true` if the string is guaranteed to be ASCII only.
unsafe fn pystring_fast_new_maybe_ascii<'py>(py: Python<'py>, s: &str, ascii_only: bool) -> Bound<'py, PyString> {
    if ascii_only {
        // SAFETY: caller upholds invariant
        unsafe { pystring_ascii_new(py, s) }
    } else {
        PyString::new(py, s)
    }
}

/// Faster creation of PyString from an ASCII string, inspired by
/// https://github.com/ijl/orjson/blob/3.10.0/src/str/create.rs#L41
///
/// # Safety
///
/// `s` must be ASCII only
pub unsafe fn pystring_ascii_new<'py>(py: Python<'py>, s: &str) -> Bound<'py, PyString> {
    #[cfg(not(any(PyPy, GraalPy, Py_LIMITED_API)))]
    {
        let ptr = pyo3::ffi::PyUnicode_New(s.len() as isize, 127);
        // see https://github.com/pydantic/jiter/pull/72#discussion_r1545485907
        debug_assert_eq!(pyo3::ffi::PyUnicode_KIND(ptr), pyo3::ffi::PyUnicode_1BYTE_KIND);
        let data_ptr = pyo3::ffi::PyUnicode_DATA(ptr).cast();
        core::ptr::copy_nonoverlapping(s.as_ptr(), data_ptr, s.len());
        core::ptr::write(data_ptr.add(s.len()), 0);
        Bound::from_owned_ptr(py, ptr).cast_into_unchecked()
    }

    #[cfg(any(PyPy, GraalPy, Py_LIMITED_API))]
    {
        PyString::new(py, s)
    }
}
