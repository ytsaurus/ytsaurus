package yson

// IsTrue is a helper function for compatibility with C++ ConvertTo<bool>().
//
// It returns true, if value is bool(true) or string("true").
//
// Early versions of YSON was missing separate wire type for bool. Instead, string "true" was used to specify true value.
// C++ code still accepts string value where bool is expected, for compatibility reasons.
func IsTrue(value any) bool {
	switch v := value.(type) {
	case bool:
		return v
	case string:
		return v == "true"
	case nil:
		return false
	default:
		return false
	}
}
