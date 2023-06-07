package yterrors

func encodeNonASCII(s string) string {
	var result []byte
	for _, b := range []byte(s) {
		if b < 128 {
			result = append(result, b)
		} else {
			result = append(result, '\xC0'|b>>6)
			result = append(result, '\x80'|(b & ^byte('\xC0')))
		}
	}
	return string(result)
}

func decodeNonASCII(s string) string {
	var result []byte

	in := []byte(s)
	for i := 0; i < len(in); i++ {
		if in[i] < 128 {
			result = append(result, in[i])
		} else if i+1 < len(in) && (in[i]&'\xFC' == '\xC0') {
			decoded := (in[i]&'\x03')<<6 | (in[i+1] & '\x3F')
			result = append(result, decoded)
			i += 1
		} else {
			// C++ code throws exception here, but we are decoding errors, and don't care about correctness
			// in case of format misconfiguration.
			result = append(result, in[i])
		}
	}
	return string(result)
}

func fixStrings(v any, cb func(string) string) any {
	switch vv := v.(type) {
	case string:
		return cb(vv)
	case map[string]any:
		copy := map[string]any{}
		for k, v := range vv {
			copy[k] = fixStrings(v, cb)
		}
		return copy
	case []any:
		copy := []any{}
		for _, v := range vv {
			copy = append(copy, fixStrings(v, cb))
		}
		return copy
	default:
		return vv
	}
}
