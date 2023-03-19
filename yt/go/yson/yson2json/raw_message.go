package yson2json

import (
	"bytes"
	"encoding/json"
	"errors"
	"strconv"

	"go.ytsaurus.tech/yt/go/yson"
)

// RawMessage is type that wraps raw JSON message and marshals it directly to YSON using streaming API
type RawMessage struct {
	JSON json.RawMessage

	// UseInt64 controls conversion of JSON numbers.
	//
	// When UseInt64 is set to true, json numbers that can be represented as int64
	// are converted to int64 in yson.
	UseInt64 bool

	// UseUint64 controls conversion of JSON numbers.
	//
	// When UseUint64 is set to true, json numbers that can be represented as uint64
	// are converted to uint64 in yson.
	//
	// When both UseInt64 and UseUint64 are set to true, conversion to int64 tried first.
	UseUint64 bool
}

func (m *RawMessage) UnmarshalYSON(r *yson.Reader) error {
	return errors.New("yson2json: unmarshal is not implemented")
}

func (m RawMessage) MarshalYSON(w *yson.Writer) error {
	d := json.NewDecoder(bytes.NewBuffer(m.JSON))

	if m.UseInt64 || m.UseUint64 {
		d.UseNumber()
	}

	var mapKey bool
	var inMap []bool

	isInMap := func() bool {
		return len(inMap) != 0 && inMap[len(inMap)-1]
	}

	for {
		tok, err := d.Token()
		if err != nil {
			return err
		}

		switch v := tok.(type) {
		case bool:
			w.Bool(v)

		case float64:
			w.Float64(v)

		case json.Number:
			if m.UseInt64 {
				i, err := strconv.ParseInt(v.String(), 10, 64)
				if err == nil {
					w.Int64(i)
					break
				}
			}

			if m.UseUint64 {
				u, err := strconv.ParseUint(v.String(), 10, 64)
				if err == nil {
					w.Uint64(u)
					break
				}
			}

			d, err := strconv.ParseFloat(v.String(), 64)
			if err != nil {
				return err
			}

			w.Float64(d)

		case json.Delim:
			mapKey = false

			switch v {
			case '[':
				w.BeginList()
				inMap = append(inMap, false)
			case ']':
				w.EndList()
				inMap = inMap[:len(inMap)-1]
			case '{':
				w.BeginMap()
				inMap = append(inMap, true)
			case '}':
				w.EndMap()
				inMap = inMap[:len(inMap)-1]
			default:
				panic("invalid delim")
			}

		case string:
			if mapKey {
				w.MapKeyString(v)
			} else {
				w.String(v)
			}

		case nil:
			w.Entity()
		}

		if isInMap() {
			mapKey = !mapKey
		}

		if len(inMap) == 0 {
			break
		}
	}

	return w.Err()
}

var (
	_ yson.StreamMarshaler   = RawMessage{}
	_ yson.StreamUnmarshaler = (*RawMessage)(nil)
)
