package yson2json

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"

	"go.ytsaurus.tech/yt/go/yson"
)

const (
	valueKey      = "$value"
	attributesKey = "$attributes"
)

// RawMessage wraps raw JSON data and provides bidirectional conversion between YSON and JSON.
// It supports both marshalling JSON to YSON and unmarshalling YSON to JSON using streaming API.
type RawMessage struct {
	JSON json.RawMessage

	// UseInt64 controls conversion of JSON numbers during marshalling to YSON.
	//
	// When UseInt64 is true, JSON numbers that can be represented as int64
	// are converted to int64 in YSON instead of float64.
	UseInt64 bool

	// UseUint64 controls conversion of JSON numbers during marshalling to YSON.
	//
	// When UseUint64 is true, JSON numbers that can be represented as uint64
	// are converted to uint64 in YSON instead of float64.
	//
	// When both UseInt64 and UseUint64 are true, conversion to int64 is tried first.
	UseUint64 bool
}

func (m *RawMessage) UnmarshalYSON(r *yson.Reader) error {
	var buf bytes.Buffer

	err := m.writeValue(r, &buf)
	if err != nil {
		return err
	}

	m.JSON = buf.Bytes()
	return nil
}

func (m *RawMessage) writeValue(r *yson.Reader, w io.Writer) error {
	event, err := r.Next(false)
	if err != nil {
		return err
	}

	switch event {
	case yson.EventBeginAttrs:
		return m.writeValueWithAttributes(r, w)

	case yson.EventLiteral:
		return m.writeLiteral(r, w)

	case yson.EventBeginList:
		return m.writeList(r, w)

	case yson.EventBeginMap:
		return m.writeMap(r, w)

	default:
		return fmt.Errorf("unexpected event: %v", event)
	}
}

func (m *RawMessage) writeLiteral(r *yson.Reader, w io.Writer) error {
	switch r.Type() {
	case yson.TypeEntity:
		_, err := w.Write([]byte("null"))
		return err
	case yson.TypeBool:
		if r.Bool() {
			_, err := w.Write([]byte("true"))
			return err
		} else {
			_, err := w.Write([]byte("false"))
			return err
		}
	case yson.TypeString:
		return m.marshalJSONAndWrite(r.String(), w)
	case yson.TypeInt64:
		return m.marshalJSONAndWrite(r.Int64(), w)
	case yson.TypeUint64:
		return m.marshalJSONAndWrite(r.Uint64(), w)
	case yson.TypeFloat64:
		return m.marshalJSONAndWrite(r.Float64(), w)
	default:
		return fmt.Errorf("unexpected literal type: %v", r.Type())
	}
}

// marshalJSONAndWrite marshals a value to JSON and writes it to the writer.
func (m *RawMessage) marshalJSONAndWrite(value any, w io.Writer) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	_, err = w.Write(data)
	return err
}

func (m *RawMessage) writeList(r *yson.Reader, w io.Writer) error {
	if _, err := w.Write([]byte("[")); err != nil {
		return err
	}

	isFirst := true
	for {
		hasItem, err := r.NextListItem()
		if err != nil {
			return err
		}
		if !hasItem {
			break
		}

		if !isFirst {
			if _, err := w.Write([]byte(",")); err != nil {
				return err
			}
		}
		isFirst = false

		if err := m.writeValue(r, w); err != nil {
			return err
		}
	}

	event, err := r.Next(false)
	if err != nil {
		return err
	}
	if event != yson.EventEndList {
		return fmt.Errorf("expected end list, got: %v", event)
	}

	_, err = w.Write([]byte("]"))
	return err
}

func (m *RawMessage) writeMap(r *yson.Reader, w io.Writer) error {
	if _, err := w.Write([]byte("{")); err != nil {
		return err
	}

	if err := m.writeKeyValuePairs(r, w); err != nil {
		return err
	}

	event, err := r.Next(false)
	if err != nil {
		return err
	}
	if event != yson.EventEndMap {
		return fmt.Errorf("expected end map, got: %v", event)
	}

	_, err = w.Write([]byte("}"))
	return err
}

// writeKeyValuePairs writes a sequence of key-value pairs from YSON reader to JSON writer.
func (m *RawMessage) writeKeyValuePairs(r *yson.Reader, w io.Writer) error {
	isFirst := true
	for {
		hasKey, err := r.NextKey()
		if err != nil {
			return err
		}
		if !hasKey {
			break
		}

		if !isFirst {
			if _, err := w.Write([]byte(",")); err != nil {
				return err
			}
		}
		isFirst = false

		if err := m.writeKeyValue(r, w); err != nil {
			return err
		}
	}
	return nil
}

// writeKeyValue writes a single key-value pair from YSON reader to JSON writer.
func (m *RawMessage) writeKeyValue(r *yson.Reader, w io.Writer) error {
	key := r.String()
	keyData, err := json.Marshal(key)
	if err != nil {
		return err
	}
	if _, err := w.Write(keyData); err != nil {
		return err
	}

	if _, err := w.Write([]byte(":")); err != nil {
		return err
	}

	return m.writeValue(r, w)
}

func (m *RawMessage) writeValueWithAttributes(r *yson.Reader, w io.Writer) error {
	if _, err := w.Write([]byte("{\"" + attributesKey + "\":{")); err != nil {
		return err
	}

	if err := m.writeKeyValuePairs(r, w); err != nil {
		return err
	}

	if _, err := w.Write([]byte("}," + "\"" + valueKey + "\"" + ":")); err != nil {
		return err
	}

	// Consume end attrs event.
	event, err := r.Next(false)
	if err != nil {
		return err
	}
	if event != yson.EventEndAttrs {
		return fmt.Errorf("expected end attrs, got: %v", event)
	}

	if err := m.writeValue(r, w); err != nil {
		return err
	}

	_, err = w.Write([]byte("}"))
	return err
}

func (m RawMessage) MarshalYSON(w *yson.Writer) error {
	d := json.NewDecoder(bytes.NewBuffer(m.JSON))

	if m.UseInt64 || m.UseUint64 {
		d.UseNumber()
	}

	var mapKey bool
	var inMap []bool
	var maybeValueWithAttributes bool

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
				maybeValueWithAttributes = true
				inMap = append(inMap, true)
			case '}':
				if maybeValueWithAttributes {
					maybeValueWithAttributes = false
					w.BeginMap()
				}
				w.EndMap()
				inMap = inMap[:len(inMap)-1]
			default:
				panic("invalid delim")
			}

		case string:
			if maybeValueWithAttributes {
				if v == valueKey || v == attributesKey {
					inMap = inMap[:len(inMap)-1]
					mapKey = false
					valueWithAttrs, err := parseValueWithAttributes(d, v)
					if err != nil {
						return fmt.Errorf("failed to parse value with attributes: %w", err)
					}
					if err = valueWithAttrs.marshalYSON(w, m.UseInt64, m.UseUint64); err != nil {
						return fmt.Errorf("failed to marshal value with attributes: %w", err)
					}
					if err := parseObjectFinishDelim(d); err != nil {
						return err
					}
				} else {
					w.BeginMap()
					mapKey = true
					w.MapKeyString(v)
				}

				maybeValueWithAttributes = false
			} else if mapKey {
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

type kv struct {
	k string
	v json.RawMessage
}

type valueWithAttributes struct {
	value      json.RawMessage
	attributes map[string]json.RawMessage
}

func (v *valueWithAttributes) marshalYSON(w *yson.Writer, useInt64, useUint64 bool) error {
	sv := make([]kv, 0, len(v.attributes))
	for k, v := range v.attributes {
		sv = append(sv, kv{k: k, v: v})
	}
	sort.Slice(sv, func(i, j int) bool { return sv[i].k < sv[j].k })

	w.BeginAttrs()
	for _, item := range sv {
		w.MapKeyString(item.k)
		rawMessage := RawMessage{JSON: item.v, UseInt64: useInt64, UseUint64: useUint64}
		if err := rawMessage.MarshalYSON(w); err != nil {
			return fmt.Errorf("failed to marshal attribute %q: %w", item.k, err)
		}
	}
	w.EndAttrs()

	rawMessage := RawMessage{JSON: v.value, UseInt64: useInt64, UseUint64: useUint64}
	if err := rawMessage.MarshalYSON(w); err != nil {
		return fmt.Errorf("failed to marshal value: %w", err)
	}
	return nil
}

func parseValueWithAttributes(d *json.Decoder, firstKey string) (*valueWithAttributes, error) {
	var valueWithAttrs valueWithAttributes
	keyToDecodedValue := map[string]any{
		valueKey:      &valueWithAttrs.value,
		attributesKey: &valueWithAttrs.attributes,
	}
	secondKey := valueKey
	if firstKey == valueKey {
		secondKey = attributesKey
	}

	if err := d.Decode(keyToDecodedValue[firstKey]); err != nil {
		return nil, err
	}

	nextKey, err := parseStringToken(d)
	if err != nil {
		return nil, err
	}

	if nextKey != secondKey {
		return nil, fmt.Errorf("%s without %s", firstKey, secondKey)
	}

	if err := d.Decode(keyToDecodedValue[secondKey]); err != nil {
		return nil, err
	}

	return &valueWithAttrs, nil
}

func parseStringToken(d *json.Decoder) (string, error) {
	tok, err := d.Token()
	if err != nil {
		return "", err
	}
	s, ok := tok.(string)
	if !ok {
		return "", fmt.Errorf("token %v is not a string", s)
	}
	return s, nil
}

func parseObjectFinishDelim(d *json.Decoder) error {
	tok, err := d.Token()
	if err != nil {
		return err
	}
	delim, ok := tok.(json.Delim)
	if !ok {
		return fmt.Errorf("token %v is not a delim", tok)
	}
	if delim != '}' {
		return fmt.Errorf("token %v is not an object finish delim", tok)
	}
	return nil
}

var (
	_ yson.StreamMarshaler   = RawMessage{}
	_ yson.StreamUnmarshaler = (*RawMessage)(nil)
)
