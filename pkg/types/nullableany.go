package types

import (
	"encoding/json"
	"errors"
)

type NullableAny struct {
	value json.RawMessage
	valid bool // Valid is true if Value is not nil
}

func (ns *NullableAny) IsNil() bool {
	return !ns.valid
}

func (ns *NullableAny) Set(value any) error {
	var jsonValue json.RawMessage

	switch v := value.(type) {
	case json.RawMessage:
		// If already a json.RawMessage, validate it
		if !json.Valid(v) {
			ns.value = nil
			ns.valid = false
			return errors.New("value is not valid JSON")
		}
		jsonValue = v
	case []byte:
		// Check if []byte contains valid JSON
		if !json.Valid(v) {
			// If not valid JSON, try marshaling it
			marshaledValue, err := json.Marshal(value)
			if err != nil {
				ns.value = nil
				ns.valid = false
				return err
			}
			jsonValue = marshaledValue
		} else {
			jsonValue = v
		}
	default:
		// Marshal any other type
		marshaledValue, err := json.Marshal(value)
		if err != nil {
			ns.value = nil
			ns.valid = false
			return err
		}
		jsonValue = marshaledValue
	}

	// Assign validated/marshaled value
	ns.value = jsonValue
	ns.valid = true
	return nil
}

func (ns *NullableAny) Get() any {
	if ns.valid {
		var v any
		err := json.Unmarshal(ns.value, &v)
		if err != nil {
			return nil
		}
		return v
	}
	return nil
}

func (ns *NullableAny) GetAs(v any) error {
	if ns.valid {
		return json.Unmarshal(ns.value, v)
	}
	return errors.New("value is not set")
}

var _ json.Marshaler = &NullableAny{}   // Ensure NullableString implements json.Marshaler
var _ json.Unmarshaler = &NullableAny{} // Ensure NullableString implements json.Unmarshaler
var _ Nullable = &NullableAny{}         // Ensure NullableString implements Nullable interface

var _ json.Marshaler = NullableAny{}

// implement json.Marshaler interface

func (ns NullableAny) MarshalJSON() ([]byte, error) {
	if ns.valid {
		return ns.value, nil
	}
	return json.Marshal(nil)
}

func (ns *NullableAny) UnmarshalJSON(data []byte) error {
	if len(data) == 0 {
		ns.value = nil
		ns.valid = false
		return nil
	}
	if !json.Valid(data) {
		return errors.New("invalid JSON")
	}
	ns.value = data
	ns.valid = true
	return nil
}
