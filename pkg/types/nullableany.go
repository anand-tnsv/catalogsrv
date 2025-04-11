package types

import "encoding/json"

type NullableAny struct {
	Value any
	Valid bool // Valid is true if Value is not nil
}

func (ns NullableAny) IsNil() bool {
	return !ns.Valid
}

func (ns NullableAny) Set(value any) {
	ns.Value = value
	ns.Valid = true
	_ = ns
}

var _ json.Marshaler = &NullableAny{}   // Ensure NullableString implements json.Marshaler
var _ json.Unmarshaler = &NullableAny{} // Ensure NullableString implements json.Unmarshaler
var _ Nullable = &NullableAny{}         // Ensure NullableString implements Nullable interface

// implement json.Marshaler interface
func (ns NullableAny) MarshalJSON() ([]byte, error) {
	if ns.Valid {
		return json.Marshal(ns.Value)
	}
	return json.Marshal(nil)
}

func (ns *NullableAny) UnmarshalJSON(data []byte) error {
	if len(data) == 0 {
		ns.Value = nil
		ns.Valid = false
		return nil
	}
	err := json.Unmarshal(data, &ns.Value)
	if err != nil {
		return err
	}
	ns.Valid = true
	return nil
}
