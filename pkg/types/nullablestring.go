package types

import "encoding/json"

type NullableString struct {
	Value string
	Valid bool // Valid is true if Value is not nil
}

func (ns NullableString) String() string {
	if ns.Valid { // not needed, but for clarity
		return ns.Value
	}
	return ""
}

func (ns NullableString) IsNil() bool {
	return !ns.Valid
}

func (ns NullableString) Set(value string) {
	ns.Value = value
	ns.Valid = true
}

var _ json.Marshaler = &NullableString{}   // Ensure NullableString implements json.Marshaler
var _ json.Unmarshaler = &NullableString{} // Ensure NullableString implements json.Unmarshaler
var _ Nullable = &NullableString{}         // Ensure NullableString implements Nullable interface

// implement json.Marshaler interface
func (ns NullableString) MarshalJSON() ([]byte, error) {
	if ns.Valid {
		return json.Marshal(ns.Value)
	}
	return json.Marshal(nil)
}

func (ns *NullableString) UnmarshalJSON(data []byte) error {
	if len(data) == 0 {
		ns.Value = ""
		ns.Valid = false
		return nil
	}
	ns.Valid = true
	return json.Unmarshal(data, &ns.Value)
}
