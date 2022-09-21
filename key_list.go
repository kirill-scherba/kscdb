// Copyright 2022 Kirill Scherba <kirill@scherba.ru>.  All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Keylist module

package kscdb

import (
	"encoding/json"
	"sort"
	"strings"
)

// KeyList is strings array of keys
type KeyList struct {
	keys []string
}

// Append one key or range of keys to KeyList keys slice
func (k *KeyList) Append(keys ...string) {
	k.keys = append(k.keys, keys...)
}

// Keys return keys string slice
func (k *KeyList) Keys() []string {
	return k.keys
}

// String return keys in string divided by end of line
func (k *KeyList) String() string {
	var str string
	for i, s := range k.Keys() {
		if i > 0 {
			str += "\n"
		}
		str += s
	}
	return str
}

// Len return length of keys array
func (k *KeyList) Len() int {
	return len(k.keys)
}

// MarshalJSON returns the JSON encoding
func (k *KeyList) MarshalJSON() (data []byte, err error) {
	jdata := k.Keys()
	sort.Strings(jdata)
	data, err = json.Marshal(jdata)
	return
}

// MarshalBinary marshal Keylist (string slice) to byte slice with \0x00 separator
func (k *KeyList) MarshalBinary() (data []byte, err error) {
	for i, key := range k.keys {
		if i > 0 {
			data = append(data, 0)
		}
		data = append(data, []byte(key)...)
	}
	return
}

// UnmarshalBinary unmarshal byte slice with \0x00 separator to Keylist (string slice)
func (k *KeyList) UnmarshalBinary(data []byte) (err error) {
	if len(data) == 0 {
		return
	}
	k.keys = strings.Split(string(data), "\x00")
	return
}
