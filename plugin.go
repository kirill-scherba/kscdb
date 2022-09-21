// Copyright 2022 Kirill Scherba <kirill@scherba.ru>.  All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Plugin module

package kscdb

import (
	"bytes"
	"encoding/binary"
	"io"
)

// Plugin is plugins function and parameters used in requests
// and responce teonet commands.
type Plugin struct {
	ID            uint32   // Packet id
	Name          string   // Plugin name
	Func          string   // Function name
	Params        []string // Function parameters
	RequestInJSON bool     // Request packet format
}

// Empty clears KeyValue values to it default values.
func (p *Plugin) Empty() {
	p.ID = 0
	p.Name = ""
	p.Func = ""
	p.Params = []string{}
	p.RequestInJSON = false
}

// MarshalBinary encodes Plugin receiver data into binary buffer and returns
// it in byte slice.
func (p Plugin) MarshalBinary() (data []byte, err error) {
	buf := new(bytes.Buffer)
	le := binary.LittleEndian

	binary.Write(buf, le, p.ID)
	binary.Write(buf, le, uint16(len(p.Name)))
	binary.Write(buf, le, []byte(p.Name))
	binary.Write(buf, le, uint16(len(p.Func)))
	binary.Write(buf, le, []byte(p.Func))

	binary.Write(buf, le, uint16(len(p.Params)))
	for _, par := range p.Params {
		binary.Write(buf, le, uint16(len(par)))
		binary.Write(buf, le, []byte(par))
	}

	data = buf.Bytes()
	return
}

// UnmarshalBinary decode binary buffer into Plugin receiver data.
func (p *Plugin) UnmarshalBinary(data []byte) (err error) {
	if len(data) == 0 {
		p.Empty()
		return
	}
	buf := bytes.NewReader(data)
	le := binary.LittleEndian
	ReadData := func(r io.Reader, order binary.ByteOrder, dataLen uint16) (data []byte) {
		data = make([]byte, dataLen)
		binary.Read(r, order, &data)
		return
	}
	ReadString := func(r io.Reader, order binary.ByteOrder) (str string) {
		var strLen uint16
		binary.Read(r, order, &strLen)
		str = string(ReadData(r, order, strLen))
		return
	}

	binary.Read(buf, le, &p.ID)
	p.Name = ReadString(buf, le)
	p.Func = ReadString(buf, le)
	var numP uint16
	binary.Read(buf, le, &numP)
	for i := 0; i < int(numP); i++ {
		p.Params = append(p.Params, ReadString(buf, le))
	}
	binary.Read(buf, le, &p.RequestInJSON)
	return
}
