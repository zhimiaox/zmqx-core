package common

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
)

func WriteUint16(w *bytes.Buffer, i uint16) {
	w.WriteByte(byte(i >> 8))
	w.WriteByte(byte(i))
}

func WriteBool(w *bytes.Buffer, b bool) {
	if b {
		w.WriteByte(1)
	} else {
		w.WriteByte(0)
	}
}

func ReadBool(r *bytes.Buffer) (bool, error) {
	b, err := r.ReadByte()
	if err != nil {
		return false, err
	}
	if b == 0 {
		return false, nil
	}
	return true, nil
}

func WriteString(w *bytes.Buffer, s string) {
	WriteUint16(w, uint16(len(s)))
	w.WriteString(s)
}

func ReadString(r *bytes.Buffer) (b string, err error) {
	stringBytes, err := ReadBytes(r)
	if err != nil {
		return "", err
	}
	return string(stringBytes), nil
}

func WriteBytes(w *bytes.Buffer, s []byte) {
	WriteUint16(w, uint16(len(s)))
	w.Write(s)
}

func ReadBytes(r *bytes.Buffer) (b []byte, err error) {
	l := make([]byte, 2)
	if _, err = io.ReadFull(r, l); err != nil {
		return nil, err
	}
	payload := make([]byte, int(binary.BigEndian.Uint16(l)))
	if _, err = io.ReadFull(r, payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func WriteUint32(w *bytes.Buffer, i uint32) {
	w.WriteByte(byte(i >> 24))
	w.WriteByte(byte(i >> 16))
	w.WriteByte(byte(i >> 8))
	w.WriteByte(byte(i))
}

func ReadUint16(r *bytes.Buffer) (uint16, error) {
	if r.Len() < 2 {
		return 0, errors.New("invalid length")
	}
	return binary.BigEndian.Uint16(r.Next(2)), nil
}

func ReadUint32(r *bytes.Buffer) (uint32, error) {
	if r.Len() < 4 {
		return 0, errors.New("invalid length")
	}
	return binary.BigEndian.Uint32(r.Next(4)), nil
}
