package packets

import (
	"bytes"
	"fmt"
	"io"

	"github.com/zhimiaox/zmqx-core/consts"
	"github.com/zhimiaox/zmqx-core/errors"
)

type Auth struct {
	FixHeader  *FixHeader
	Code       byte
	Properties *Properties
}

func (a *Auth) String() string {
	return fmt.Sprintf("Auth, Code: %v, properties: %s", a.Code, a.Properties)
}

func (a *Auth) Pack(w io.Writer) error {
	a.FixHeader = &FixHeader{PacketType: AUTH, Flags: FlagReserved}
	bufWriter := &bytes.Buffer{}
	if a.Code != consts.Success || a.Properties != nil {
		bufWriter.WriteByte(a.Code)
		if err := a.Properties.Pack(bufWriter, AUTH); err != nil {
			return err
		}
	}
	a.FixHeader.RemainLength = bufWriter.Len()
	err := a.FixHeader.Pack(w)
	if err != nil {
		return err
	}
	_, err = bufWriter.WriteTo(w)
	return err
}

func (a *Auth) Unpack(r io.Reader) error {
	if a.FixHeader.RemainLength == 0 {
		a.Code = consts.Success
		return nil
	}
	restBuffer := make([]byte, a.FixHeader.RemainLength)
	_, err := io.ReadFull(r, restBuffer)
	if err != nil {
		return errors.ErrMalformed
	}
	bufReader := bytes.NewBuffer(restBuffer)
	a.Code, err = bufReader.ReadByte()
	if err != nil {
		return errors.ErrMalformed
	}
	if !ValidateCode(AUTH, a.Code) {
		return errors.ErrProtocol
	}
	a.Properties = &Properties{}
	return a.Properties.Unpack(bufReader, AUTH)
}

func NewAuthPacket(fh *FixHeader, r io.Reader) (*Auth, error) {
	p := &Auth{FixHeader: fh}
	// 判断 标志位 flags 是否合法[MQTT-2.2.2-2]
	if fh.Flags != FlagReserved {
		return nil, errors.ErrMalformed
	}
	err := p.Unpack(r)
	if err != nil {
		return nil, err
	}
	return p, err
}
