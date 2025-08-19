package packets

import (
	"bytes"
	"fmt"
	"io"

	"github.com/zhimiaox/zmqx-core/consts"
	"github.com/zhimiaox/zmqx-core/errors"
)

// Puback represents the MQTT Puback  packet
type Puback struct {
	Version   Version
	FixHeader *FixHeader
	PacketID  PacketID
	// V5
	Code       consts.Code
	Properties *Properties
}

func (p *Puback) String() string {
	return fmt.Sprintf("Puback, Version: %v, Pid: %v, properties: %s", p.Version, p.PacketID, p.Properties)
}

// NewPubackPacket returns a Puback instance by the given FixHeader and io.Reader
func NewPubackPacket(fh *FixHeader, version Version, r io.Reader) (*Puback, error) {
	p := &Puback{FixHeader: fh, Version: version}
	err := p.Unpack(r)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// Pack encodes the packet struct into bytes and writes it into io.Writer.
func (p *Puback) Pack(w io.Writer) error {
	p.FixHeader = &FixHeader{PacketType: PUBACK, Flags: FlagReserved}
	bufWriter := &bytes.Buffer{}
	writeUint16(bufWriter, p.PacketID)
	if IsVersion5(p.Version) && (p.Code != consts.Success || p.Properties != nil) {
		bufWriter.WriteByte(p.Code)
		if err := p.Properties.Pack(bufWriter, PUBACK); err != nil {
			return err
		}
	}
	p.FixHeader.RemainLength = bufWriter.Len()
	err := p.FixHeader.Pack(w)
	if err != nil {
		return err
	}
	_, err = bufWriter.WriteTo(w)
	return err
}

// Unpack read the packet bytes from io.Reader and decodes it into the packet struct.
func (p *Puback) Unpack(r io.Reader) error {
	restBuffer := make([]byte, p.FixHeader.RemainLength)
	_, err := io.ReadFull(r, restBuffer)
	if err != nil {
		return errors.ErrMalformed
	}
	bufReader := bytes.NewBuffer(restBuffer)

	p.PacketID, err = readUint16(bufReader)
	if err != nil {
		return err
	}
	if p.FixHeader.RemainLength == 2 {
		p.Code = consts.Success
		return nil
	}

	if IsVersion5(p.Version) {
		p.Properties = &Properties{}
		if p.Code, err = bufReader.ReadByte(); err != nil {
			return errors.ErrMalformed
		}
		if !ValidateCode(PUBACK, p.Code) {
			return errors.ErrProtocol
		}
		if err := p.Properties.Unpack(bufReader, PUBACK); err != nil {
			return err
		}
	}
	return nil
}
