package packets

import (
	"bytes"
	"fmt"
	"io"

	"github.com/zhimiaox/zmqx-core/consts"
	"github.com/zhimiaox/zmqx-core/errors"
)

// Pubcomp represents the MQTT Pubcomp  packet
type Pubcomp struct {
	Version    Version
	FixHeader  *FixHeader
	PacketID   PacketID
	Code       byte
	Properties *Properties
}

func (p *Pubcomp) String() string {
	return fmt.Sprintf("Pubcomp, Version: %v, Pid: %v, properties: %s", p.Version, p.PacketID, p.Properties)
}

// NewPubcompPacket returns a Pubcomp instance by the given FixHeader and io.Reader
func NewPubcompPacket(fh *FixHeader, version Version, r io.Reader) (*Pubcomp, error) {
	p := &Pubcomp{FixHeader: fh, Version: version}
	err := p.Unpack(r)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// Pack encodes the packet struct into bytes and writes it into io.Writer.
func (p *Pubcomp) Pack(w io.Writer) error {
	p.FixHeader = &FixHeader{PacketType: PUBCOMP, Flags: FlagReserved}
	bufWriter := &bytes.Buffer{}
	writeUint16(bufWriter, p.PacketID)
	if IsVersion5(p.Version) && (p.Code != consts.Success || p.Properties != nil) {
		bufWriter.WriteByte(p.Code)
		if err := p.Properties.Pack(bufWriter, PUBCOMP); err != nil {
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
func (p *Pubcomp) Unpack(r io.Reader) error {
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
		if !ValidateCode(PUBCOMP, p.Code) {
			return errors.ErrProtocol
		}
		return p.Properties.Unpack(bufReader, PUBCOMP)
	}
	return nil
}
