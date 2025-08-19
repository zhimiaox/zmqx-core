package packets

import (
	"bytes"
	"fmt"
	"io"

	"github.com/zhimiaox/zmqx-core/consts"
	"github.com/zhimiaox/zmqx-core/errors"
)

// Suback represents the MQTT Suback  packet.
type Suback struct {
	Version    Version
	FixHeader  *FixHeader
	PacketID   PacketID
	Payload    []consts.Code
	Properties *Properties
}

func (p *Suback) String() string {
	return fmt.Sprintf("Suback,Version: %v, Pid: %v, Payload: %v, properties: %s", p.Version, p.PacketID, p.Payload, p.Properties)
}

// NewSubackPacket returns a Suback instance by the given FixHeader and io.Reader.
func NewSubackPacket(fh *FixHeader, version Version, r io.Reader) (*Suback, error) {
	p := &Suback{FixHeader: fh, Version: version}
	// flag bit legality check [MQTT-3.8.1-1]
	if fh.Flags != FlagReserved {
		return nil, errors.ErrMalformed
	}
	err := p.Unpack(r)
	return p, err
}

// Pack encodes the packet struct into bytes and writes it into io.Writer.
func (p *Suback) Pack(w io.Writer) error {
	p.FixHeader = &FixHeader{PacketType: SUBACK, Flags: FlagReserved}
	bufWriter := &bytes.Buffer{}
	writeUint16(bufWriter, p.PacketID)
	if IsVersion5(p.Version) {
		if err := p.Properties.Pack(bufWriter, SUBACK); err != nil {
			return err
		}
	}
	bufWriter.Write(p.Payload)
	p.FixHeader.RemainLength = bufWriter.Len()
	err := p.FixHeader.Pack(w)
	if err != nil {
		return err
	}
	_, err = bufWriter.WriteTo(w)
	return err

}

// Unpack read the packet bytes from io.Reader and decodes it into the packet struct.
func (p *Suback) Unpack(r io.Reader) error {
	restBuffer := make([]byte, p.FixHeader.RemainLength)
	_, err := io.ReadFull(r, restBuffer)
	if err != nil {
		return errors.ErrMalformed
	}
	bufReader := bytes.NewBuffer(restBuffer)

	p.PacketID, err = readUint16(bufReader)
	if err != nil {
		return errors.ErrMalformed
	}
	if IsVersion5(p.Version) {
		p.Properties = &Properties{}
		err = p.Properties.Unpack(bufReader, SUBACK)
		if err != nil {
			return err
		}
	}
	for {
		b, err := bufReader.ReadByte()
		if err != nil {
			return errors.ErrMalformed
		}
		if !ValidateCode(SUBACK, b) {
			return errors.ErrProtocol
		}
		p.Payload = append(p.Payload, b)
		if bufReader.Len() == 0 {
			return nil
		}
	}
}
