package packets

import (
	"bytes"
	"fmt"
	"io"

	"github.com/zhimiaox/zmqx-core/consts"
	"github.com/zhimiaox/zmqx-core/errors"
)

// Unsuback represents the MQTT Unsuback  packet.
type Unsuback struct {
	Version    Version
	FixHeader  *FixHeader
	PacketID   PacketID
	Properties *Properties
	Payload    []consts.Code
}

func (p *Unsuback) String() string {
	return fmt.Sprintf("Unsuback, Version: %v, Pid: %v, Payload: %v, properties: %s", p.Version, p.PacketID, p.Payload, p.Properties)
}

// Pack encodes the packet struct into bytes and writes it into io.Writer.
func (p *Unsuback) Pack(w io.Writer) error {
	p.FixHeader = &FixHeader{PacketType: UNSUBACK, Flags: FlagReserved}
	bufWriter := &bytes.Buffer{}
	writeUint16(bufWriter, p.PacketID)
	if IsVersion5(p.Version) {
		if err := p.Properties.Pack(bufWriter, UNSUBACK); err != nil {
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
func (p *Unsuback) Unpack(r io.Reader) error {
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
	if IsVersion3X(p.Version) {
		return nil
	}

	p.Properties = &Properties{}
	err = p.Properties.Unpack(bufReader, UNSUBACK)
	if err != nil {
		return err
	}
	for {
		b, err := bufReader.ReadByte()
		if err != nil {
			return errors.ErrMalformed
		}
		if IsVersion5(p.Version) && !ValidateCode(UNSUBACK, b) {
			return errors.ErrProtocol
		}
		p.Payload = append(p.Payload, b)
		if bufReader.Len() == 0 {
			return nil
		}
	}
}

// NewUnsubackPacket returns a Unsuback instance by the given FixHeader and io.Reader.
func NewUnsubackPacket(fh *FixHeader, version Version, r io.Reader) (*Unsuback, error) {
	p := &Unsuback{FixHeader: fh, Version: version}
	if fh.Flags != FlagReserved {
		return nil, errors.ErrMalformed
	}
	if err := p.Unpack(r); err != nil {
		return nil, err
	}
	return p, nil
}
