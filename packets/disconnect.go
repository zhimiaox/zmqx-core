package packets

import (
	"bytes"
	"fmt"
	"io"

	"github.com/zhimiaox/zmqx-core/consts"
	"github.com/zhimiaox/zmqx-core/errors"
)

// Disconnect represents the MQTT Disconnect  packet
type Disconnect struct {
	Version   Version
	FixHeader *FixHeader
	// V5
	Code       consts.Code
	Properties *Properties
}

func (d *Disconnect) String() string {
	return fmt.Sprintf("Disconnect, Version: %v, Code: %v, properties: %s", d.Version, d.Code, d.Properties)
}

// Pack encodes the packet struct into bytes and writes it into io.Writer.
func (d *Disconnect) Pack(w io.Writer) error {
	var err error
	d.FixHeader = &FixHeader{PacketType: DISCONNECT, Flags: FlagReserved}
	if IsVersion3X(d.Version) {
		d.FixHeader.RemainLength = 0
		return d.FixHeader.Pack(w)
	}
	bufWriter := &bytes.Buffer{}
	if d.Code != consts.Success || d.Properties != nil {
		bufWriter.WriteByte(d.Code)
		if err = d.Properties.Pack(bufWriter, DISCONNECT); err != nil {
			return err
		}
	}
	d.FixHeader.RemainLength = bufWriter.Len()
	err = d.FixHeader.Pack(w)
	if err != nil {
		return err
	}
	_, err = bufWriter.WriteTo(w)
	return err
}

// Unpack read the packet bytes from io.Reader and decodes it into the packet struct.
func (d *Disconnect) Unpack(r io.Reader) error {
	restBuffer := make([]byte, d.FixHeader.RemainLength)
	_, err := io.ReadFull(r, restBuffer)
	if err != nil {
		return errors.ErrMalformed
	}
	if IsVersion5(d.Version) {
		d.Properties = &Properties{}
		bufReader := bytes.NewBuffer(restBuffer)
		if d.FixHeader.RemainLength == 0 {
			d.Code = consts.Success
			return nil
		}
		d.Code, err = bufReader.ReadByte()
		if err != nil {
			return errors.ErrMalformed
		}
		if !ValidateCode(DISCONNECT, d.Code) {
			return errors.ErrProtocol
		}
		return d.Properties.Unpack(bufReader, DISCONNECT)
	}
	return nil
}

// NewDisConnectPackets returns a Disconnect instance by the given FixHeader and io.Reader
func NewDisConnectPackets(fh *FixHeader, version Version, r io.Reader) (*Disconnect, error) {
	if fh.Flags != 0 {
		return nil, errors.ErrMalformed
	}
	p := &Disconnect{FixHeader: fh, Version: version}
	err := p.Unpack(r)
	if err != nil {
		return nil, err
	}
	return p, nil
}
