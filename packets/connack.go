package packets

import (
	"bytes"
	"fmt"
	"io"

	"github.com/zhimiaox/zmqx-core/consts"
	"github.com/zhimiaox/zmqx-core/errors"
)

// Connack represents the MQTT Connack  packet
type Connack struct {
	Version        Version
	FixHeader      *FixHeader
	Code           consts.Code
	SessionPresent bool
	Properties     *Properties
}

func (c *Connack) String() string {
	return fmt.Sprintf("Connack, Version: %v, Code:%v, SessionPresent:%v, properties: %s", c.Version, c.Code, c.SessionPresent, c.Properties)
}

// Pack encodes the packet struct into bytes and writes it into io.Writer.
func (c *Connack) Pack(w io.Writer) error {
	var err error
	c.FixHeader = &FixHeader{PacketType: CONNACK, Flags: FlagReserved}
	bufWriter := &bytes.Buffer{}
	if c.SessionPresent {
		bufWriter.WriteByte(1)
	} else {
		bufWriter.WriteByte(0)
	}
	bufWriter.WriteByte(c.Code)
	if IsVersion5(c.Version) {
		if err = c.Properties.Pack(bufWriter, CONNACK); err != nil {
			return err
		}
	}
	c.FixHeader.RemainLength = bufWriter.Len()
	err = c.FixHeader.Pack(w)
	if err != nil {
		return err
	}
	_, err = bufWriter.WriteTo(w)
	return err
}

// Unpack read the packet bytes from io.Reader and decodes it into the packet struct
func (c *Connack) Unpack(r io.Reader) error {
	restBuffer := make([]byte, c.FixHeader.RemainLength)
	_, err := io.ReadFull(r, restBuffer)
	if err != nil {
		return errors.ErrMalformed
	}
	bufReader := bytes.NewBuffer(restBuffer)
	sp, err := bufReader.ReadByte()
	if err != nil {
		return errors.ErrMalformed
	}
	if (127 & (sp >> 1)) > 0 {
		return errors.ErrMalformed
	}
	c.SessionPresent = sp == 1

	code, err := bufReader.ReadByte()
	if err != nil {
		return errors.ErrMalformed
	}

	c.Code = code
	if IsVersion5(c.Version) {
		if !ValidateCode(CONNACK, code) {
			return errors.ErrProtocol
		}
		c.Properties = &Properties{}
		return c.Properties.Unpack(bufReader, CONNACK)
	}
	return nil
}

// NewConnackPacket returns a Connack instance by the given FixHeader and io.Reader
func NewConnackPacket(fh *FixHeader, _ Version, r io.Reader) (*Connack, error) {
	p := &Connack{FixHeader: fh, Version: Version5}
	if fh.Flags != FlagReserved {
		return nil, errors.ErrMalformed
	}
	err := p.Unpack(r)
	if err != nil {
		return nil, err
	}
	return p, err
}
