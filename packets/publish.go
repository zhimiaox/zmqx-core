package packets

import (
	"bytes"
	"fmt"
	"io"

	"github.com/zhimiaox/zmqx-core/consts"
	"github.com/zhimiaox/zmqx-core/errors"
)

// Publish represents the MQTT Publish  packet
type Publish struct {
	Version   Version
	FixHeader *FixHeader
	Dup       bool
	Qos       uint8
	Retain    bool
	TopicName []byte
	PacketID
	Payload    []byte
	Properties *Properties
}

func (p *Publish) String() string {
	return fmt.Sprintf("Publish, Version: %v, Pid: %v, Dup: %v, Qos: %v, Retain: %v, TopicName: %s, Payload: %s, properties: %s",
		p.Version, p.PacketID, p.Dup, p.Qos, p.Retain, p.TopicName, p.Payload, p.Properties)
}

// NewPublishPacket returns a Publish instance by the given FixHeader and io.Reader.
func NewPublishPacket(fh *FixHeader, version Version, r io.Reader) (*Publish, error) {
	p := &Publish{FixHeader: fh, Version: version}
	p.Dup = (1 & (fh.Flags >> 3)) > 0
	p.Qos = (fh.Flags >> 1) & 3
	// [MQTT-3.3.1-2]、 [MQTT-4.3.1-1]
	if p.Qos == 0 && p.Dup {
		return nil, errors.ErrMalformed
	}
	if p.Qos > Qos2 {
		return nil, errors.ErrMalformed
	}
	// retain mqtt message flag
	if fh.Flags&1 == 1 {
		p.Retain = true
	}
	err := p.Unpack(r)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// Pack encodes the packet struct into bytes and writes it into io.Writer.
func (p *Publish) Pack(w io.Writer) error {
	p.FixHeader = &FixHeader{PacketType: PUBLISH}
	bufWriter := &bytes.Buffer{}
	var dup, retain byte
	dup = 0
	retain = 0
	if p.Dup {
		dup = 8
	}
	if p.Retain {
		retain = 1
	}
	p.FixHeader.Flags = dup | retain | (p.Qos << 1)
	writeBinary(bufWriter, p.TopicName)
	if p.Qos == Qos1 || p.Qos == Qos2 {
		writeUint16(bufWriter, p.PacketID)
	}
	if IsVersion5(p.Version) {
		if err := p.Properties.Pack(bufWriter, PUBLISH); err != nil {
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
func (p *Publish) Unpack(r io.Reader) error {
	var err error
	restBuffer := make([]byte, p.FixHeader.RemainLength)
	_, err = io.ReadFull(r, restBuffer)
	if err != nil {
		return errors.ErrMalformed
	}
	bufReader := bytes.NewBuffer(restBuffer)
	p.TopicName, err = readUTF8String(true, bufReader)
	if err != nil {
		return err
	}
	if !ValidTopicName(true, p.TopicName) {
		return errors.ErrMalformed
	}
	if p.Qos > Qos0 {
		p.PacketID, err = readUint16(bufReader)
		if err != nil {
			return err
		}
	}
	if IsVersion5(p.Version) {
		p.Properties = &Properties{}
		if err := p.Properties.Unpack(bufReader, PUBLISH); err != nil {
			return err
		}
	}
	p.Payload = bufReader.Next(bufReader.Len())
	return nil
}

// NewPuback returns the puback struct related to the publish struct in QoS 1
func (p *Publish) NewPuback(code consts.Code, ppt *Properties) *Puback {
	pub := &Puback{
		Version:    p.Version,
		Code:       code,
		PacketID:   p.PacketID,
		Properties: ppt,
	}
	return pub
}

// NewPubrec returns the pubrec struct related to the publish struct in QoS 2
func (p *Publish) NewPubrec(code consts.Code, ppt *Properties) *Pubrec {
	pub := &Pubrec{
		Version:    p.Version,
		Code:       code,
		PacketID:   p.PacketID,
		Properties: ppt,
	}
	return pub
}
