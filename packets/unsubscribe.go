package packets

import (
	"bytes"
	"fmt"
	"io"

	"github.com/zhimiaox/zmqx-core/consts"
	"github.com/zhimiaox/zmqx-core/errors"
)

// Unsubscribe represents the MQTT Unsubscribe  packet.
type Unsubscribe struct {
	Version    Version
	FixHeader  *FixHeader
	PacketID   PacketID
	Topics     []string
	Properties *Properties
}

func (u *Unsubscribe) String() string {
	return fmt.Sprintf("Unsubscribe, Version: %v, Pid: %v, Topics: %v, properties: %s", u.Version, u.PacketID, u.Topics, u.Properties)
}

// NewUnSubBack returns the Unsuback struct which is the ack packet of the Unsubscribe packet.
func (u *Unsubscribe) NewUnSubBack() *Unsuback {
	fh := &FixHeader{PacketType: UNSUBACK, Flags: 0}
	unSuback := &Unsuback{FixHeader: fh, PacketID: u.PacketID, Version: u.Version}
	if IsVersion5(unSuback.Version) {
		unSuback.Payload = make([]consts.Code, len(u.Topics))
	}
	return unSuback
}

// NewUnsubscribePacket returns a Unsubscribe instance by the given FixHeader and io.Reader.
func NewUnsubscribePacket(fh *FixHeader, version Version, r io.Reader) (*Unsubscribe, error) {
	p := &Unsubscribe{FixHeader: fh, Version: version}
	// 判断 标志位 flags 是否合法[MQTT-3.10.1-1]
	if fh.Flags != FlagUnsubscribe {
		return nil, errors.ErrMalformed
	}
	if err := p.Unpack(r); err != nil {
		return nil, err
	}
	return p, nil
}

// Pack encodes the packet struct into bytes and writes it into io.Writer.
func (u *Unsubscribe) Pack(w io.Writer) error {
	u.FixHeader = &FixHeader{PacketType: UNSUBSCRIBE, Flags: FlagUnsubscribe}
	bufWriter := &bytes.Buffer{}
	writeUint16(bufWriter, u.PacketID)
	if IsVersion5(u.Version) {
		if err := u.Properties.Pack(bufWriter, UNSUBSCRIBE); err != nil {
			return err
		}
	}
	for _, topic := range u.Topics {
		writeUTF8String(bufWriter, []byte(topic))
	}
	u.FixHeader.RemainLength = bufWriter.Len()
	err := u.FixHeader.Pack(w)
	if err != nil {
		return err
	}
	_, err = bufWriter.WriteTo(w)
	return err
}

// Unpack read the packet bytes from io.Reader and decodes it into the packet struct.
func (u *Unsubscribe) Unpack(r io.Reader) error {
	restBuffer := make([]byte, u.FixHeader.RemainLength)
	_, err := io.ReadFull(r, restBuffer)
	if err != nil {
		return errors.ErrMalformed
	}
	bufReader := bytes.NewBuffer(restBuffer)
	u.PacketID, err = readUint16(bufReader)
	if err != nil {
		return err
	}

	if IsVersion5(u.Version) {
		u.Properties = &Properties{}
		if err := u.Properties.Unpack(bufReader, UNSUBSCRIBE); err != nil {
			return err
		}
	}
	for {
		topicFilter, err := readUTF8String(true, bufReader)
		if err != nil {
			return err
		}
		if !ValidTopicFilter(true, topicFilter) {
			return errors.ErrProtocol
		}
		u.Topics = append(u.Topics, string(topicFilter))
		if bufReader.Len() == 0 {
			return nil
		}
	}
}
