package packets

import (
	"bytes"
	"fmt"
	"io"

	"github.com/zhimiaox/zmqx-core/errors"
)

// Subscribe represents the MQTT Subscribe  packet.
type Subscribe struct {
	Version    Version
	FixHeader  *FixHeader
	PacketID   PacketID
	Topics     []Topic // suback响应之前填充
	Properties *Properties
}

func (p *Subscribe) String() string {
	str := fmt.Sprintf("Subscribe, Version: %v, Pid: %v", p.Version, p.PacketID)
	for k, t := range p.Topics {
		str += fmt.Sprintf(", Topic[%d][Name: %s, Qos: %v]", k, t.Name, t.Qos)
	}
	str += fmt.Sprintf(", properties: %s", p.Properties)
	return str
}

// NewSuback returns the Suback struct which is the ack packet of the Subscribe packet.
func (p *Subscribe) NewSuback() *Suback {
	fh := &FixHeader{PacketType: SUBACK, Flags: FlagReserved}
	suback := &Suback{FixHeader: fh, Version: p.Version, Payload: make([]byte, 0, len(p.Topics))}
	suback.PacketID = p.PacketID
	var qos byte
	for _, v := range p.Topics {
		qos = v.Qos
		suback.Payload = append(suback.Payload, qos)
	}
	return suback
}

// NewSubscribePacket returns a Subscribe instance by the given FixHeader and io.Reader.
func NewSubscribePacket(fh *FixHeader, version Version, r io.Reader) (*Subscribe, error) {
	p := &Subscribe{FixHeader: fh, Version: version}
	// flag bit legality check[MQTT-3.8.1-1]
	if fh.Flags != FlagSubscribe {
		return nil, errors.ErrMalformed
	}
	err := p.Unpack(r)
	if err != nil {
		return nil, err
	}
	return p, err
}

// Pack encodes the packet struct into bytes and writes it into io.Writer.
func (p *Subscribe) Pack(w io.Writer) error {
	p.FixHeader = &FixHeader{PacketType: SUBSCRIBE, Flags: FlagSubscribe}
	bufWriter := &bytes.Buffer{}
	writeUint16(bufWriter, p.PacketID)
	var nl, rap byte
	if IsVersion5(p.Version) {
		if err := p.Properties.Pack(bufWriter, SUBSCRIBE); err != nil {
			return err
		}
		for _, v := range p.Topics {
			writeUTF8String(bufWriter, []byte(v.Name))
			if v.NoLocal {
				nl = 4
			} else {
				nl = 0
			}
			if v.RetainAsPublished {
				rap = 8
			} else {
				rap = 0
			}
			bufWriter.WriteByte(v.Qos | nl | rap | (v.RetainHandling << 4))
		}
	} else {
		for _, t := range p.Topics {
			writeUTF8String(bufWriter, []byte(t.Name))
			bufWriter.WriteByte(t.Qos)
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
func (p *Subscribe) Unpack(r io.Reader) (err error) {
	restBuffer := make([]byte, p.FixHeader.RemainLength)
	_, err = io.ReadFull(r, restBuffer)
	if err != nil {
		return errors.ErrMalformed
	}
	bufReader := bytes.NewBuffer(restBuffer)
	p.PacketID, err = readUint16(bufReader)
	if err != nil {
		return err
	}
	if IsVersion5(p.Version) {
		p.Properties = &Properties{}
		if err := p.Properties.Unpack(bufReader, SUBSCRIBE); err != nil {
			return err
		}
	}
	for {
		topicFilter, err := readUTF8String(true, bufReader)
		if err != nil {
			return err
		}
		if IsVersion5(p.Version) {
			// check shared subscription syntax
			if !ValidV5Topic(topicFilter) {
				return errors.ErrMalformed
			}
		} else {
			if !ValidTopicFilter(true, topicFilter) {
				return errors.ErrMalformed
			}
		}
		opts, err := bufReader.ReadByte()
		if err != nil {
			return errors.ErrMalformed
		}
		topic := Topic{
			Name: string(topicFilter),
		}
		if IsVersion5(p.Version) {
			topic.Qos = opts & 3
			topic.NoLocal = (1 & (opts >> 2)) > 0
			topic.RetainAsPublished = (1 & (opts >> 3)) > 0
			topic.RetainHandling = 3 & (opts >> 4)
		} else {
			topic.Qos = opts
			if topic.Qos > Qos2 {
				return errors.ErrProtocol
			}
		}

		// check reserved
		if 3&(opts>>6) != 0 {
			return errors.ErrProtocol
		}
		if topic.Qos > Qos2 {
			return errors.ErrProtocol
		}
		p.Topics = append(p.Topics, topic)
		if bufReader.Len() == 0 {
			return nil
		}
	}
}
