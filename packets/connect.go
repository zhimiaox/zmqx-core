package packets

import (
	"bytes"
	"fmt"
	"io"

	"github.com/zhimiaox/zmqx-core/consts"
	"github.com/zhimiaox/zmqx-core/errors"
)

// Connect represents the MQTT Connect  packet
type Connect struct {
	Version   Version
	FixHeader *FixHeader
	// Variable header
	ProtocolLevel byte
	// Connect Flags
	UsernameFlag bool
	ProtocolName []byte
	PasswordFlag bool
	WillRetain   bool
	WillQos      uint8
	WillFlag     bool
	WillTopic    []byte
	WillMsg      []byte
	CleanStart   bool
	KeepAlive    uint16 // 如果非零，1.5倍时间没收到则断开连接[MQTT-3.1.2-24]
	// if set
	ClientID []byte
	Username []byte
	Password []byte

	Properties     *Properties
	WillProperties *Properties
}

func (c *Connect) String() string {
	return fmt.Sprintf("Connect, Version: %v,"+"ProtocolLevel: %v, UsernameFlag: %v, PasswordFlag: %v, ProtocolName: %s, CleanStart: %v, KeepAlive: %v, ClientID: %s, Username: %s, Password: %s"+
		", WillFlag: %v, WillRetain: %v, WillQos: %v, WillMsg: %s, properties: %s, WillProperties: %s",
		c.Version, c.ProtocolLevel, c.UsernameFlag, c.PasswordFlag, c.ProtocolName, c.CleanStart, c.KeepAlive, c.ClientID, c.Username, c.Password, c.WillFlag, c.WillRetain, c.WillQos, c.WillMsg, c.Properties, c.WillProperties)
}

// Pack encodes the packet struct into bytes and writes it into io.Writer.
func (c *Connect) Pack(w io.Writer) error {
	var err error
	c.FixHeader = &FixHeader{PacketType: CONNECT, Flags: FlagReserved}

	bufWriter := &bytes.Buffer{}
	bufWriter.Write([]byte{0x00, 0x04})
	bufWriter.Write(c.ProtocolName)
	bufWriter.WriteByte(c.ProtocolLevel)
	// write flag
	var (
		usernameFlag = 0
		passwordFlag = 0
		willRetain   = 0
		willFlag     = 0
		willQos      = 0
		CleanStart   = 0
		reserved     = 0
	)
	if c.UsernameFlag {
		usernameFlag = 128
	}
	if c.PasswordFlag {
		passwordFlag = 64
	}
	if c.WillRetain {
		willRetain = 32
	}
	if c.WillQos == 1 {
		willQos = 8
	} else if c.WillQos == 2 {
		willQos = 16
	}
	if c.WillFlag {
		willFlag = 4
	}
	if c.CleanStart {
		CleanStart = 2
	}
	connFlag := usernameFlag | passwordFlag | willRetain | willFlag | willQos | CleanStart | reserved
	bufWriter.Write([]byte{uint8(connFlag)})
	writeUint16(bufWriter, c.KeepAlive)

	if IsVersion5(c.Version) {
		if err = c.Properties.Pack(bufWriter, CONNECT); err != nil {
			return err
		}
	}
	clientIDByte, _, err := EncodeUTF8String(c.ClientID)

	if err != nil {
		return err
	}
	bufWriter.Write(clientIDByte)

	if c.WillFlag {
		if IsVersion5(c.Version) {
			if err = c.WillProperties.PackWillProperties(bufWriter); err != nil {
				return err
			}
		}
		willTopicByte, _, err := EncodeUTF8String(c.WillTopic)
		if err != nil {
			return err
		}
		bufWriter.Write(willTopicByte)
		willMsgByte, _, err := EncodeUTF8String(c.WillMsg)
		if err != nil {
			return err
		}
		bufWriter.Write(willMsgByte)
	}
	if c.UsernameFlag {
		usernameByte, _, err := EncodeUTF8String(c.Username)
		if err != nil {
			return err
		}
		bufWriter.Write(usernameByte)
	}
	if c.PasswordFlag {
		passwordByte, _, err := EncodeUTF8String(c.Password)
		if err != nil {
			return err
		}
		bufWriter.Write(passwordByte)
	}

	c.FixHeader.RemainLength = bufWriter.Len()
	err = c.FixHeader.Pack(w)
	if err != nil {
		return err
	}
	_, err = bufWriter.WriteTo(w)
	return err
}

// Unpack read the packet bytes from io.Reader and decodes it into the packet struct.
func (c *Connect) Unpack(r io.Reader) (err error) {
	restBuffer := make([]byte, c.FixHeader.RemainLength)
	_, err = io.ReadFull(r, restBuffer)
	if err != nil {
		return err
	}
	bufReader := bytes.NewBuffer(restBuffer)
	c.ProtocolName, err = readUTF8String(false, bufReader)
	if err != nil {
		return err
	}
	c.ProtocolLevel, err = bufReader.ReadByte()
	if err != nil {
		return errors.ErrMalformed
	}
	c.Version = c.ProtocolLevel
	if name, ok := version2protoName[c.ProtocolLevel]; !ok {
		return errors.NewError(consts.V3UnacceptableProtocolVersion)
	} else if !bytes.Equal(c.ProtocolName, name) {
		return errors.NewError(consts.UnsupportedProtocolVersion)
	}

	connectFlags, err := bufReader.ReadByte()
	if err != nil {
		return errors.ErrMalformed
	}
	reserved := 1 & connectFlags
	// [MQTT-3.1.2-3]
	if reserved != 0 {
		return errors.ErrMalformed
	}
	c.CleanStart = (1 & (connectFlags >> 1)) > 0
	c.WillFlag = (1 & (connectFlags >> 2)) > 0
	c.WillQos = 3 & (connectFlags >> 3)
	// [MQTT-3.1.2-11]
	if !c.WillFlag && c.WillQos != 0 {
		return errors.ErrMalformed
	}
	c.WillRetain = (1 & (connectFlags >> 5)) > 0
	// [MQTT-3.1.2-11]
	if !c.WillFlag && c.WillRetain {
		return errors.ErrMalformed
	}
	c.PasswordFlag = (1 & (connectFlags >> 6)) > 0
	c.UsernameFlag = (1 & (connectFlags >> 7)) > 0
	c.KeepAlive, err = readUint16(bufReader)
	if err != nil {
		return errors.ErrMalformed
	}

	if IsVersion5(c.Version) {
		// resolve properties
		c.Properties = new(Properties)
		c.WillProperties = new(Properties)
		if err := c.Properties.Unpack(bufReader, CONNECT); err != nil {
			return err
		}
	}
	return c.unpackPayload(bufReader)
}

func (c *Connect) unpackPayload(bufReader *bytes.Buffer) error {
	var err error
	c.ClientID, err = readUTF8String(true, bufReader)
	if err != nil {
		return err
	}

	// v311 [MQTT-3.1.3-7]
	if IsVersion3X(c.Version) && len(c.ClientID) == 0 && !c.CleanStart {
		// v311 [MQTT-3.1.3-8]
		return errors.NewError(consts.V3IdentifierRejected)
	}

	if c.WillFlag {
		if IsVersion5(c.Version) {
			err := c.WillProperties.UnpackWillProperties(bufReader)
			if err != nil {
				return err
			}
		}
		c.WillTopic, err = readUTF8String(true, bufReader)
		if err != nil {
			return err
		}
		c.WillMsg, err = readUTF8String(false, bufReader)
		if err != nil {
			return err
		}
	}

	if c.UsernameFlag {
		c.Username, err = readUTF8String(true, bufReader)
		if err != nil {
			return err
		}
	}
	if c.PasswordFlag {
		c.Password, err = readUTF8String(true, bufReader)
		if err != nil {
			return err
		}
	}
	return nil
}

// NewConnectPacket returns a Connect instance by the given FixHeader and io.Reader
func NewConnectPacket(fh *FixHeader, version Version, r io.Reader) (*Connect, error) {
	// b1 := buffer[0], must be 16
	p := &Connect{FixHeader: fh, Version: version}
	// flag bit legality check [MQTT-2.2.2-2]
	if fh.Flags != FlagReserved {
		return nil, errors.ErrMalformed
	}
	err := p.Unpack(r)
	if err != nil {
		return nil, err
	}
	return p, err
}

// NewConnackPacket returns the Connack struct which is the ack packet of the Connect packet.
func (c *Connect) NewConnackPacket(code consts.Code) *Connack {
	return &Connack{Code: code, Version: c.Version}
}
