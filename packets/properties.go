package packets

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"strings"

	"github.com/zhimiaox/zmqx-core/errors"
)

const (
	PropPayloadFormat          byte = 0x01
	PropMessageExpiry          byte = 0x02
	PropContentType            byte = 0x03
	PropResponseTopic          byte = 0x08
	PropCorrelationData        byte = 0x09
	PropSubscriptionIdentifier byte = 0x0B
	PropSessionExpiryInterval  byte = 0x11
	PropAssignedClientID       byte = 0x12
	PropServerKeepAlive        byte = 0x13
	PropAuthMethod             byte = 0x15
	PropAuthData               byte = 0x16
	PropRequestProblemInfo     byte = 0x17
	PropWillDelayInterval      byte = 0x18
	PropRequestResponseInfo    byte = 0x19
	PropResponseInfo           byte = 0x1A
	PropServerReference        byte = 0x1C
	PropReasonString           byte = 0x1F
	PropReceiveMaximum         byte = 0x21
	PropTopicAliasMaximum      byte = 0x22
	PropTopicAlias             byte = 0x23
	PropMaximumQoS             byte = 0x24
	PropRetainAvailable        byte = 0x25
	PropUser                   byte = 0x26
	PropMaximumPacketSize      byte = 0x27
	PropWildcardSubAvailable   byte = 0x28
	PropSubIDAvailable         byte = 0x29
	PropSharedSubAvailable     byte = 0x2A
)

func errMoreThanOnce(property byte) error {
	return fmt.Errorf("property %v presents more than once", property)
}

type UserProperty struct {
	K []byte
	V []byte
}

// Properties is a struct representing the all the described properties
// allowed by the MQTT protocol, determining the validity of a property
// relative to the packet type it was received in is provided by the
// ValidateID function
type Properties struct {
	// PayloadFormat indicates the format of the payload of the message
	// 0 is unspecified bytes
	// 1 is UTF8 encoded character data
	PayloadFormat *byte
	// MessageExpiry is the lifetime of the message in seconds
	MessageExpiry *uint32
	// ContentType is a UTF8 string describing the content of the message
	// for example it could be a MIME type
	ContentType []byte
	// ResponseTopic is a UTF8 string indicating the topic name to which any
	// response to this message should be sent
	ResponseTopic []byte
	// CorrelationData is binary data used to associate future response
	// messages with the original request message
	CorrelationData []byte
	// SubscriptionIdentifier is an identifier of the subscription to which
	// the Publish matched
	SubscriptionIdentifier []uint32
	// SessionExpiryInterval is the time in seconds after a client disconnects
	// that the server should retain the session Info (subscriptions etc)
	SessionExpiryInterval *uint32
	// AssignedClientID is the server assigned client identifier in the case
	// that a client connected without specifying a clientID the server
	// generates one and returns it in the Connack
	AssignedClientID []byte
	// ServerKeepAlive allows the server to specify in the Connack packet
	// the time in seconds to be used as the keep alive value
	ServerKeepAlive *uint16
	// AuthMethod is a UTF8 string containing the name of the authentication
	// method to be used for extended authentication
	AuthMethod []byte
	// AuthData is binary data containing authentication data
	AuthData []byte
	// RequestProblemInfo is used by the Client to indicate to the server to
	// include the Reason String and/or User Properties in case of failures
	RequestProblemInfo *byte
	// WillDelayInterval is the number of seconds the server waits after the
	// point at which it would otherwise send the will message before sending
	// it. The client reconnecting before that time expires causes the server
	// to cancel sending the will
	WillDelayInterval *uint32
	// RequestResponseInfo is used by the Client to request the Server provide
	// Response Info in the Connack
	RequestResponseInfo *byte
	// ResponseInfo is a UTF8 encoded string that can be used as the basis for
	// creating a Response Topic. The way in which the Client creates a
	// Response Topic from the Response Info is not defined. A common
	// use of this is to pass a globally unique portion of the topic tree which
	// is reserved for this Client for at least the lifetime of its Session. This
	// often cannot just be a random name as both the requesting Client and the
	// responding Client need to be authorized to use it. It is normal to use this
	// as the root of a topic tree for a particular Client. For the Server to
	// return this Info, it normally needs to be correctly configured.
	// Using this mechanism allows this configuration to be done once in the
	// Server rather than in each Client
	ResponseInfo []byte
	// ServerReference is a UTF8 string indicating another server the client
	// can use
	ServerReference []byte
	// ReasonString is a UTF8 string representing the reason associated with
	// this response, intended to be human readable for diagnostic purposes
	ReasonString []byte
	// ReceiveMaximum is the maximum number of QOS1 & 2 messages allowed to be
	// 'inflight' (not having received a PUBACK/PUBCOMP response for)
	ReceiveMaximum *uint16
	// TopicAliasMaximum is the highest value permitted as a Topic Alias
	TopicAliasMaximum *uint16
	// TopicAlias is used in place of the topic string to reduce the size of
	// packets for repeated messages on a topic
	TopicAlias *uint16
	// MaximumQoS is the highest QOS level permitted for a Publish
	MaximumQoS *byte
	// RetainAvailable indicates whether the server supports messages with the
	// retain flag set
	RetainAvailable *byte
	// User is a map of user provided properties
	User []UserProperty

	// MaximumPacketSize allows the client or server to specify the maximum packet
	// size in bytes that they support
	MaximumPacketSize *uint32
	// WildcardSubAvailable indicates whether wildcard subscriptions are permitted
	WildcardSubAvailable *byte
	// SubIDAvailable indicates whether subscription identifiers are supported
	SubIDAvailable *byte
	// SharedSubAvailable indicates whether shared subscriptions are supported
	SharedSubAvailable *byte
}

func sprintf(name string, v interface{}) string {
	if v == nil {
		return fmt.Sprintf("%s: %v", name, v)
	}
	t := reflect.TypeOf(v)
	if t.Kind() == reflect.Ptr {
		rv := reflect.ValueOf(v)
		if rv.IsNil() {
			return fmt.Sprintf("%s: nil", name)
		}
		return fmt.Sprintf("%s: %v", name, reflect.ValueOf(v).Elem())
	}
	return fmt.Sprintf("%s: %v", name, v)

}

func (p *Properties) String() string {
	var str []string
	str = append(str, sprintf("PayloadFormat", p.PayloadFormat))
	str = append(str, sprintf("MessageExpiry", p.MessageExpiry))
	str = append(str, sprintf("ContentType", p.ContentType))
	str = append(str, sprintf("ResponseTopic", p.ResponseTopic))
	str = append(str, sprintf("CorrelationData", p.CorrelationData))
	str = append(str, sprintf("SubscriptionIdentifier", p.SubscriptionIdentifier))
	str = append(str, sprintf("SessionExpiryInterval", p.SessionExpiryInterval))
	str = append(str, sprintf("AssignedClientID", p.AssignedClientID))
	str = append(str, sprintf("ServerKeepAlive", p.ServerKeepAlive))
	str = append(str, sprintf("AuthMethod", p.AuthMethod))
	str = append(str, sprintf("AuthData", p.AuthData))
	str = append(str, sprintf("RequestProblemInfo", p.RequestProblemInfo))
	str = append(str, sprintf("WillDelayInterval", p.WillDelayInterval))
	str = append(str, sprintf("RequestResponseInfo", p.RequestResponseInfo))
	str = append(str, sprintf("ResponseInfo", p.ResponseInfo))
	str = append(str, sprintf("ServerReference", p.ServerReference))
	str = append(str, sprintf("ReasonString", p.ReasonString))
	str = append(str, sprintf("ReceiveMaximum", p.ReceiveMaximum))
	str = append(str, sprintf("TopicAliasMaximum", p.TopicAliasMaximum))
	str = append(str, sprintf("TopicAlias", p.TopicAlias))
	str = append(str, sprintf("MaximumQoS", p.MaximumQoS))
	str = append(str, sprintf("RetainAvailable", p.RetainAvailable))
	str = append(str, sprintf("User", p.User))
	str = append(str, sprintf("MaximumPacketSize", p.MaximumPacketSize))
	str = append(str, sprintf("WildcardSubAvailable", p.WildcardSubAvailable))
	str = append(str, sprintf("SubIDAvailable", p.SubIDAvailable))
	str = append(str, sprintf("SharedSubAvailable", p.SharedSubAvailable))
	return strings.Join(str, ", ")
}

func (p *Properties) PackWillProperties(bufWriter *bytes.Buffer) (err error) {
	newBufWriter := &bytes.Buffer{}
	defer func() {
		b, _ := DecodeRemainLength(newBufWriter.Len())
		bufWriter.Write(b)
		_, err = newBufWriter.WriteTo(bufWriter)
	}()
	if p == nil {
		return
	}
	propertyWriteByte(PropPayloadFormat, p.PayloadFormat, newBufWriter)
	propertyWriteUint32(PropMessageExpiry, p.MessageExpiry, newBufWriter)
	propertyWriteString(PropContentType, p.ContentType, newBufWriter)
	propertyWriteString(PropResponseTopic, p.ResponseTopic, newBufWriter)
	propertyWriteString(PropCorrelationData, p.CorrelationData, newBufWriter)
	propertyWriteUint32(PropWillDelayInterval, p.WillDelayInterval, newBufWriter)
	if len(p.User) != 0 {
		for _, v := range p.User {
			newBufWriter.WriteByte(PropUser)
			writeBinary(newBufWriter, v.K)
			writeBinary(newBufWriter, v.V)
		}
	}
	return
}

// Pack takes all the defined properties for an Properties and produces
// a slice of bytes representing the wire format for the Info
func (p *Properties) Pack(bufWriter *bytes.Buffer, packetType byte) (err error) {
	newBufWriter := &bytes.Buffer{}
	defer func() {
		b, _ := DecodeRemainLength(newBufWriter.Len())
		bufWriter.Write(b)
		_, err = newBufWriter.WriteTo(bufWriter)
	}()
	if p == nil {
		return
	}
	propertyWriteByte(PropPayloadFormat, p.PayloadFormat, newBufWriter)
	propertyWriteUint32(PropMessageExpiry, p.MessageExpiry, newBufWriter)
	propertyWriteString(PropContentType, p.ContentType, newBufWriter)
	propertyWriteString(PropResponseTopic, p.ResponseTopic, newBufWriter)
	propertyWriteString(PropCorrelationData, p.CorrelationData, newBufWriter)

	if len(p.SubscriptionIdentifier) != 0 {
		for _, v := range p.SubscriptionIdentifier {
			newBufWriter.WriteByte(PropSubscriptionIdentifier)
			b, _ := DecodeRemainLength(int(v))
			newBufWriter.Write(b)
		}
	}
	propertyWriteUint32(PropSessionExpiryInterval, p.SessionExpiryInterval, newBufWriter)
	propertyWriteString(PropAssignedClientID, p.AssignedClientID, newBufWriter)
	propertyWriteUint16(PropServerKeepAlive, p.ServerKeepAlive, newBufWriter)
	propertyWriteString(PropAuthMethod, p.AuthMethod, newBufWriter)
	propertyWriteString(PropAuthData, p.AuthData, newBufWriter)
	propertyWriteByte(PropRequestProblemInfo, p.RequestProblemInfo, newBufWriter)
	propertyWriteUint32(PropWillDelayInterval, p.WillDelayInterval, newBufWriter)
	propertyWriteByte(PropRequestResponseInfo, p.RequestResponseInfo, newBufWriter)
	propertyWriteString(PropResponseInfo, p.ResponseInfo, newBufWriter)
	propertyWriteString(PropServerReference, p.ServerReference, newBufWriter)
	propertyWriteString(PropReasonString, p.ReasonString, newBufWriter)
	propertyWriteUint16(PropReceiveMaximum, p.ReceiveMaximum, newBufWriter)
	propertyWriteUint16(PropTopicAliasMaximum, p.TopicAliasMaximum, newBufWriter)
	propertyWriteUint16(PropTopicAlias, p.TopicAlias, newBufWriter)
	propertyWriteByte(PropMaximumQoS, p.MaximumQoS, newBufWriter)
	propertyWriteByte(PropRetainAvailable, p.RetainAvailable, newBufWriter)

	if len(p.User) != 0 {
		for _, v := range p.User {
			newBufWriter.WriteByte(PropUser)
			writeBinary(newBufWriter, v.K)
			writeBinary(newBufWriter, v.V)
		}
	}

	propertyWriteUint32(PropMaximumPacketSize, p.MaximumPacketSize, newBufWriter)
	propertyWriteByte(PropWildcardSubAvailable, p.WildcardSubAvailable, newBufWriter)
	propertyWriteByte(PropSubIDAvailable, p.SubIDAvailable, newBufWriter)
	propertyWriteByte(PropSharedSubAvailable, p.SharedSubAvailable, newBufWriter)
	return
}

func (p *Properties) UnpackWillProperties(bufReader *bytes.Buffer) error {
	length, err := EncodeRemainLength(bufReader)
	// read exceeded length error
	if err != nil {
		return err
	}
	if length == 0 {
		return nil
	}
	newBufReader := bytes.NewBuffer(bufReader.Next(length))
	var propType byte
	for {
		if err != nil {
			return err
		}
		propType, err = newBufReader.ReadByte()
		if err != nil && err != io.EOF {
			return err
		}
		if err == io.EOF {
			break
		}
		switch propType {
		case PropWillDelayInterval:
			p.WillDelayInterval, err = propertyReadUint32(p.WillDelayInterval, newBufReader, propType, nil)
		case PropPayloadFormat:
			p.PayloadFormat, err = propertyReadBool(p.PayloadFormat, newBufReader, propType)
		case PropMessageExpiry:
			p.MessageExpiry, err = propertyReadUint32(p.MessageExpiry, newBufReader, propType, nil)
		case PropContentType:
			p.ContentType, err = propertyReadUTF8String(p.ContentType, newBufReader, propType, nil)
		case PropResponseTopic:
			p.ResponseTopic, err = propertyReadUTF8String(p.ResponseTopic, newBufReader, propType, func(u []byte) bool {
				// [MQTT-3.3.2-14]
				return ValidTopicName(true, u)
			})
		case PropCorrelationData:
			p.CorrelationData, err = propertyReadBinary(p.CorrelationData, newBufReader, propType, nil)
		case PropUser:
			k, err := readUTF8String(true, newBufReader)
			if err != nil {
				return errors.ErrMalformed
			}
			v, err := readUTF8String(true, newBufReader)
			if err != nil {
				return errors.ErrMalformed
			}
			p.User = append(p.User, UserProperty{K: k, V: v})
		default:
			return errors.ErrMalformed
		}
	}
	return nil
}

// Unpack takes a buffer of bytes and reads out the defined properties
// filling in the appropriate entries in the struct, it returns the number
// of bytes used to store the Prop data and any error in decoding them
func (p *Properties) Unpack(bufReader *bytes.Buffer, packetType byte) error {
	var err error
	length, err := EncodeRemainLength(bufReader)
	// read exceeded length error
	if err != nil {
		return err
	}
	if length == 0 {
		return nil
	}
	newBufReader := bytes.NewBuffer(bufReader.Next(length))
	var propType byte
	for {
		if err != nil {
			return err
		}
		propType, err = newBufReader.ReadByte()
		if err != nil && err != io.EOF {
			return err
		}
		if err == io.EOF {
			break
		}

		if !ValidateID(packetType, propType) {
			return errors.ErrProtocol
		}

		switch propType {
		case PropPayloadFormat:
			p.PayloadFormat, err = propertyReadBool(p.PayloadFormat, newBufReader, propType)
		case PropMessageExpiry:
			p.MessageExpiry, err = propertyReadUint32(p.MessageExpiry, newBufReader, propType, nil)
		case PropContentType:
			p.ContentType, err = propertyReadUTF8String(p.ContentType, newBufReader, propType, nil)
		case PropResponseTopic:
			p.ResponseTopic, err = propertyReadUTF8String(p.ResponseTopic, newBufReader, propType, func(u []byte) bool {
				// [MQTT-3.3.2-14]
				return ValidTopicName(true, u)
			})
		case PropCorrelationData:
			p.CorrelationData, err = propertyReadBinary(p.CorrelationData, newBufReader, propType, nil)
		case PropSubscriptionIdentifier:
			if len(p.SubscriptionIdentifier) != 0 {
				return errors.ErrProtocol
			}
			si, err := EncodeRemainLength(newBufReader)
			if err != nil {
				return errors.ErrMalformed
			}
			if si == 0 {
				return errors.ErrProtocol
			}
			p.SubscriptionIdentifier = append(p.SubscriptionIdentifier, uint32(si))
		case PropSessionExpiryInterval:
			p.SessionExpiryInterval, err = propertyReadUint32(p.SessionExpiryInterval, newBufReader, propType, nil)
		case PropAssignedClientID:
			p.AssignedClientID, err = propertyReadUTF8String(p.AssignedClientID, newBufReader, propType, nil)
		case PropServerKeepAlive:
			p.ServerKeepAlive, err = propertyReadUint16(p.ServerKeepAlive, newBufReader, propType, nil)
		case PropAuthMethod:
			p.AuthMethod, err = propertyReadUTF8String(p.AuthMethod, newBufReader, propType, nil)
		case PropAuthData:
			p.AuthData, err = propertyReadUTF8String(p.AuthData, newBufReader, propType, nil)
		case PropRequestProblemInfo:
			p.RequestProblemInfo, err = propertyReadBool(p.RequestProblemInfo, newBufReader, propType)
		case PropWillDelayInterval:
			p.WillDelayInterval, err = propertyReadUint32(p.WillDelayInterval, newBufReader, propType, nil)
		case PropRequestResponseInfo:
			p.RequestResponseInfo, err = propertyReadBool(p.RequestResponseInfo, newBufReader, propType)
		case PropResponseInfo:
			p.ResponseInfo, err = propertyReadUTF8String(p.ResponseInfo, newBufReader, propType, nil)
		case PropServerReference:
			p.ServerReference, err = propertyReadUTF8String(p.ServerReference, newBufReader, propType, nil)
		case PropReasonString:
			p.ReasonString, err = propertyReadUTF8String(p.ReasonString, newBufReader, propType, nil)
		case PropReceiveMaximum:
			p.ReceiveMaximum, err = propertyReadUint16(p.ReceiveMaximum, newBufReader, propType, func(u uint16) bool {
				return u != 0
			})
		case PropTopicAliasMaximum:
			p.TopicAliasMaximum, err = propertyReadUint16(p.TopicAliasMaximum, newBufReader, propType, nil)
		case PropTopicAlias:
			// alias cannot be 0, because the error code is not uniform, it has been moved to publish-handler [MQTT-3.3.2-8]
			p.TopicAlias, err = propertyReadUint16(p.TopicAlias, newBufReader, propType, nil)
		case PropMaximumQoS:
			p.MaximumQoS, err = propertyReadBool(p.MaximumQoS, newBufReader, propType)
		case PropRetainAvailable:
			p.RetainAvailable, err = propertyReadBool(p.RetainAvailable, newBufReader, propType)
		case PropUser:
			k, err := readUTF8String(true, newBufReader)
			if err != nil {
				return errors.ErrMalformed
			}
			v, err := readUTF8String(true, newBufReader)
			if err != nil {
				return errors.ErrMalformed
			}
			p.User = append(p.User, UserProperty{K: k, V: v})
		case PropMaximumPacketSize:
			p.MaximumPacketSize, err = propertyReadUint32(p.MaximumPacketSize, newBufReader, propType, func(u uint32) bool {
				return u != 0
			})
		case PropWildcardSubAvailable:
			p.WildcardSubAvailable, err = propertyReadBool(p.WildcardSubAvailable, newBufReader, propType)
		case PropSubIDAvailable:
			p.SubIDAvailable, err = propertyReadBool(p.SubIDAvailable, newBufReader, propType)
		case PropSharedSubAvailable:
			p.SharedSubAvailable, err = propertyReadBool(p.SharedSubAvailable, newBufReader, propType)
		default:
			return errors.ErrMalformed
		}
	}
	if p.AuthData != nil && p.AuthMethod == nil {
		return errors.ErrMalformed
	}
	return nil
}

// ValidProperties is a map of the various properties and the
// PacketTypes that is valid for server to unpack.
var ValidProperties = map[byte]map[byte]struct{}{
	PropPayloadFormat:          {CONNECT: {}, PUBLISH: {}},
	PropMessageExpiry:          {CONNECT: {}, PUBLISH: {}},
	PropContentType:            {CONNECT: {}, PUBLISH: {}},
	PropResponseTopic:          {CONNECT: {}, PUBLISH: {}},
	PropCorrelationData:        {CONNECT: {}, PUBLISH: {}},
	PropSubscriptionIdentifier: {SUBSCRIBE: {}},
	PropSessionExpiryInterval:  {CONNECT: {}, CONNACK: {}, DISCONNECT: {}},
	PropAssignedClientID:       {CONNACK: {}},
	PropServerKeepAlive:        {CONNACK: {}},
	PropAuthMethod:             {CONNECT: {}, CONNACK: {}, AUTH: {}},
	PropAuthData:               {CONNECT: {}, CONNACK: {}, AUTH: {}},
	PropRequestProblemInfo:     {CONNECT: {}},
	PropWillDelayInterval:      {CONNECT: {}},
	PropRequestResponseInfo:    {CONNECT: {}},
	PropResponseInfo:           {CONNACK: {}},
	PropServerReference:        {CONNACK: {}, DISCONNECT: {}},
	PropReasonString:           {CONNACK: {}, PUBACK: {}, PUBREC: {}, PUBREL: {}, PUBCOMP: {}, SUBACK: {}, UNSUBACK: {}, DISCONNECT: {}, AUTH: {}},
	PropReceiveMaximum:         {CONNECT: {}, CONNACK: {}},
	PropTopicAliasMaximum:      {CONNECT: {}, CONNACK: {}},
	PropTopicAlias:             {PUBLISH: {}},
	PropMaximumQoS:             {CONNACK: {}},
	PropRetainAvailable:        {CONNACK: {}},
	PropUser:                   {CONNECT: {}, CONNACK: {}, PUBLISH: {}, PUBACK: {}, PUBREC: {}, PUBREL: {}, PUBCOMP: {}, SUBSCRIBE: {}, UNSUBSCRIBE: {}, SUBACK: {}, UNSUBACK: {}, DISCONNECT: {}, AUTH: {}},
	PropMaximumPacketSize:      {CONNECT: {}, CONNACK: {}},
	PropWildcardSubAvailable:   {CONNACK: {}},
	PropSubIDAvailable:         {CONNACK: {}},
	PropSharedSubAvailable:     {CONNACK: {}},
}

// ValidateID takes a PacketType and a property name and returns
// a boolean indicating if that property is valid for that
// PacketType
func ValidateID(packetType byte, i byte) bool {
	_, ok := ValidProperties[i][packetType]
	return ok
}

func ValidateCode(packType byte, code byte) bool {
	return true
}

func propertyReadBool(i *byte, r *bytes.Buffer, _ byte) (*byte, error) {
	if i != nil {
		return nil, errors.ErrProtocol
	}
	o, err := r.ReadByte()
	if err != nil {
		return nil, errors.ErrMalformed
	}
	if o != 0 && o != 1 {
		return nil, errors.ErrProtocol
	}
	return &o, nil
}

func propertyReadUint32(
	i *uint32,
	r *bytes.Buffer,
	propType byte,
	validate func(u uint32) bool,
) (*uint32, error) {
	if i != nil {
		return nil, errMoreThanOnce(propType)
	}
	o, err := readUint32(r)
	if err != nil {
		return nil, errors.ErrMalformed
	}
	if validate != nil {
		if !validate(o) {
			return nil, errors.ErrProtocol
		}
	}
	return &o, nil
}

func propertyReadUint16(i *uint16, r *bytes.Buffer, propType byte, validate func(u uint16) bool) (*uint16, error) {
	if i != nil {
		return nil, errors.ErrProtocol
	}
	o, err := readUint16(r)
	if err != nil {
		return nil, errors.ErrMalformed
	}
	if validate != nil {
		if !validate(o) {
			return nil, errors.ErrProtocol
		}
	}
	return &o, nil
}

func propertyReadUTF8String(i []byte, r *bytes.Buffer, propType byte, validate func(u []byte) bool) (b []byte, err error) {
	if i != nil {
		return nil, errMoreThanOnce(propType)
	}
	o, err := readUTF8String(true, r)
	if err != nil {
		return nil, err
	}
	if validate != nil {
		if !validate(o) {
			return nil, errors.ErrProtocol
		}
	}
	return o, nil
}

func propertyReadBinary(i []byte, r *bytes.Buffer, propType byte,
	validate func(u []byte) bool) (b []byte, err error) {
	if i != nil {
		return nil, errMoreThanOnce(propType)
	}
	o, err := readUTF8String(false, r)
	if err != nil {
		return nil, errors.ErrMalformed
	}
	if validate != nil {
		if !validate(o) {
			return nil, errors.ErrProtocol
		}
	}
	return o, nil
}

func propertyWriteByte(t byte, i *byte, w *bytes.Buffer) {
	if i != nil {
		w.Write([]byte{t, *i})
	}
}

func propertyWriteUint16(t byte, i *uint16, w *bytes.Buffer) {
	if i != nil {
		w.WriteByte(t)
		writeUint16(w, *i)
	}
}

func propertyWriteUint32(t byte, i *uint32, w *bytes.Buffer) {
	if i != nil {
		w.WriteByte(t)
		writeUint32(w, *i)
	}
}

func propertyWriteString(t byte, i []byte, w *bytes.Buffer) {
	if i != nil {
		w.WriteByte(t)
		writeBinary(w, i)
	}
}
