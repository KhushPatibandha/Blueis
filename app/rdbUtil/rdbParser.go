package rdbutil

import (
	"encoding/binary"
	"fmt"
	"os"
	"time"

	typestructs "github.com/codecrafters-io/redis-starter-go/app/typeStructs"
)

func readSizeEncoding(file *os.File) (int, error) {
	var firstByte byte
	err := binary.Read(file, binary.LittleEndian, &firstByte)
	if err != nil {
		return 0, err
	}

	switch firstByte >> 6 {
	case 0b00:
		return int(firstByte & 0x3F), nil
	case 0b01:
		var secondByte byte
		err := binary.Read(file, binary.BigEndian, &secondByte)
		if err != nil {
			return 0, err
		}
		return int(firstByte&0x3F)<<8 | int(secondByte), nil
	case 0b10:
		var size int32
		err := binary.Read(file, binary.BigEndian, &size)
		if err != nil {
			return 0, err
		}
		return int(size), nil
	case 0b11:
		return int(firstByte), nil
	}
	return 0, fmt.Errorf("invalid size encoding: 0x%x", firstByte)
}

func readStringEncoding(file *os.File) (string, error) {
	size, err := readSizeEncoding(file)
	if err != nil {
		return "", err
	}

	switch size & 0xC0 {
	case 0x00, 0x40, 0x80:
		data := make([]byte, size)
		_, err = file.Read(data)
		if err != nil {
			return "", err
		}
		return string(data), nil
	case 0xC0:
		switch size & 0x3F {
		case 0x00:
			var value int8
			err = binary.Read(file, binary.LittleEndian, &value)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("%d", value), nil
		case 0x01:
			var value int16
			err = binary.Read(file, binary.LittleEndian, &value)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("%d", value), nil
		case 0x02:
			var value int32
			err = binary.Read(file, binary.LittleEndian, &value)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("%d", value), nil
		case 0x03:
			// LZF compression not supported
			return "", fmt.Errorf("LZF compression is not supported")
		}
	}

	return "", fmt.Errorf("unsupported string encoding: 0x%x", size)
}

func ReadAllKeyValues(file *os.File) (map[string]typestructs.KeyValue, error) {
	keyValueMap := make(map[string]typestructs.KeyValue)

	for {
		var flag byte
		err := binary.Read(file, binary.LittleEndian, &flag)
		if err != nil {
			return nil, err
		}

		switch flag {
		case 0xFA:
			// Read auxiliary field (ignore content)
			_, err := readStringEncoding(file) // key
			if err != nil {
				return nil, err
			}
			_, err = readStringEncoding(file) // value
			if err != nil {
				return nil, err
			}
		case 0xFE:
			// Read database selector (ignore)
			_, err := readSizeEncoding(file)
			if err != nil {
				return nil, err
			}
		case 0xFB:
			// Read resizedb field (ignore sizes)
			_, err := readSizeEncoding(file)
			if err != nil {
				return nil, err
			}
			_, err = readSizeEncoding(file)
			if err != nil {
				return nil, err
			}
		case 0xFC:
			// Expiry time in milliseconds
			var expiryMs int64
			err := binary.Read(file, binary.LittleEndian, &expiryMs)
			if err != nil {
				return nil, err
			}
			expiryTime := time.Unix(0, expiryMs*int64(time.Millisecond))

			// Read key-value pair
			var valueType byte
			err = binary.Read(file, binary.LittleEndian, &valueType)
			if err != nil {
				return nil, err
			}
			key, err := readStringEncoding(file)
			if err != nil {
				return nil, err
			}
			value, err := readStringEncoding(file)
			if err != nil {
				return nil, err
			}
			keyValueMap[key] = typestructs.KeyValue{Value: value, ExpiryTime: &expiryTime}
		case 0xFD:
			// Expiry time in seconds
			var expirySeconds int32
			err := binary.Read(file, binary.LittleEndian, &expirySeconds)
			if err != nil {
				return nil, err
			}
			expiryTime := time.Unix(int64(expirySeconds), 0)

			// Read key-value pair
			var valueType byte
			err = binary.Read(file, binary.LittleEndian, &valueType)
			if err != nil {
				return nil, err
			}
			key, err := readStringEncoding(file)
			if err != nil {
				return nil, err
			}
			value, err := readStringEncoding(file)
			if err != nil {
				return nil, err
			}
			keyValueMap[key] = typestructs.KeyValue{Value: value, ExpiryTime: &expiryTime}
		case 0xFF:
			// End of file
			return keyValueMap, nil
		default:
			// Read key-value pair without expiry
			key, err := readStringEncoding(file)
			if err != nil {
				return nil, err
			}
			value, err := readStringEncoding(file)
			if err != nil {
				return nil, err
			}
			keyValueMap[key] = typestructs.KeyValue{Value: value, ExpiryTime: nil}
		}
	}
}