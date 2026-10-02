// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package stress

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
)

// DNS query types the load generator sends.
const (
	typeA    uint16 = 1
	typeAAAA uint16 = 28
)

// Response codes the harness distinguishes.
const (
	rcodeSuccess  = 0
	rcodeServFail = 2
	rcodeNXDomain = 3
	rcodeNotImp   = 4
	rcodeRefused  = 5
)

const headerLen = 12

// rrALen is the size of one A record whose name is a pointer to the question.
const rrALen = 16

// buildQuery returns a recursive query for name and qtype with the given ID.
func buildQuery(id uint16, name string, qtype uint16) []byte {
	msg := make([]byte, headerLen, headerLen+len(name)+6)
	binary.BigEndian.PutUint16(msg[0:], id)
	msg[2] = 0x01                          // RD
	binary.BigEndian.PutUint16(msg[4:], 1) // QDCOUNT
	for label := range strings.SplitSeq(strings.TrimSuffix(name, "."), ".") {
		msg = append(msg, byte(len(label)))
		msg = append(msg, label...)
	}
	msg = append(msg, 0)
	msg = binary.BigEndian.AppendUint16(msg, qtype)
	return binary.BigEndian.AppendUint16(msg, 1) // QCLASS IN
}

// buildAnswer answers query with rcode. A successful answer carries A records
// for 192.0.2.1, as many as it takes to reach size bytes (at least one).
func buildAnswer(query []byte, rcode int, size int) ([]byte, error) {
	qEnd, err := questionEnd(query)
	if err != nil {
		return nil, err
	}
	resp := make([]byte, qEnd, max(qEnd+rrALen, size))
	copy(resp, query[:qEnd])
	resp[2] |= 0x80 // QR
	resp[3] = 0x80 | byte(rcode&0x0f)
	binary.BigEndian.PutUint16(resp[6:], 0)  // ANCOUNT
	binary.BigEndian.PutUint16(resp[8:], 0)  // NSCOUNT
	binary.BigEndian.PutUint16(resp[10:], 0) // ARCOUNT: any OPT is dropped
	if rcode != rcodeSuccess {
		return resp, nil
	}
	var n uint16
	for n == 0 || (len(resp)+rrALen <= size && n < 0xffff) {
		resp = append(resp,
			0xc0, headerLen, // name: pointer to the question
			0, 1, 0, 1, // TYPE A, CLASS IN
			0, 0, 0, 30, // TTL
			0, 4, 192, 0, 2, 1)
		n++
	}
	binary.BigEndian.PutUint16(resp[6:], n)
	return resp, nil
}

// checkAnswer validates resp as the answer to query and returns its rcode.
func checkAnswer(query, resp []byte) (int, error) {
	if len(resp) < headerLen {
		return 0, fmt.Errorf("answer is %d bytes, shorter than a DNS header", len(resp))
	}
	if !bytes.Equal(resp[:2], query[:2]) {
		return 0, errors.New("answer ID does not match the query")
	}
	if resp[2]&0x80 == 0 {
		return 0, errors.New("answer does not have QR set")
	}
	qEnd, err := questionEnd(query)
	if err != nil {
		return 0, err
	}
	if len(resp) < qEnd || !bytes.Equal(resp[headerLen:qEnd], query[headerLen:qEnd]) {
		return 0, errors.New("answer question does not match the query")
	}
	return int(resp[3] & 0x0f), nil
}

// messageID returns the ID of msg, which must hold a header.
func messageID(msg []byte) uint16 { return binary.BigEndian.Uint16(msg) }

// questionEnd returns the offset just past the single, uncompressed question.
func questionEnd(msg []byte) (int, error) {
	if len(msg) < headerLen {
		return 0, errors.New("message is shorter than a DNS header")
	}
	if binary.BigEndian.Uint16(msg[4:]) != 1 {
		return 0, errors.New("message does not have exactly one question")
	}
	off := headerLen
	for {
		if off >= len(msg) {
			return 0, errors.New("question name is truncated")
		}
		l := int(msg[off])
		if l == 0 {
			off++
			break
		}
		if l&0xc0 != 0 {
			return 0, errors.New("question name is compressed")
		}
		off += 1 + l
	}
	if off+4 > len(msg) {
		return 0, errors.New("question is truncated")
	}
	return off + 4, nil
}

// rcodeName returns the mnemonic for rcode.
func rcodeName(rcode int) string {
	switch rcode {
	case rcodeSuccess:
		return "NOERROR"
	case rcodeServFail:
		return "SERVFAIL"
	case rcodeNXDomain:
		return "NXDOMAIN"
	case rcodeNotImp:
		return "NOTIMP"
	case rcodeRefused:
		return "REFUSED"
	}
	return fmt.Sprintf("RCODE%d", rcode)
}
