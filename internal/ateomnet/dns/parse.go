package dns

import (
	"errors"
	"fmt"
)

// Response codes that say the resolver failed rather than answered. NXDOMAIN
// and NOERROR are answers and are passed back as they are.
const (
	rcodeServFail = 2
	rcodeNotImp   = 4
	rcodeRefused  = 5
)

// failoverRcode reports the response code when the relay should try the next
// upstream. Reads the 12-byte header only; anything shorter is passed through.
func failoverRcode(msg []byte) (byte, bool) {
	if len(msg) < 12 {
		return 0, false
	}
	rcode := msg[3] & 0x0f
	switch rcode {
	case rcodeServFail, rcodeNotImp, rcodeRefused:
		return rcode, true
	}
	return 0, false
}

func rcodeError(rcode byte) error {
	switch rcode {
	case rcodeServFail:
		return errors.New("answered SERVFAIL")
	case rcodeNotImp:
		return errors.New("answered NOTIMP")
	case rcodeRefused:
		return errors.New("answered REFUSED")
	}
	return fmt.Errorf("answered rcode %d", rcode)
}
