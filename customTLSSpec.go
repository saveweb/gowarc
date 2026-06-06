package warc

import (
	tls "github.com/bogdanfinn/utls"
	"github.com/bogdanfinn/tls-client/profiles"
)

type TLSProfile struct {
	clientHelloID               tls.ClientHelloID
	withRandomTLSExtensionOrder bool
	clientProfile               profiles.ClientProfile
}

func NewTLSProfile(profile profiles.ClientProfile, randomExtOrder bool) *TLSProfile {
	helloID := profile.GetClientHelloId()
	if helloID.Client == "" && helloID.Version == "" {
		profile = profiles.Chrome_146
		helloID = profile.GetClientHelloId()
	}
	return &TLSProfile{
		clientHelloID:               helloID,
		withRandomTLSExtensionOrder: randomExtOrder,
		clientProfile:               profile,
	}
}

func DefaultTLSProfile() *TLSProfile {
	return NewTLSProfile(profiles.Chrome_146, true)
}
