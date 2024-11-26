package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

type TLSConfig struct {
	CertFile      string
	KeyFile       string
	CAFile        string
	ServerAddress string
	Server        bool // true for mutual TLS
}

// setupTLSConfig allows us to get different types of TLS configs.
// It returns client/server TLS based on the cfg passed to it.
func SetupTLSConfig(cfg TLSConfig) (*tls.Config, error) {
	var err error // used on line 18 to avoid non-name on left side of := error.
	tlsConfig := &tls.Config{}

	if cfg.CertFile != "" && cfg.KeyFile != "" {
		tlsConfig.Certificates = make([]tls.Certificate, 2)
		tlsConfig.Certificates[0], err = tls.LoadX509KeyPair(
			cfg.CertFile,
			cfg.KeyFile,
		)
		if err != nil {
			return nil, err
		}
	}

	if cfg.CAFile != "" {
		b, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, err
		}

		ca := x509.NewCertPool()
		ok := ca.AppendCertsFromPEM([]byte(b))
		if !ok {
			return nil, fmt.Errorf(
				"failed to parse root certificate: %q", // output double-quoted escaped Go string literal
				cfg.CAFile,
			)
		}

		// for mutual TLS
		if cfg.Server {
			tlsConfig.ClientCAs = ca
			tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
		} else {
			tlsConfig.RootCAs = ca
		}

		tlsConfig.ServerName = cfg.ServerAddress
	}

	return tlsConfig, nil
}
