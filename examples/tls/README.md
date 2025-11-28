# TLS Example

This example demonstrates how to configure TLS encryption for secure Flight connections.

## Generating Test Certificates

For testing purposes, generate self-signed certificates:

```bash
# Generate CA
openssl genrsa -out ca-key.pem 4096
openssl req -new -x509 -days 365 -key ca-key.pem -out ca-cert.pem \
  -subj "/CN=Test CA"

# Generate server certificate
openssl genrsa -out server-key.pem 4096
openssl req -new -key server-key.pem -out server.csr \
  -subj "/CN=localhost"
openssl x509 -req -days 365 -in server.csr -CA ca-cert.pem -CAkey ca-key.pem \
  -CAcreateserial -out server-cert.pem
```

## Running the Server

```bash
cd examples/tls
go run main.go
```

## Connecting from DuckDB

### Basic TLS

```sql
ATTACH 'grpc+tls://localhost:50051' AS secure_data (TYPE airport);
SELECT * FROM secure_data.secure.messages;
```

### TLS with Bearer Token Authentication

If the server requires authentication, configure DuckDB with a bearer token:

```sql
CREATE SECRET airport_tls (
    TYPE AIRPORT,
    auth_token 'your-bearer-token',
    scope 'grpc+tls://localhost:50051'
);

ATTACH '' AS secure_data (
    TYPE airport,
    SECRET airport_tls,
    LOCATION 'grpc+tls://localhost:50051'
);
```

Note: DuckDB Airport currently supports TLS for transport encryption only. Client certificate authentication (mTLS) is not yet supported in the Airport extension.

## Configuration Options

### Server-side TLS Config

```go
tlsConfig := &tls.Config{
    Certificates: []tls.Certificate{serverCert},
    ClientAuth:   tls.RequireAndVerifyClientCert, // Enable mTLS
    ClientCAs:    certPool,
    MinVersion:   tls.VersionTLS13, // Use TLS 1.3
}
```

### Client Authentication Modes

The server-side code supports various TLS client authentication modes:

- `tls.NoClientCert` - Server-only TLS (default, recommended for DuckDB Airport)
- `tls.RequestClientCert` - Request but don't verify client cert
- `tls.RequireAnyClientCert` - Require client cert, any CA
- `tls.VerifyClientCertIfGiven` - Verify if provided
- `tls.RequireAndVerifyClientCert` - Mutual TLS (mTLS)

Note: While the server supports mTLS, DuckDB Airport extension currently only supports TLS transport without client certificates. Use bearer token authentication if you need client authentication.

## Production Considerations

1. **Certificate Management**
   - Use proper CA-signed certificates
   - Implement certificate rotation
   - Store private keys securely (e.g., HSM, secrets manager)

2. **TLS Version**
   - Use TLS 1.3 when possible
   - Minimum TLS 1.2 for compatibility

3. **Cipher Suites**
   - Configure strong cipher suites
   - Disable weak ciphers

4. **Certificate Validation**
   - Verify certificate chains
   - Check certificate revocation (CRL/OCSP)
   - Validate hostname/SAN

5. **Monitoring**
   - Log TLS handshake failures
   - Monitor certificate expiration
   - Alert on weak cipher usage

## See Also

- [gRPC Authentication Guide](https://grpc.io/docs/guides/auth/)
- [Go TLS Documentation](https://pkg.go.dev/crypto/tls)
- [DuckDB Airport Extension](https://duckdb.org/docs/extensions/airport.html)
