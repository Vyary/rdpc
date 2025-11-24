package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"log"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	pb "github.com/Vyary/rdpc/proto"
)

func main() {
	clientCert, err := tls.LoadX509KeyPair("./certs/client.crt", "./certs/client.key")
	if err != nil {
		log.Fatalf("Failed to load client certificate: %v", err)
	}

	caCert, err := os.ReadFile("./certs/ca.crt")
	if err != nil {
		log.Fatalf("Failed to read CA certificate: %v", err)
	}

	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(caCert) {
		log.Fatal("Failed to add CA certificate to pool")
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{clientCert},
		RootCAs:      certPool,
		ServerName:   "myserver.example.com",
	}

	creds := credentials.NewTLS(tlsConfig)

	conn, err := grpc.NewClient("localhost:50052", grpc.WithTransportCredentials(creds))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	db := pb.NewDatabaseClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	price := pb.Price{
		ItemId:     "test123",
		Price:      3.4,
		CurrencyId: "werwe",
		Volume:     324,
		Stock:      23,
		League:     "Testing",
		Timestamp:  time.Now().Unix(),
	}

	_, err = db.InsertPrice(ctx, &price)
	if err != nil {
		log.Fatal(err)
	}
}
