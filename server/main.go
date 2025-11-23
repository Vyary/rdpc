package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"log"
	"log/slog"
	"net"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	pb "github.com/Vyary/rdpc/rdpc"

	_ "modernc.org/sqlite"
)

type service struct {
	pb.UnimplementedDatabaseServer
	db *sql.DB
}

func main() {
	lis, err := net.Listen("tcp", ":50052")
	if err != nil {
		log.Fatal("cant create listener")
	}

	dbName := "file:./local/local.db"

	db, err := sql.Open("sqlite", dbName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open db %s", err)
		os.Exit(1)
	}

	serverCert, err := tls.LoadX509KeyPair("./certs/server.crt", "./certs/server.key")
	if err != nil {
		log.Fatalf("Failed to load server certificate: %v", err)
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
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert, // Require mTLS
		ClientCAs:    certPool,
	}

	creds := credentials.NewTLS(tlsConfig)

	grpcSrv := grpc.NewServer(grpc.Creds(creds))

	pb.RegisterDatabaseServer(grpcSrv, &service{db: db})

	slog.Info("serving grpc server")

	if err := grpcSrv.Serve(lis); err != nil {
		log.Fatal("serving server")
	}
}

func (s *service) GetItemsByCategory(context context.Context, category *pb.CategoryRequest) (*pb.Items, error) {
	query := `
	SELECT
		realm,
		category,
		sub_category,
		icon,
		icon_tier_text,
		name,
		base_type,
		rarity,
		w,
		h,
		ilvl,
		socketed_items,
		properties,
		requirements,
		rune_mods,
		implicit_mods,
		explicit_mods,
		fractured_mods,
		desecrated_mods,
		flavour_text,
		descr_text,
		sec_descr_text,
		support,
		duplicated,
		corrupted,
		sanctified,
		desecrated
	FROM items
	WHERE category = ?`

	rows, err := s.db.Query(query, category.Category)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items = &pb.Items{}
	for rows.Next() {
		var i pb.Item
		err := rows.Scan(
			&i.Realm,
			&i.Category,
			&i.SubCategory,
			&i.Icon,
			&i.IconTierText,
			&i.Name,
			&i.BaseType,
			&i.Rarity,
			&i.W,
			&i.H,
			&i.Ilvl,
			&i.SocketedItems,
			&i.Properties,
			&i.Requirements,
			&i.RuneMods,
			&i.ImplicitMods,
			&i.ExplicitMods,
			&i.FracturedMods,
			&i.DesecratedMods,
			&i.FlavourText,
			&i.DescrText,
			&i.SecDescrText,
			&i.Support,
			&i.Duplicated,
			&i.Corrupted,
			&i.Sanctified,
			&i.Desecrated,
		)
		if err != nil {
			return nil, err
		}

		items.Items = append(items.Items, &i)
	}

	return items, rows.Err()
}

func (s *service) InsertPrice(ctx context.Context, p *pb.InsertPriceRequest) (*pb.InsertPriceResponse, error) {
	query := `
		INSERT INTO prices (item_id, price, currency_id, volume, stock, league, timestamp)
		VALUES (?, ?, ?, ?, ?, ?, ?)`

	res, err := s.db.Exec(query, p.Price.ItemId, p.Price.Price, p.Price.CurrencyId, p.Price.Volume, p.Price.Stock, p.Price.League, p.Price.Timestamp)
	if err != nil {
		return &pb.InsertPriceResponse{}, status.Errorf(codes.DataLoss, "inserting price for ItemId: %s", err.Error())
	}

	id, err := res.LastInsertId()
	if err == nil {
		return &pb.InsertPriceResponse{}, status.Error(codes.Internal, "retrieving insert id")
	}

	return &pb.InsertPriceResponse{Id: id}, nil
}
