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
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	pb "github.com/Vyary/rdpc/proto"

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

	db.SetMaxOpenConns(1)
	db.Exec("PRAGMA journal_mode=WAL;")
	db.Exec("PRAGMA busy_timeout=5000;")

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

func (s *service) InsertStats(ctx context.Context, stats *pb.Stats) (*pb.Empty, error) {
	query := `
	INSERT INTO stats (id, text, type)
	VALUES (?, ?, ?)
	ON CONFLICT(id) DO UPDATE SET
		text = excluded.text,
		type = excluded.type`

	_, err := s.db.Exec(query, stats.Id, stats.Text, stats.Type)
	if err != nil {
		return &pb.Empty{}, status.Errorf(codes.Internal, "inserting stats for id: %s: %s", stats.Id, err.Error())
	}

	return &pb.Empty{}, nil
}

func (s *service) InsertItem(ctx context.Context, i *pb.Item) (*pb.Empty, error) {
	query := `
	INSERT INTO items (name, base_type, category, sub_category, realm)
	VALUES (?, ?, ?, ?, ?)`

	_, err := s.db.Exec(query, i.Name, i.BaseType, i.Category, i.SubCategory, "poe2")
	if err != nil {
		return &pb.Empty{}, status.Errorf(codes.Internal, "inserting item %s", err.Error())
	}

	return &pb.Empty{}, nil
}

func (s *service) InsertItemWithID(ctx context.Context, i *pb.Item) (*pb.Empty, error) {
	query := `
	INSERT INTO items (id, name, base_type, category, sub_category, realm)
	VALUES (?, ?, ?, ?, ?, ?)`

	_, err := s.db.Exec(query, i.Id, i.Name, i.BaseType, i.Category, i.SubCategory, "poe2")
	if err != nil {
		return &pb.Empty{}, status.Errorf(codes.Internal, "inserting item with id: %s: %s", i.Id, err.Error())
	}

	return &pb.Empty{}, nil
}

func (s *service) InsertQuery(ctx context.Context, q *pb.Query) (*pb.Empty, error) {
	query := `
	INSERT INTO queries (item_id, realm, league, search_query, update_interval, next_run, run_once)
	VALUES (?, ?, ?, ?, ?, ?, ?)`

	_, err := s.db.Exec(query, q.ItemId, q.Realm, q.League, q.Query, q.Update, q.NextRun, q.RunOnce)
	if err != nil {
		return &pb.Empty{}, status.Errorf(codes.Internal, "inserting query with id: %s: %s", q.ItemId, err.Error())
	}

	return &pb.Empty{}, nil
}

func (s *service) InsertPrice(ctx context.Context, p *pb.Price) (*pb.Empty, error) {
	query := `
		INSERT INTO prices (item_id, price, currency_id, volume, stock, league, timestamp)
		VALUES (?, ?, ?, ?, ?, ?, ?)`

	_, err := s.db.Exec(query, p.ItemId, p.Price, p.CurrencyId, p.Volume, p.Stock, p.League, p.Timestamp)
	if err != nil {
		return &pb.Empty{}, status.Errorf(codes.Internal, "inserting price for item_id: %s: %s", p.ItemId, err.Error())
	}

	return &pb.Empty{}, nil
}

func (s *service) HasItem(ctx context.Context, ir *pb.HasItemRequest) (*pb.BoolResponse, error) {
	query := `SELECT EXISTS(SELECT 1 FROM items WHERE name = ? AND base_type = ?)`

	var exists bool
	err := s.db.QueryRow(query, ir.Name, ir.BaseType).Scan(&exists)
	if err != nil {
		return &pb.BoolResponse{Has: false}, status.Errorf(codes.Internal, "checking if item exists: %s", err.Error())
	}

	return &pb.BoolResponse{Has: exists}, nil
}

func (s *service) HasInfo(ctx context.Context, ir *pb.ItemIDRequest) (*pb.BoolResponse, error) {
	existsQuery := `
	SELECT EXISTS(SELECT 1 
	FROM queries 
	WHERE item_id = ? AND run_once = true);`

	var exists bool
	err := s.db.QueryRow(existsQuery, ir.ItemId).Scan(&exists)
	if err != nil {
		return &pb.BoolResponse{Has: false}, status.Errorf(codes.Internal, "failed to check query existence for item_id %s: %s", ir.ItemId, err.Error())
	}

	if exists {
		return &pb.BoolResponse{Has: true}, nil
	}

	query := `
	SELECT icon
	FROM items
	WHERE id = ?`

	var icon string
	err = s.db.QueryRow(query, ir.ItemId).Scan(&icon)
	if err != nil {
		return &pb.BoolResponse{Has: false}, status.Errorf(codes.Internal, "checking for info for item_id %s: %s", ir.ItemId, err)
	}

	if icon != "" {
		return &pb.BoolResponse{Has: true}, nil
	}

	return &pb.BoolResponse{Has: false}, nil
}

func (s *service) HasPriceQuery(ctx context.Context, pr *pb.HasPriceRequest) (*pb.BoolResponse, error) {
	query := `
	SELECT EXISTS(SELECT 1 
	FROM queries 
	WHERE item_id = ? AND league = ? AND run_once = false);`

	var exists bool
	err := s.db.QueryRow(query, pr.ItemId, pr.League).Scan(&exists)
	if err != nil {
		return &pb.BoolResponse{Has: false}, status.Errorf(codes.Internal, "checking query existence for item_id %s: %s", pr.ItemId, err.Error())
	}

	return &pb.BoolResponse{Has: exists}, nil
}

func (s *service) GetBaseItems(ctx context.Context, cr *pb.CategoryRequest) (*pb.BaseItems, error) {
	query := `
	SELECT
		id,
		realm,
		name,
		base_type
	FROM items
	WHERE (? = '' OR category = ?)`

	rows, err := s.db.Query(query, cr.Category, cr.Category)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := &pb.BaseItems{}

	for rows.Next() {
		var i pb.BaseItem
		err := rows.Scan(
			&i.Id,
			&i.Realm,
			&i.Name,
			&i.BaseType,
		)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "retrieving items: %s", err.Error())
		}
		items.Items = append(items.Items, &i)
	}

	if err := rows.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "iteraton error: %s", err.Error())
	}

	return items, nil
}

func (s *service) GetInfoQueries(ctx context.Context, _ *pb.Empty) (*pb.Queries, error) {
	query := `
	UPDATE queries
	SET status = 'in_progress', started_at = ?
	WHERE id IN (
		SELECT id
		FROM queries
		WHERE (status = 'queued' OR (status = 'in_progress' AND started_at < ?)) AND next_run < ? AND run_once = true
		ORDER BY id
		LIMIT 4
	)
	RETURNING id, item_id, realm, league, search_query, update_interval, next_run, status, started_at, run_once`

	now := time.Now().UTC().Unix()
	lease := time.Now().Add(-5 * time.Minute).UTC().Unix()

	rows, err := s.db.Query(query, now, lease, now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	queries := &pb.Queries{}

	for rows.Next() {
		var q pb.Query

		if err := rows.Scan(&q.Id, &q.ItemId, &q.Realm, &q.League, &q.Query, &q.Update, &q.NextRun, &q.Status, &q.StartedAt, &q.RunOnce); err != nil {
			return nil, err
		}

		queries.Queries = append(queries.Queries, &q)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return queries, nil
}

func (s *service) GetPriceQueries(ctx context.Context, _ *pb.Empty) (*pb.Queries, error) {
	query := `
	UPDATE queries
	SET status = 'in_progress', started_at = ?
	WHERE id IN (
		SELECT id
		FROM queries
		WHERE (status = 'queued' OR (status = 'in_progress' AND started_at < ?)) AND next_run < ? AND run_once = false
		ORDER BY id
		LIMIT 4
	)
	RETURNING id, item_id, realm, league, search_query, update_interval, next_run, status, started_at, run_once`

	now := time.Now().UTC().Unix()
	lease := time.Now().Add(-5 * time.Minute).UTC().Unix()

	rows, err := s.db.Query(query, now, lease, now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	queries := &pb.Queries{}

	for rows.Next() {
		var q pb.Query

		if err := rows.Scan(&q.Id, &q.ItemId, &q.Realm, &q.League, &q.Query, &q.Update, &q.NextRun, &q.Status, &q.StartedAt, &q.RunOnce); err != nil {
			return nil, err
		}

		queries.Queries = append(queries.Queries, &q)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return queries, nil
}

func (s *service) GetMod(ctx context.Context, mr *pb.GetModRequest) (*pb.GetModResponse, error) {
	query := `
	SELECT text
	FROM stats
	WHERE id = ?`

	var mod pb.GetModResponse
	err := s.db.QueryRow(query, mr.Hash).Scan(&mod)

	return &mod, err
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
func (s *service) UpdateItemInfo(ctx context.Context, i *pb.Item) (*pb.Empty, error) {
	query := `
	UPDATE items
	SET 
		realm = ?, 
		icon = ?, 
		icon_tier_text = ?, 
		name = ?, 
		base_type = ?, 
		rarity = ?, 
		w = ?, 
		h = ?, 
		ilvl = ?,
		socketed_items = ?,
		properties = ?, 
		requirements = ?, 
		enchant_mods = ?, 
		rune_mods = ?, 
		implicit_mods = ?, 
		explicit_mods = ?, 
		fractured_mods = ?, 
		desecrated_mods = ?, 
		flavour_text = ?, 
		descr_text = ?, 
		sec_descr_text = ?, 
		support = ?, 
		duplicated = ?,
		corrupted = ?,
		sanctified = ?,
		desecrated = ?
	WHERE id = ?`

	_, err := s.db.Exec(query, i.Realm, i.Icon, i.IconTierText, i.Name, i.BaseType, i.Rarity, i.W, i.H, i.Ilvl, i.SocketedItems, i.Properties, i.Requirements, i.EnchantMods, i.RuneMods, i.ImplicitMods, i.ExplicitMods, i.FracturedMods, i.DesecratedMods, i.FlavourText, i.DescrText, i.SecDescrText, i.Support, i.Duplicated, i.Corrupted, i.Sanctified, i.Desecrated, i.Id)
	if err != nil {
		return &pb.Empty{}, err
	}

	return &pb.Empty{}, nil
}

func (s *service) UpdateNextRun(ctx context.Context, q *pb.Query) (*pb.Empty, error) {
	query := `
	UPDATE queries
	SET next_run = ?, status = 'queued', started_at = 0
	WHERE id = ? AND league = ?`

	nextRun := time.Now().Add(time.Duration(q.Update) * time.Hour).UTC().Unix()

	_, err := s.db.Exec(query, nextRun, q.Id, q.League)

	return &pb.Empty{}, err
}

func (s *service) DeleteQuery(ctx context.Context, ir *pb.ItemIDRequest) (*pb.Empty, error) {
	query := `
	DELETE FROM queries
	WHERE id = ?`

	_, err := s.db.Exec(query, ir.ItemId)
	return &pb.Empty{}, err
}
