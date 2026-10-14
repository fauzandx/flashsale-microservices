package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/joho/godotenv"
	amqp "github.com/rabbitmq/amqp091-go"
)

type OrderMessage struct {
	OrderID   string    `json:"order_id"`
	UserID    string    `json:"user_id"`
	ItemID    string    `json:"item_id"`
	CreatedAt time.Time `json:"created_at"`
}

func initDatabase(db *sqlx.DB) error {
	query := `
	CREATE TABLE IF NOT EXISTS orders (
		id VARCHAR(64) PRIMARY KEY,
		user_id VARCHAR(64) NOT NULL,
		item_id VARCHAR(64) NOT NULL,
		status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
		invoice_number VARCHAR(64),
		created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
	);`

	_, err := db.Exec(query)
	return err
}

func main() {
	_ = godotenv.Load("../../.env")

	// 1. Koneksi ke PostgreSQL dan init DB
	dbHost := getEnv("POSTGRES_HOST", "localhost")
	dbUser := getEnv("POSTGRES_USER", "devuser")
	dbPass := getEnv("POSTGRES_PASSWORD", "devpassword")
	dbName := getEnv("POSTGRES_DB", "flashsale")

	dsn := fmt.Sprintf("postgres://%s:%s@%s:5432/%s?sslmode=disable", dbUser, dbPass, dbHost, dbName)
	db, err := sqlx.Connect("pgx", dsn)
	if err != nil {
		log.Fatalf("Gagal terhubung ke PostgreSQL: %v", err)
	}
	defer db.Close()
	fmt.Println("🚀 Worker berhasil terhubung ke PostgreSQL!")

	if err := initDatabase(db); err != nil {
		log.Fatalf("❌ Gagal membuat tabel orders: %v", err)
	}
	log.Println("✅ Tabel orders siap/terverifikasi.")

	// 2. Koneksi ke RabbitMQ
	rmqUser := getEnv("RABBITMQ_USER", "devuser")
	rmqPass := getEnv("RABBITMQ_PASS", "devpassword")
	rmqHost := getEnv("RABBITMQ_HOST", "localhost")

	rmqURL := fmt.Sprintf("amqp://%s:%s@%s:5672/", rmqUser, rmqPass, rmqHost)
	conn, err := amqp.Dial(rmqURL)
	if err != nil {
		log.Fatalf("Gagal terhubung ke RabbitMQ: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Gagal membuka channel RabbitMQ: %v", err)
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"flashsale_orders",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Gagal mendaftarkan queue: %v", err)
	}

	// Set Prefetch Count (Membatasi worker hanya mengambil 10 pesan sekaligus)
	err = ch.Qos(10, 0, false)
	if err != nil {
		log.Fatalf("Gagal set QOS: %v", err)
	}

	// 3. Mulai Consume Message dari Queue
	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack (Sengaja set FALSE agar manual Ack)
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		log.Fatalf("Gagal mendaftarkan consumer: %v", err)
	}

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			var msg OrderMessage
			err := json.Unmarshal(d.Body, &msg)
			if err != nil {
				log.Printf("Gagal parse JSON: %v", err)
				d.Nack(false, false) // Reject pesan tanpa requeue jika format busuk
				continue
			}

			// Simpan Transaksi ke PostgreSQL dengan ON CONFLICT
			query := `INSERT INTO orders (id, user_id, item_id, created_at) 
                      VALUES ($1, $2, $3, $4) 
                      ON CONFLICT (id) DO NOTHING`

			_, err = db.Exec(query, msg.OrderID, msg.UserID, msg.ItemID, msg.CreatedAt)
			if err != nil {
				log.Printf("Gagal menyimpan order %s ke DB: %v", msg.OrderID, err)
				d.Nack(false, true) // Re-queue hanya jika error DB (koneksi putus/down)
				continue
			}

			// Berikan ACK single ke RabbitMQ karena transaksi berhasil
			d.Ack(false)
			log.Printf("✅ [ORDER PROCESSED] Order ID: %s | User: %s | Item: %s tersimpan di PostgreSQL", msg.OrderID, msg.UserID, msg.ItemID)
		}
	}()

	fmt.Println(" [*] Worker mendengarkan antrean pesanan. Untuk keluar tekan CTRL+C")
	<-forever
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok && value != "" {
		return value
	}
	return fallback
}
