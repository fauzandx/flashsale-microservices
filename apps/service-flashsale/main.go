package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
)

var (
	ctx         = context.Background()
	rdb         *redis.Client
	amqpChannel *amqp.Channel
)

var flashSaleLuaScript = redis.NewScript(`
	local stockKey = KEYS[1]
	local currentStock = tonumber(redis.call('GET', stockKey) or "0")

	if currentStock <= 0 then
		return -1
	end

	return redis.call('DECR', stockKey)
`)

type BuyRequest struct {
	UserID string `json:"user_id" binding:"required"`
	ItemID string `json:"item_id" binding:"required"`
}

type OrderMessage struct {
	OrderID   string    `json:"order_id"`
	UserID    string    `json:"user_id"`
	ItemID    string    `json:"item_id"`
	CreatedAt time.Time `json:"created_at"`
}

func main() {
	_ = godotenv.Load("../../.env")

	// Baca host dari Environment Variable
	redisHost := getEnv("REDIS_HOST", "localhost")
	redisPort := getEnv("REDIS_PORT", "6379")
	rabbitUser := getEnv("RABBITMQ_USER", "devuser")
	rabbitPass := getEnv("RABBITMQ_PASS", "devpassword")
	rabbitHost := getEnv("RABBITMQ_HOST", "localhost")

	// 1. Inisialisasi Redis
	rdb := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:%s", redisHost, redisPort),
	})
	if _, err := rdb.Ping(ctx).Result(); err != nil {
		log.Fatalf("Gagal terhubung ke Redis: %v", err)
	}
	fmt.Println("🚀 Berhasil terhubung ke Redis!")

	// 2. Inisialisasi RabbitMQ
	rabbitURL := fmt.Sprintf("amqp://%s:%s@%s:5672/", rabbitUser, rabbitPass, rabbitHost)
	conn, err := amqp.Dial(rabbitURL)
	if err != nil {
		log.Fatalf("Gagal terhubung ke RabbitMQ: %v", err)
	}
	defer conn.Close()

	amqpChannel, err = conn.Channel()
	if err != nil {
		log.Fatalf("Gagal membuka channel RabbitMQ: %v", err)
	}
	defer amqpChannel.Close()

	// Deklarasi Queue
	_, err = amqpChannel.QueueDeclare(
		"flashsale_orders", // nama queue
		true,               // durable
		false,              // auto-delete
		false,              // exclusive
		false,              // no-wait
		nil,                // arguments
	)
	if err != nil {
		log.Fatalf("Gagal mendeklarasikan queue: %v", err)
	}
	fmt.Println("🚀 Berhasil terhubung ke RabbitMQ Queue: flashsale_orders!")

	// 3. Gin Router
	r := gin.Default()

	r.GET("/health", func(c *gin.Context) {
		// Cek koneksi Redis
		redisStatus := "up"
		if err := rdb.Ping(c.Request.Context()).Err(); err != nil {
			redisStatus = "down"
		}

		// Cek status keseluruhan
		status := "OK"
		httpCode := http.StatusOK
		if redisStatus == "down" {
			status = "DEGRADED"
			httpCode = http.StatusServiceUnavailable
		}

		c.JSON(httpCode, gin.H{
			"status":    status,
			"service":   "service-flashsale",
			"redis":     redisStatus,
			"timestamp": time.Now().UTC(),
		})
	})

	r.POST("/api/flashsale/seed", func(c *gin.Context) {
		itemID := c.Query("item_id")
		stock := c.DefaultQuery("stock", "10")

		if itemID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "item_id wajib diisi"})
			return
		}

		key := fmt.Sprintf("flashsale:stock:%s", itemID)
		err := rdb.Set(ctx, key, stock, 0).Err()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"message": "Stok flash sale berhasil di-set!",
			"item_id": itemID,
			"stock":   stock,
		})
	})

	r.GET("/api/flashsale/stock", func(c *gin.Context) {
		itemID := c.Query("item_id")
		if itemID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "item_id parameter required"})
			return
		}

		key := fmt.Sprintf("flashsale:stock:%s", itemID)
		val, err := rdb.Get(c.Request.Context(), key).Result()

		// Tangani jika key belum ada di Redis
		if err == redis.Nil {
			c.JSON(http.StatusOK, gin.H{
				"item_id": itemID,
				"stock":   0,
				"message": "Stock not initialized",
			})
			return
		} else if err != nil {
			log.Printf("Error fetching stock from Redis: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch stock"})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"item_id": itemID,
			"stock":   val,
		})
	})

	r.POST("/api/flashsale/checkout", func(c *gin.Context) {
		var req BuyRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Payload tidak valid"})
			return
		}

		stockKey := fmt.Sprintf("flashsale:stock:%s", req.ItemID)

		// Atomic Decrement di Redis
		res, err := flashSaleLuaScript.Run(ctx, rdb, []string{stockKey}).Result()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal memproses transaksi"})
			return
		}

		remainingStock := res.(int64)
		if remainingStock < 0 {
			c.JSON(http.StatusConflict, gin.H{
				"status":  "failed",
				"message": "Maaf, stok barang flash sale sudah habis!",
			})
			return
		}

		// Kirim Payload Order ke RabbitMQ Queue
		orderPayload := OrderMessage{
			OrderID:   fmt.Sprintf("ORD-%s", uuid.New().String()),
			UserID:    req.UserID,
			ItemID:    req.ItemID,
			CreatedAt: time.Now(),
		}

		body, _ := json.Marshal(orderPayload)

		err = amqpChannel.PublishWithContext(
			ctx,
			"",                 // exchange
			"flashsale_orders", // routing key / queue name
			false,              // mandatory
			false,              // immediate
			amqp.Publishing{
				ContentType: "application/json",
				Body:        body,
			},
		)

		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal mengirim pesanan ke antrean"})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"status":          "success",
			"message":         "Berhasil mengamankan item! Pesanan sedang diproses.",
			"order_id":        orderPayload.OrderID,
			"remaining_stock": remainingStock,
		})
	})

	r.Run(":8081")
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}
