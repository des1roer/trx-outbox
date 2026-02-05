package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	_ "github.com/go-sql-driver/mysql"
)

// Config содержит конфигурацию приложения
type Config struct {
	DatabaseURL string
	KafkaBroker string
	KafkaTopic  string
	Faulty      float64
}

// OutboxMessage представляет запись из таблицы trx-outbox
type OutboxMessage struct {
	ID            int64
	AccountID     int64
	TransactionID string
	Payload       json.RawMessage
	Processed     bool
}

// DeliveryReportHandler обрабатывает результат доставки сообщения в Kafka
type DeliveryReportHandler struct {
	DB     *sql.DB
	MsgID  int64
	Ctx    context.Context
	Logger *log.Logger
}

func main() {
	// Загрузка конфигурации
	cfg := loadConfig()

	// Настройка логгера
	logger := log.New(os.Stdout, "", log.LstdFlags)
	logger.SetFlags(log.LstdFlags | log.Lmicroseconds)

	// Инициализация Kafka Producer
	producer, err := createKafkaProducer(cfg.KafkaBroker)
	if err != nil {
		logger.Fatalf("Failed to create Kafka producer: %v", err)
	}
	defer producer.Close()

	// Установка соединения с БД
	db, err := connectToMySQL(cfg.DatabaseURL, logger)
	if err != nil {
		logger.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Основной цикл обработки
	runOutboxProcessor(context.Background(), db, producer, cfg.KafkaTopic, logger)
}

// loadConfig загружает конфигурацию из переменных окружения
func loadConfig() Config {
	faulty, _ := strconv.ParseFloat(os.Getenv("FAULTY"), 64)
	if faulty == 0 {
		faulty = 0
	}

	return Config{
		DatabaseURL: os.Getenv("DATABASE_URL"),
		KafkaBroker: getEnv("KAFKA_BROKER", "localhost:9092"),
		KafkaTopic:  getEnv("KAFKA_TOPIC", "transactions"),
		Faulty:      faulty,
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// createKafkaProducer создает и настраивает Kafka producer
func createKafkaProducer(broker string) (*kafka.Producer, error) {
	config := &kafka.ConfigMap{
		"bootstrap.servers":   broker,
		"enable.idempotence":  true,
		"acks":                "all",
		"retries":             5,
		"linger.ms":           100,
		"batch.size":          16384,
		"go.delivery.reports": true,
	}

	return kafka.NewProducer(config)
}

// connectToMySQL устанавливает соединение с MySQL
func connectToMySQL(databaseURL string, logger *log.Logger) (*sql.DB, error) {
	var db *sql.DB
	var err error

	// Формат подключения для MySQL должен быть: user:password@tcp(host:port)/dbname
	// Если в DATABASE_URL другой формат, нужно его преобразовать
	mysqlDSN := convertToMySQLDSN(databaseURL)

	for i := 0; i < 30; i++ {
		db, err = sql.Open("mysql", mysqlDSN)
		if err != nil {
			logger.Printf("Failed to open database connection (attempt %d): %v", i+1, err)
			time.Sleep(time.Second)
			continue
		}

		// Проверяем соединение
		if err = db.Ping(); err != nil {
			logger.Printf("Failed to ping database (attempt %d): %v", i+1, err)
			db.Close()
			time.Sleep(time.Second)
			continue
		}

		// Настройки пула соединений
		db.SetMaxOpenConns(10)
		db.SetMaxIdleConns(5)
		db.SetConnMaxLifetime(5 * time.Minute)

		logger.Println("MySQL connection established")
		return db, nil
	}

	return nil, fmt.Errorf("failed to connect to MySQL after 30 attempts: %w", err)
}

// convertToMySQLDSN преобразует URL подключения в формат MySQL DSN
func convertToMySQLDSN(url string) string {
	// Если URL уже в формате MySQL, возвращаем как есть
	// Пример формата: "user:password@tcp(localhost:3306)/dbname?charset=utf8mb4&parseTime=true&loc=Local"
	if url != "" {
		return url
	}

	// Если переменная окружения не установлена, можно использовать дефолтные значения
	user := getEnv("MYSQL_USER", "root")
	password := getEnv("MYSQL_PASSWORD", "")
	host := getEnv("MYSQL_HOST", "localhost")
	port := getEnv("MYSQL_PORT", "3306")
	database := getEnv("MYSQL_DATABASE", "test")

	// Формируем DSN для MySQL
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", user, password, host, port, database)

	// Добавляем параметры соединения
	dsn += "?charset=utf8mb4&parseTime=true&loc=Local"

	return dsn
}

// runOutboxProcessor основной цикл обработки сообщений
func runOutboxProcessor(ctx context.Context, db *sql.DB, producer *kafka.Producer, topic string, logger *log.Logger) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Println("Shutting down trx-outbox processor")
			return
		case <-ticker.C:
			if err := processOutboxBatch(ctx, db, producer, topic, logger); err != nil {
				logger.Printf("Error processing trx-outbox batch: %v", err)
			}
		}
	}
}

// processOutboxBatch обрабатывает пакет сообщений из trx-outbox
func processOutboxBatch(ctx context.Context, db *sql.DB, producer *kafka.Producer, topic string, logger *log.Logger) error {
	// Начинаем транзакцию с уровнем изоляции READ COMMITTED
	tx, err := db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
	})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Получаем непрочитанные сообщения с блокировкой
	messages, err := fetchOutboxMessagesMySQL(ctx, tx)
	if err != nil {
		return fmt.Errorf("failed to fetch trx-outbox messages: %w", err)
	}

	if len(messages) == 0 {
		return nil // Нет сообщений для обработки
	}

	// Отправляем сообщения в Kafka
	for _, msg := range messages {
		if err := sendToKafka(ctx, producer, topic, msg, tx, logger); err != nil {
			return err
		}
	}

	// Подтверждаем транзакцию
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Ожидаем подтверждения доставки
	producer.Flush(5000)

	return nil
}

// fetchOutboxMessagesMySQL получает сообщения из trx-outbox для MySQL
func fetchOutboxMessagesMySQL(ctx context.Context, tx *sql.Tx) ([]OutboxMessage, error) {
	// Для MySQL используем FOR UPDATE для блокировки строк
	query := `
		SELECT id, account_id, transaction_id, payload 
		FROM trx-outbox 
		WHERE processed = FALSE 
		ORDER BY id 
		LIMIT 1 
		FOR UPDATE
	`

	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var messages []OutboxMessage
	for rows.Next() {
		var msg OutboxMessage
		// MySQL может возвращать account_id как int или string в зависимости от настроек
		// Используем sql.RawBytes для гибкости
		var accountIDRaw []byte
		if err := rows.Scan(&msg.ID, &accountIDRaw, &msg.TransactionID, &msg.Payload); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// Преобразуем account_id в int64
		accountID, err := strconv.ParseInt(string(accountIDRaw), 10, 64)
		if err != nil {
			// Если преобразование не удалось, пробуем сканировать напрямую
			if err := rows.Scan(&msg.ID, &msg.AccountID, &msg.TransactionID, &msg.Payload); err != nil {
				return nil, fmt.Errorf("failed to scan account_id: %w", err)
			}
		} else {
			msg.AccountID = accountID
		}

		messages = append(messages, msg)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}

	return messages, nil
}

// sendToKafka отправляет сообщение в Kafka
func sendToKafka(ctx context.Context, producer *kafka.Producer, topic string, msg OutboxMessage, tx *sql.Tx, logger *log.Logger) error {
	// Конвертируем payload в JSON
	payloadBytes, err := json.Marshal(msg.Payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	// Создаем канал для получения результата доставки
	deliveryChan := make(chan kafka.Event, 1)

	// Отправляем сообщение в Kafka
	err = producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(strconv.FormatInt(msg.AccountID, 10)),
		Value:          payloadBytes,
	}, deliveryChan)

	if err != nil {
		return fmt.Errorf("failed to produce message %d: %w", msg.ID, err)
	}

	// Ожидаем результат доставки
	select {
	case e := <-deliveryChan:
		m := e.(*kafka.Message)
		if m.TopicPartition.Error != nil {
			return fmt.Errorf("delivery failed for message %d: %v", msg.ID, m.TopicPartition.Error)
		}

		// Обновляем статус сообщения в БД
		if err := markMessageAsProcessed(ctx, tx, msg.ID); err != nil {
			return fmt.Errorf("failed to mark message %d as processed: %w", msg.ID, err)
		}

		logger.Printf("Message %d delivered to %s [%d] @ %d",
			msg.ID, *m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)

	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(30 * time.Second):
		return fmt.Errorf("delivery timeout for message %d", msg.ID)
	}

	return nil
}

// markMessageAsProcessed помечает сообщение как обработанное
func markMessageAsProcessed(ctx context.Context, tx *sql.Tx, msgID int64) error {
	query := "UPDATE trx-outbox SET processed = TRUE WHERE id = ?"

	result, err := tx.ExecContext(ctx, query, msgID)
	if err != nil {
		return fmt.Errorf("failed to execute update query: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("no rows updated for message ID %d", msgID)
	}

	return nil
}

// HandleDeliverySuccess обрабатывает успешную доставку сообщения
func (h *DeliveryReportHandler) HandleDeliverySuccess() {
	query := "UPDATE trx-outbox SET processed = TRUE WHERE id = ?"

	result, err := h.DB.ExecContext(h.Ctx, query, h.MsgID)
	if err != nil {
		h.Logger.Printf("Failed to mark message %d as processed: %v", h.MsgID, err)
		return
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		h.Logger.Printf("Failed to get rows affected for message %d: %v", h.MsgID, err)
		return
	}

	if rowsAffected == 0 {
		h.Logger.Printf("No rows updated for message %d", h.MsgID)
		return
	}

	h.Logger.Printf("Message %d delivered successfully and marked as processed", h.MsgID)
}
