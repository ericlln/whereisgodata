package db

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"log"
	"sync"
)

type Postgres struct {
	Db *pgxpool.Pool
}

var (
	pgInstance *Postgres
	pgOnce     sync.Once
)

func NewPG(ctx context.Context, connString string) (*Postgres, error) {
	pgOnce.Do(func() {
		db, err := pgxpool.New(ctx, connString)
		if err != nil {
			return
		}
		pgInstance = &Postgres{db}
	})

	return pgInstance, nil
}

func Close(pg *Postgres) {
	pg.Db.Close()
}

type Redis struct {
	Client *redis.Client
}

var (
	redisInstance *Redis
	redisOnce     sync.Once
)

func NewRedis(connString string) (*Redis, error) {
	redisOnce.Do(func() {
		opt, err := redis.ParseURL(connString)
		if err != nil {
			log.Fatalln("Error parsing Redis connection URL")
		}

		client := redis.NewClient(opt)

		redisInstance = &Redis{client}
	})

	return redisInstance, nil
}

func CloseRedis(redis *Redis) {
	_ = redis.Client.Close()
}
