package main

import (
	"context"
	"github.com/ericlln/whereisgo/server/pkg/config"
	"github.com/ericlln/whereisgo/server/pkg/db"
	"github.com/ericlln/whereisgodata/internal/importdata"
	"github.com/robfig/cron"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ctx := context.Background()
	cfg := config.GetConfig("whereisgodata")

	pg, err := db.NewPG(ctx, cfg.DatabaseUrl)
	if err != nil {
		log.Fatal("Error creating database connection")
	}

	redis, err := db.NewRedis(cfg.RedisUrl)
	if err != nil {
		log.Fatal("Error creating Redis connection")
	}

	c := cron.New()

	err = c.AddFunc("@monthly", func() {
		//importdata.UpdateStaticData(pg)
	})
	if err != nil {
		log.Fatalf("Error adding CRON job: %s", err)
	}

	err = c.AddFunc("@every 10s", func() { importdata.GetRealTimeData(redis, pg) })
	if err != nil {
		log.Fatalf("Error adding CRON job: %s", err)
	}
	c.Start()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	select {
	case <-signals:
		log.Println("Received interrupt signal")
		c.Stop()
		db.Close(pg)
		db.CloseRedis(redis)
	}

	log.Println("Program terminated")
}
