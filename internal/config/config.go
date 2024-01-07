package config

import (
	"github.com/joho/godotenv"
	"log"
	"os"
	"regexp"
)

const projectDirName = "whereisgodata"

func loadEnv() {
	projectName := regexp.MustCompile(`^(.*` + projectDirName + `)`)
	currentWorkDirectory, _ := os.Getwd()
	rootPath := projectName.Find([]byte(currentWorkDirectory))

	err := godotenv.Load(string(rootPath) + `/.env`)

	if err != nil {
		log.Fatalf("Error loading .env file")
	}
}

type Config struct {
	DatabaseUrl   string
	RedisUrl      string
	TransitApiKey string
}

func GetConfig() *Config {
	loadEnv()

	return &Config{
		DatabaseUrl:   os.Getenv("DATABASE_URL"),
		RedisUrl:      os.Getenv("REDIS_URL"),
		TransitApiKey: os.Getenv("TRANSIT_API_KEY"),
	}
}
