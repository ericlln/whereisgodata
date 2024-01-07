package importdata

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/ericlln/whereisgodata/internal/config"
	"github.com/ericlln/whereisgodata/internal/db"
	"github.com/ericlln/whereisgodata/internal/limiter"
	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
	"io"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"
)

var (
	transitApiKey = ""
)

func init() {
	transitApiKey = config.GetConfig().TransitApiKey
	if transitApiKey == "" {
		log.Println("TransitApiKey could not be retrieved from config")
	}
}

type CompleteStation struct {
	LocationCode string
	StationName  string
	StationType  int
	Lat          float64
	Lng          float64
	City         string
	ZoneCode     int
}

type AllStationsResponse struct {
	Stations struct {
		Station []Station
	}
}

type Station struct {
	LocationCode string `json:"LocationCode"`
	PublicStopID string `json:"PublicStopID"`
	LocationName string `json:"LocationName"`
	LocationType string `json:"LocationType"`
}

type StationDetailsResponse struct {
	Stop struct {
		ZoneCode  string `json:"ZoneCode"`
		City      string `json:"City"`
		Latitude  string `json:"Latitude"`
		Longitude string `json:"Longitude"`
	}
}

type StationType int

const (
	BusStop StationType = iota
	BusTerminal
	ParkRide
	TrainBusStation
	TrainStation
)

var stationTypeToString = map[StationType]string{
	BusStop:         "Bus Stop",
	BusTerminal:     "Bus Terminal",
	ParkRide:        "Park & Ride",
	TrainBusStation: "Train & Bus Station",
	TrainStation:    "Train Station",
}

func getStationType(stationType string) int {
	for st, str := range stationTypeToString {
		if str == stationType {
			return int(st)
		}
	}
	return -1
}

func updateAllStops(pg *db.Postgres) {
	url := fmt.Sprintf("http://api.openmetrolinx.com/OpenDataAPI/api/V1/Stop/All?key=%s", transitApiKey)
	resp, err := http.Get(url)
	if err != nil {
		log.Printf("Error fetching from %s \n", url)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Println(err)
	}

	var allStationsResponse AllStationsResponse
	err = json.Unmarshal(body, &allStationsResponse)
	if err != nil {
		log.Println("Error parsing JSON")
	}

	stations := allStationsResponse.Stations.Station

	ch := make(chan CompleteStation)
	var wg sync.WaitGroup

	go func() {
		defer close(ch)
		ticker := limiter.NewOutgoingLimiter(time.Second / 10) // 10 requests per second

		for _, station := range stations {
			wg.Add(1)
			go getCompleteStation(ch, &wg, station, transitApiKey)
			ticker.WaitForOutgoingLimiter()
		}
		wg.Wait()
	}()

	batch := &pgx.Batch{}
	query := `INSERT INTO stations (location_code, station_name, station_type, lat, lng, city, zone_code) 
							VALUES (@locationCode, @stationName, @stationType, @lat, @lng, @city, @zoneCode)
							ON CONFLICT (location_code)
							DO UPDATE SET station_name = @stationName, station_type = @stationType, lat = @lat, lng = @lng, city = @city, zone_code = @zoneCode`

	for station := range ch {
		args := pgx.NamedArgs{
			"locationCode": station.LocationCode,
			"stationName":  station.StationName,
			"stationType":  station.StationType,
			"lat":          station.Lat,
			"lng":          station.Lng,
			"city":         station.City,
			"zoneCode":     station.ZoneCode,
		}
		log.Println("Queued", station)
		batch.Queue(query, args)
	}

	results := pg.Db.SendBatch(context.Background(), batch)

	defer func(results pgx.BatchResults) {
		err := results.Close()
		if err != nil {
			log.Println("Error closing batch results: ", err)
		}
	}(results)

	errorCount := 0
	for i := 0; i < batch.Len(); i++ {
		_, err := results.Exec()

		if err != nil {
			log.Println("Error inserting row", err)
			errorCount++
			continue
		}
	}
	log.Println(batch.Len()-errorCount, "rows were successfully inserted into {stations}")
}

func getCompleteStation(ch chan<- CompleteStation, wg *sync.WaitGroup, station Station, transitApiKey string) {
	log.Println("Getting location information for", station)
	defer wg.Done()

	url := fmt.Sprintf("http://api.openmetrolinx.com/OpenDataAPI/api/V1/Stop/Details/%s?key=%s", station.LocationCode, transitApiKey)
	resp, err := http.Get(url)
	if err != nil {
		log.Println("Error fetching station with code ", station.LocationCode)
		return
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Println("Error reading API response: ", err)
		return
	}

	var stationDetailsResponse StationDetailsResponse
	err = json.Unmarshal(body, &stationDetailsResponse)
	if err != nil {
		log.Println("Error parsing API response: ", err)
		return
	}

	locate := stationDetailsResponse.Stop
	lat, _ := strconv.ParseFloat(locate.Latitude, 64)
	lng, _ := strconv.ParseFloat(locate.Longitude, 64)
	zoneCode, _ := strconv.Atoi(locate.ZoneCode)

	ch <- CompleteStation{
		LocationCode: station.LocationCode,
		StationName:  station.LocationName,
		StationType:  getStationType(station.LocationType),
		Lat:          lat,
		Lng:          lng,
		City:         locate.City,
		ZoneCode:     zoneCode,
	}
}

// UpdateStaticData updates transit data that is not expected to change frequently (i.e. stations, etc.)
func UpdateStaticData(pg *db.Postgres) {
	startTime := time.Now()
	log.Println("UpdateStaticData was started")

	updateAllStops(pg)

	log.Println("UpdateStaticData was completed in: ", time.Since(startTime))
}

type BusesGlanceResponse struct {
	Trips struct {
		Trip []Trip `json:"Trip"`
	} `json:"Trips"`
}

type Trip struct {
	BusType       string  `json:"BusType"`
	TripNumber    string  `json:"TripNumber"`
	StartTime     string  `json:"StartTime"`
	EndTime       string  `json:"EndTime"`
	LineCode      string  `json:"LineCode"`
	RouteNumber   string  `json:"RouteNumber"`
	VariantDir    string  `json:"VariantDir"`
	Display       string  `json:"Display"`
	Latitude      float64 `json:"Latitude"`
	Longitude     float64 `json:"Longitude"`
	IsInMotion    bool    `json:"IsInMotion"`
	DelaySeconds  int     `json:"DelaySeconds"`
	Course        float64 `json:"Course"`
	FirstStopCode string  `json:"FirstStopCode"`
	LastStopCode  string  `json:"LastStopCode"`
	PrevStopCode  string  `json:"PrevStopCode"`
	NextStopCode  string  `json:"NextStopCode"`
	AtStationCode string  `json:"AtStationCode"`
	ModifiedDate  string  `json:"ModifiedDate"`
}

type Position struct {
	Lat       float32
	Lng       float32
	Course    int32
	Timestamp int64
}

func estToUnix(t time.Time) int64 {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		fmt.Println("Error loading location: ", err)
		return -1
	}

	return time.Date(
		t.Year(),
		t.Month(),
		t.Day(),
		t.Hour(),
		t.Minute(),
		t.Day(),
		t.Second(),
		loc).Unix()
}

func updateBusLocations(r *db.Redis) {
	ctx := context.Background()
	url := fmt.Sprintf("http://api.openmetrolinx.com/OpenDataAPI/api/V1/ServiceataGlance/Buses/All?key=%s", transitApiKey)

	resp, err := http.Get(url)
	if err != nil {
		log.Printf("Error fetching from %s \n", url)
		return
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Error reading response body: %s \n", err)
		return
	}

	var feed BusesGlanceResponse
	err = json.Unmarshal(body, &feed)
	if err != nil {
		log.Printf("Error parsing response body: %s \n", err)
		return
	}

	var positions []*redis.GeoLocation
	_, err = r.Client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, trip := range feed.Trips.Trip {
			if trip.IsInMotion == false {
				continue
			}

			parsedTime, err := time.Parse("2006-01-02 15:04:05", trip.ModifiedDate)
			if err != nil {
				log.Printf("Error parsing date: %s \n", err)
				continue
			}

			positions = append(positions, &redis.GeoLocation{
				Name:      trip.TripNumber,
				Latitude:  trip.Latitude,
				Longitude: trip.Longitude,
			})

			pos := &Position{
				Lat:       float32(trip.Latitude),
				Lng:       float32(trip.Longitude),
				Course:    int32(trip.Course),
				Timestamp: estToUnix(parsedTime),
			}

			posJson, err := json.Marshal(pos)
			if err != nil {
				log.Printf("Error encoding into JSON: %s \n", err)
				continue
			}

			err = pipe.Set(ctx, trip.TripNumber, posJson, 0).Err()
			if err != nil {
				log.Printf("Error updating key: %s \n", err)
				continue
			}
		}
		return nil
	})

	err = r.Client.GeoAdd(ctx, "locates", positions...).Err()
	if err != nil {
		log.Printf("Unable to update locates: %s \n", err)
	}
}

// GetRealTimeData updates locations of vehicles, designed to be run every couple of seconds
func GetRealTimeData(redis *db.Redis) {
	startTime := time.Now()
	log.Println("GetRealTimeData was started")

	updateBusLocations(redis)

	log.Println("GetRealTimeData was completed in: ", time.Since(startTime))
}
