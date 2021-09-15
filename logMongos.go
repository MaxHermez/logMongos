package logMongos

import (
	"context"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// Conn ...class object of mongo connection
type Conn struct {
	DB     string
	URI    string
	OPTS   *options.ClientOptions
	BUFFER []Insertion
}

type Insertion struct {
	Collection string
	Lines      []LogLine
	Attempts   int16
}

type LogLine struct {
	Timestamp time.Time `json:"Timestamp"`
	Filename  string    `json:"Filename"`
	Function  string    `json:"Function"`
	Line      int       `json:"Line"`
	Level     string    `json:"Level"`
	Message   string    `json:"Message"`
}

type HubsGroupCount struct {
	Name  string `json:"Name"`
	Count int    `json:"Count"`
}

func (x Conn) addOneToBuffer(coll string, line LogLine) {
	log.Trace("Adding an insertion to the buffer")
	for i, ins := range x.BUFFER {
		if ins.Collection == coll {
			x.BUFFER[i].Lines = append(x.BUFFER[i].Lines, line)
			if x.BUFFER[i].Attempts != 0 {
				x.BUFFER[i].Attempts--
			}
			return
		}
	}
	x.BUFFER = append(x.BUFFER, Insertion{coll, []LogLine{line}, 0})
}

func (x Conn) addManyToBuffer(coll string, lines []LogLine) {
	log.Trace("Adding an insertion to the buffer")
	for i, ins := range x.BUFFER {
		if ins.Collection == coll {
			x.BUFFER[i].Lines = append(x.BUFFER[i].Lines, lines...)
			if x.BUFFER[i].Attempts >= int16(len(lines)) {
				x.BUFFER[i].Attempts -= int16(len(lines))
			} else {
				x.BUFFER[i].Attempts = 0
			}
			return
		}
	}
	x.BUFFER = append(x.BUFFER, Insertion{coll, lines, 0})
}

func (x Conn) emptyBuffer() {
	client, ctx, cancel, err := getClient(x)
	if err != nil {
		log.Fatal(err)
	}
	defer cancel()
	defer client.Disconnect(ctx)
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		log.Fatal(err)
	}
	var collection *mongo.Collection
	for i, each := range x.BUFFER {
		collection = client.Database(x.DB).Collection(each.Collection)
		var posts []interface{}
		for _, line := range each.Lines {
			posts = append(posts, line)
		}
		_, err := collection.InsertMany(ctx, posts)
		if err != nil {
			x.BUFFER[i].Attempts++
			if x.BUFFER[i].Attempts >= 10 {
				// if the attempts counter reaches 10 we remove the element from the buffer
				x.BUFFER[i] = x.BUFFER[len(x.BUFFER)-1]
				x.BUFFER[len(x.BUFFER)-1] = Insertion{}
				x.BUFFER = x.BUFFER[:len(x.BUFFER)-1]
			}
			log.Fatal(err)
		}
		println("successfully inserted from buffer into the '" + each.Collection + "' collection")
		x.BUFFER[i] = x.BUFFER[len(x.BUFFER)-1]
		x.BUFFER[len(x.BUFFER)-1] = Insertion{}
		x.BUFFER = x.BUFFER[:len(x.BUFFER)-1]
	}
}

func parseURI(shards string, replica string, db string) string {
	uri := "mongodb://" + shards + "/" + db + "ssl=true&replicaSet=" + replica + "&authSource=%24external&authMechanism=MONGODB-X509&retryWrites=true&w=majority&tlsCertificateKeyFile=/etc/ssl/certs/mongocert.pem"
	return uri
}

func getTLSconf(original *tls.Config) *tls.Config {
	cwd, _ := os.Getwd()
	path := ""
	// check if it's a Windows or Linux URI
	if strings.Contains(cwd, "\\") {
		path = cwd + "\\cert.pem"
	} else {
		path = "/etc/ssl/certs/mongocert.pem"
	}
	if _, err := os.Stat(path); err != nil {
		log.Fatalf("Could not find the cert.pem file i the CWD, nor in the /etc/ssl/certs directory")
		panic("Please copy a certificate file from mongoDB into the CWD and rename it to mongocert.pem")
	} else {
		data, err := ioutil.ReadFile(path)
		if err != nil {
			panic("Failed to open the mongocert.pem file!")
		}
		success := original.RootCAs.AppendCertsFromPEM(data)
		if !success {
			panic("Failed to parse ca certificate as PEM encoded content!")
		}
		return original
	}
}

func NewConn(shards string, replica string, db string) *Conn {
	log.SetLevel(log.TraceLevel)
	URI := parseURI(shards, replica, db)
	clientOptions := options.Client().ApplyURI(URI)
	PEM := getTLSconf(clientOptions.TLSConfig)
	clientOptions.SetTLSConfig(PEM)
	return &Conn{db, URI, clientOptions, []Insertion{}}
}

// InsertPost ...(column, timestamp, level, message) into the DB
func (x Conn) InsertPost(coll string, line LogLine) {
	client, ctx, cancel, err := getClient(x)
	if err != nil {
		log.Fatal(err)
	}
	defer cancel()
	defer client.Disconnect(ctx)
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		log.Fatal(err)
	}
	// line.Timestamp = line.Timestamp.Format("2006-01-02T15:04:05.999Z07:00")
	collection := client.Database(x.DB).Collection(coll)
	println("posting")
	insertResult, err := collection.InsertOne(ctx, line)
	if err != nil {
		x.addOneToBuffer(coll, line)
		log.Fatal(err)
	} else {
		println("insertion complete!", insertResult.InsertedID)
		x.emptyBuffer()
	}
}

// InsertPosts ...(column, timestamp, level, message) into the DB
func (x Conn) InsertPosts(coll string, lines []LogLine) {
	client, ctx, cancel, err := getClient(x)
	if err != nil {
		log.Fatal(err)
	}
	defer cancel()
	defer client.Disconnect(ctx)
	var posts []interface{}
	for _, line := range lines {
		// bsonPost := bson.D{{Key: "Timestamp", Value: line.Timestamp.Format("2006-01-02T15:04:05.999Z07:00")}, {Key: "Level", Value: line.Level}, {Key: "Message", Value: line.Message}}
		posts = append(posts, line)
	}
	collection := client.Database(x.DB).Collection(coll)
	println("posting")
	insertResult, err := collection.InsertMany(ctx, posts)
	if err != nil {
		x.addManyToBuffer(coll, lines)
		log.Fatal(err)
	} else {
		println("insertion complete!", insertResult.InsertedIDs)
		x.emptyBuffer()
	}
}

func (x Conn) GetCollectionRetry(coll string, attempts int) ([]LogLine, error) {
	counter := 0
	var err error
	for counter < attempts {
		res, err := x.GetCollection(coll)
		if err == nil {
			return res, nil
		}
		counter++
	}
	return []LogLine{}, err
}

func (x Conn) GetCollection(coll string) ([]LogLine, error) {
	client, ctx, cancel, err := getClient(x)
	if err != nil {
		res, err := x.GetCollectionRetry(coll, 2)
		if err == nil {
			return res, nil
		}
		return []LogLine{}, err
	}
	defer cancel()
	defer client.Disconnect(ctx)
	var lines []LogLine
	collection := client.Database(x.DB).Collection(coll)
	cursor, err := collection.Find(context.TODO(), bson.D{})
	if err != nil {
		if err != mongo.ErrNoDocuments {
			res, err := x.GetCollectionRetry(coll, 3)
			if err == nil {
				return res, nil
			}
		}
		return []LogLine{}, err
	}
	defer cursor.Close(ctx)
	var count int
	for cursor.Next(ctx) {
		count = count + 1
		var res LogLine
		err := cursor.Decode(&res)
		lines = append(lines, res)
		if err != nil {
			return []LogLine{}, err
		}
	}
	return lines, nil
}

func (x Conn) GetCollections() ([]string, error) {
	client, ctx, cancel, err := getClient(x)
	if err != nil {
		return []string{}, err
	}
	defer cancel()
	defer client.Disconnect(ctx)
	colls, err := client.Database(x.DB).ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return []string{}, err
	}
	return colls, nil
}

func (x Conn) GetTotalJobs(hub string, db string) (int64, error) {
	filter := bson.D{}
	if hub != "" {
		filter = bson.D{{Key: "jobHub", Value: hub}}
	}
	client, ctx, cancel, err := getClient(x)
	if err != nil {
		return 0, err
	}
	defer cancel()
	defer client.Disconnect(ctx)
	tot := int64(0)
	if db == "" || db == "Datapool" {
		tot, err = client.Database("Datapool").Collection("jobEntries").CountDocuments(ctx, filter)
	} else if db == "MasterJD" {
		tot, err = client.Database(db).Collection("Tier1").CountDocuments(ctx, filter)
	}
	if err != nil {
		return 0, err
	}
	return tot, nil
}

func (x Conn) GetTotalLinks(hub string) (int64, error) {
	client, ctx, cancel, err := getClient(x)
	if err != nil {
		return 0, err
	}
	defer cancel()
	defer client.Disconnect(ctx)
	tot := int64(0)
	if hub == "" {
		tot, err = client.Database("Datapool").Collection("jobLinks").CountDocuments(ctx, bson.D{})
	} else {
		tot, err = client.Database("Datapool").Collection("jobLinks").CountDocuments(ctx, bson.D{{Key: "jobHub", Value: hub}})
	}
	if err != nil {
		return 0, err
	}
	return tot, nil
}

func (x Conn) CountJobsInPeriod(since string, hub string) (int64, error) {
	client, ctx, cancel, err := getClient(x)
	if err != nil {
		return 0, err
	}
	defer cancel()
	defer client.Disconnect(ctx)
	tot := int64(0)
	if hub == "" {
		tot, err = client.Database("Datapool").Collection("jobEntries").CountDocuments(ctx, bson.D{{
			Key: "Date of posting",
			Value: bson.D{{
				Key:   "$gt",
				Value: since,
			}},
		}})
	} else {
		tot, err = client.Database("Datapool").Collection("jobEntries").CountDocuments(ctx, bson.D{{
			Key: "Date of posting",
			Value: bson.D{{
				Key:   "$gt",
				Value: since,
			}}}, {
			Key:   "jobHub",
			Value: hub,
		},
		})
	}
	if err != nil {
		fmt.Print(since)
		return 0, err
	}
	return tot, nil
}

func (x Conn) FindOne(colName string, lvl string, text string) (LogLine, error) {
	client, ctx, cancel, err := getClient(x)
	if err != nil {
		return LogLine{}, err
	}
	defer cancel()
	defer client.Disconnect(ctx)
	coll := client.Database(x.DB).Collection(colName)
	opts := options.FindOne().SetSort(bson.D{{Key: "timestamp", Value: -1}})
	var result LogLine
	err = coll.FindOne(context.TODO(), bson.D{{
		Key: "message",
		Value: bson.D{{
			Key:   "$regex",
			Value: primitive.Regex{Pattern: text, Options: "i"},
		}},
	}}, opts).Decode(&result)
	if err != nil {
		return LogLine{}, err
	}
	return result, nil
}

func (x Conn) CountInTimeRangeRetry(colName string, text string, timeStamp time.Time, attempts int) (int64, error) {
	counter := 0
	var err error
	for counter < attempts {
		res, err := x.CountInTimeRange(colName, text, timeStamp)
		if err == nil {
			return res, nil
		}
		counter++
	}
	return 0, err
}

func (x Conn) CountInTimeRange(colName string, text string, timestamp time.Time) (int64, error) {
	client, ctx, cancel, err := getClient(x)
	if err != nil {
		res, err := x.CountInTimeRangeRetry(colName, text, timestamp, 2)
		if err == nil {
			return res, nil
		}
		return 0, err
	}
	defer cancel()
	defer client.Disconnect(ctx)
	coll := client.Database(x.DB).Collection(colName)
	dateFilter := bson.D{{
		Key:   "$gt",
		Value: timestamp,
	}}
	var count int64
	if text == "" {
		count, err = coll.CountDocuments(context.TODO(), bson.D{{
			Key:   "timestamp",
			Value: dateFilter,
		}})
	} else {
		count, err = coll.CountDocuments(context.TODO(), bson.M{
			"timestamp": dateFilter,
			"message":   primitive.Regex{Pattern: text, Options: "i"},
		})
	}
	if err != nil {
		res, err := x.CountInTimeRangeRetry(colName, text, timestamp, 2)
		if err == nil {
			return res, nil
		}
		return 0, err
	}
	return count, nil
}

func (x Conn) GetHubsCountRetry(colName string, since string, attempts int) ([]HubsGroupCount, error) {
	counter := 0
	var err error
	for counter < attempts {
		res, err := x.GetHubsCount(colName, since)
		if err == nil {
			return res, nil
		}
		counter++
	}
	return []HubsGroupCount{}, err
}

func (x Conn) GetHubsCount(col string, since string) ([]HubsGroupCount, error) {
	var pipe []bson.M
	if since == "" {
		pipe = []bson.M{{"$group": bson.M{"_id": "$jobHub", "Count": bson.M{"$sum": 1}}}, {"$project": bson.M{"_id": 0, "Name": "$_id", "Count": 1}}}
	} else {
		pipe = []bson.M{{"$match": bson.M{"Date of posting": bson.M{"$gt": since}}}, {"$group": bson.M{"_id": "$jobHub", "Count": bson.M{"$sum": 1}}}, {"$project": bson.M{"_id": 0, "Name": "$_id", "Count": 1}}}
	}
	client, ctx, cancel, err := getClient(x)
	if err != nil {
		resp, err := x.GetHubsCountRetry(col, since, 2)
		if err == nil {
			return resp, nil
		}
		return []HubsGroupCount{}, err
	}
	defer cancel()
	defer client.Disconnect(ctx)
	var out []HubsGroupCount
	var coll *mongo.Collection
	if col == "Tier1" {
		coll = client.Database("MasterJD").Collection(col)
	} else if col == "jobEntries" || col == "jobLinks" {
		coll = client.Database("Datapool").Collection(col)
	}
	curs, err := coll.Aggregate(ctx, pipe)
	if err != nil {
		log.Println("Couldn't get aggregation result for the hubs count in " + col)
		return []HubsGroupCount{}, err
	}
	err = curs.All(ctx, &out)
	if err != nil {
		log.Println("Couldn't get decode aggregation result for the last log timestamp")
		return []HubsGroupCount{}, err
	}
	// for curs.Next(ctx) {
	// 	log.Println("Here")
	// 	var res HubsGroupCount
	// 	err := curs.Decode(&res)
	// 	out = append(out, res)
	// 	if err != nil {

	// 	}
	// }
	return out, nil
}

func getClient(x Conn) (*mongo.Client, context.Context, context.CancelFunc, error) {
	ctx, cancel := context.WithCancel(context.Background())

	client, err := mongo.Connect(context.TODO(), x.OPTS)
	if err != nil {
		log.Error("Failed to get mongodb client, error message: " + err.Error())
		cancel()
		client.Disconnect(ctx)
		return nil, nil, nil, err
	}
	return client, ctx, cancel, nil
}
