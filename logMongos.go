package logMongos

import (
	"context"
	"log"
	"os"
	"time"

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

func (x Conn) addOneToBuffer(coll string, line LogLine) {
	log.Println("Adding an insertion to the buffer")
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
	log.Println("Adding an insertion to the buffer")
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

func findURI(db string) string {
	base := "mongodb+srv://fwmaster.5cnit.mongodb.net/" + db + "?authSource=%24external&authMechanism=MONGODB-X509&retryWrites=true&w=majority&tlsCertificateKeyFile="
	if _, err := os.Stat("/opt/code/cert/cert.pem"); os.IsNotExist(err) {
		return base + "/opt/code/cert/cert.pem"
	} else {
		p, _ := os.Getwd()
		return base + p + "/cert.pem"
	}
}

func NewConn(db string) *Conn {
	URI := findURI(db)
	return &Conn{db, URI, []Insertion{}}
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

func (x Conn) GetCollection(coll string) ([]LogLine, error) {
	client, ctx, cancel, err := getClient(x)
	if err != nil {
		return []LogLine{}, err
	}
	defer cancel()
	defer client.Disconnect(ctx)
	var lines []LogLine
	collection := client.Database(x.DB).Collection(coll)
	cursor, err := collection.Find(context.TODO(), bson.D{})
	if err != nil {
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

func (x Conn) GetLastLog(colName string) (time.Time, error) {
	client, ctx, cancel, err := getClient(x)
	if err != nil {
		return time.Time{}, err
	}
	defer cancel()
	defer client.Disconnect(ctx)
	coll := client.Database(x.DB).Collection(colName)
	cursor, err := coll.Aggregate(ctx, []bson.M{{"$sort": bson.M{"timeStamp": -1}}, {"$limit": 1}, {"$project": bson.M{"_id": 0}}})
	if err != nil {
		log.Println("Couldn't get aggregation result for the last log timestamp")
		return time.Time{}, err
	}
	var res LogLine
	for cursor.Next(ctx) {
		err := cursor.Decode(&res)
		if err != nil {
			log.Println("Couldn't get decode aggregation result for the last log timestamp")
			return time.Time{}, err
		}
	}
	return res.Timestamp, nil
}

func (x Conn) CountInTimeRange(colName string, text string, timestamp time.Time) (int64, error) {
	client, ctx, cancel, err := getClient(x)
	if err != nil {
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
		return 0, err
	}
	return count, nil
}

func getClient(x Conn) (*mongo.Client, context.Context, context.CancelFunc, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 1800*time.Second)
	clientOptions := options.Client().ApplyURI(x.URI)
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		cancel()
		client.Disconnect(ctx)
		return nil, nil, nil, err
	}
	return client, ctx, cancel, nil
}
