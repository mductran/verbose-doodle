package main

import (
	"context"
	"fmt"
	"github.com/karrick/godirwalk"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"gocv.io/x/gocv"
	"image"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"
)

const PaginationLimit = 1000

type PathHash struct {
	Path string `bson:"path,omitempty"`
	Md5  string `bson:"md5,omitempty"`
}

func hash(path string) string {
	img := gocv.IMRead(path, gocv.IMReadGrayScale)
	if img.Empty() {
		return ""
	}

	newSize := image.Rectangle{
		Min: image.Point{
			X: 0,
			Y: 0,
		},
		Max: image.Point{
			X: 32,
			Y: 32,
		},
	}
	gocv.Resize(img, &img, newSize.Size(), 0, 0, gocv.InterpolationLinear)
	if img.Channels() != 1 {
		gocv.CvtColor(img, &img, gocv.ColorBGRToGray)
	}
	img.ConvertTo(&img, gocv.MatTypeCV32FC1)
	gocv.DCT(img, &img, gocv.DftForward)

	newSize.Max.X = 8
	newSize.Max.Y = 8

	dctBlock := img.Region(newSize)
	dctAverage := float32(dctBlock.Mean().Val1)*float32(64) - dctBlock.GetFloatAt(0, 0)
	dctAverage = dctAverage / float32(64)

	var out string
	for i := 0; i < 8; i++ {
		for j := 0; j < 8; j++ {
			if dctBlock.GetFloatAt(i, j) < dctAverage {
				out += "0"
			} else {
				out += "1"
			}
		}
	}

	return out
}

func getDocCount(collection *mongo.Collection) int64 {
	docCount, err := collection.CountDocuments(context.TODO(), bson.D{})
	if err != nil {
		return -1
	}
	return docCount
}

func connect() *mongo.Client {
	connectionString := fmt.Sprintf("mongodb+srv://ductran:%s@inkling-cluster.jnpkxro.mongodb.net/?tls=true", os.Getenv("atlaspwd"))
	serverApi := options.ServerAPI(options.ServerAPIVersion1)
	opts := options.Client().ApplyURI(connectionString).SetServerAPIOptions(serverApi)
	client, err := mongo.Connect(context.TODO(), opts)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	return client
}

func consume(batch []string, wg *sync.WaitGroup, col *mongo.Collection) {
	defer wg.Done()
	var pathHash []interface{}
	for _, path := range batch {
		pathHash = append(pathHash, PathHash{
			Path: path,
			Md5:  hash(path),
		})
	}
	_, err := col.InsertMany(context.TODO(), pathHash)
	if err != nil {
		fmt.Println(err)
	}
}

func insert() {
	var ImageExt = []string{
		".jpg", ".jpeg", ".png",
	}
	var Root = "/home/noel/Madokami/"
	var BatchSize = 10000

	client := connect()
	database := client.Database("PathHash")
	collection := database.Collection("pathhash1")

	var wg sync.WaitGroup

	var batch []string

	start := time.Now()
	err := godirwalk.Walk(Root, &godirwalk.Options{
		Callback: func(osPathname string, de *godirwalk.Dirent) error {
			// Following string operation is not most performant way
			// of doing this, but common enough to warrant a simple
			// example here:
			if slices.Contains(ImageExt, filepath.Ext(osPathname)) {
				batch = append(batch, osPathname)
				if len(batch) == BatchSize {
					wg.Add(1)
					go consume(batch, &wg, collection)
					batch = batch[:0]
				}
			}
			return nil
		},
		Unsorted: true, // (optional) set true for faster yet non-deterministic enumeration (see godoc)
	})
	if err != nil {
		fmt.Println(err)
	}
	wg.Wait()
	fmt.Println(time.Since(start))
}

func getPagination(page, limit int, collection *mongo.Collection) *mongo.Cursor {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()

	skip := int64(page*limit - limit)
	l := int64(limit)
	opts := options.FindOptions{Limit: &l, Skip: &skip}
	cursor, err := collection.Find(ctx, bson.D{}, &opts)
	if err != nil {
		return nil
	}
	return cursor
}

func hamming(s1, s2 string) int {
	var distance = 0
	for i := 0; i < len(s1); i++ {
		if s1[i] != s2[i] {
			distance += 1
		}
	}
	return distance
}

func search(cursor *mongo.Cursor, key string, distance, batchSize int) int {
	// read data from cursor into a slice
	var batch = make([]PathHash, batchSize)
	var resultSet []string
	if err := cursor.All(context.TODO(), &batch); err != nil {
		return -1
	}

	// search
	for _, i := range batch {
		if hamming(i.Md5, key) <= distance {
			resultSet = append(resultSet, i.Md5)
		}
	}
	return len(resultSet)
}

func sequentialBatch() {
	client := connect()
	collection := client.Database("PathHash").Collection("pathhash1")

	docCount := getDocCount(collection)
	pageCount := int(docCount / PaginationLimit)

	key := "1111111100000000100000001001111101001111101000000011001001011110" // city of walls 006

	counter := 0

	total := time.Now()
	for i := 1; i <= pageCount; i++ {
		//fmt.Println("page ", i)
		//start := time.Now()
		cursor := getPagination(i, PaginationLimit, collection)
		counter += search(cursor, key, 7, PaginationLimit)
		//fmt.Println(time.Since(start))
		//fmt.Printf("\n=====\n")
	}
	fmt.Println(counter)
	fmt.Println("total time: ", time.Since(total))
}

func concurrentBatch() {
	client := connect()
}

func main() {
	sequentialBatch()
}
