// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 BoanLab @ DKU

package mongodb

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	protobuf "kubecarto/protobuf"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// DBHandler Structure
type DBHandler struct {
	client *mongo.Client
	cancel context.CancelFunc

	database      *mongo.Database
	deploys       *mongo.Collection
	pods          *mongo.Collection
	services      *mongo.Collection
	apiLogCol     *mongo.Collection
	evyMetricsCol *mongo.Collection
}

// dbHandler for Global Reference
var dbHandler DBHandler

// NewMongoDBHandler Function
func NewMongoDBHandler(mongoDBAddr string) (*DBHandler, error) {
	var err error

	// Create a MongoDB client
	dbHandler.client, err = mongo.NewClient(options.Client().ApplyURI(mongoDBAddr))
	if err != nil {
		msg := fmt.Sprintf("[MongoDB] Unable to initialize a MongoDB client (%s): %v", mongoDBAddr, err)
		return nil, errors.New(msg)
	}

	// Set timeout (10 sec)
	var ctx context.Context
	ctx, dbHandler.cancel = context.WithTimeout(context.Background(), 10*time.Second)

	// Connect to the MongoDB server
	err = dbHandler.client.Connect(ctx)
	if err != nil {
		msg := fmt.Sprintf("[MongoDB] Unable to connect the mongoDB server (%s): %v", mongoDBAddr, err)
		return nil, errors.New(msg)
	}

	// Create 'KubeCarto' database
	dbHandler.database = dbHandler.client.Database("KubeCarto")

	// Create APILogs and Metrics collections
	dbHandler.deploys = dbHandler.database.Collection("Deploys")
	dbHandler.pods = dbHandler.database.Collection("Pods")
	dbHandler.services = dbHandler.database.Collection("Services")
	dbHandler.apiLogCol = dbHandler.database.Collection("APILogs")
	dbHandler.evyMetricsCol = dbHandler.database.Collection("EnvoyMetrics")

	return &dbHandler, nil
}

// Disconnect Function
func (handler *DBHandler) Disconnect() {
	err := handler.client.Disconnect(context.Background())
	if err != nil {
		log.Printf("[MongoDB] Unable to properly disconnect: %v", err)
	}
}

// InsertAPILog Function
func (handler *DBHandler) InsertAPILog(data *protobuf.APILog) error {
	_, err := handler.apiLogCol.InsertOne(context.Background(), data)
	return err
}

// InsertEnvoyMetrics Function
func (handler *DBHandler) InsertEnvoyMetrics(data *protobuf.EnvoyMetrics) error {
	_, err := handler.evyMetricsCol.InsertOne(context.Background(), data)
	return err
}

// InsertDeploy Function
func (handler *DBHandler) InsertDeploy(dep *protobuf.Deploy) error {
	_, err := handler.deploys.InsertOne(context.Background(), dep)
	return err
}

// UpdateDeploy Function
func (handler *DBHandler) UpdateDeploy(dep *protobuf.Deploy) error {
	filter := bson.M{
		"cluster":   dep.Cluster,
		"namespace": dep.Namespace,
		"name":      dep.Name,
	}
	update := bson.M{"$set": dep}

	// Upsert 옵션 세팅
	opts := options.Update().SetUpsert(true)

	_, err := handler.deploys.UpdateOne(context.Background(), filter, update, opts)
	return err
}

// DeleteDeploy Function
func (handler *DBHandler) DeleteDeploy(dep *protobuf.Deploy) error {
	filter := bson.M{
		"cluster":   dep.Cluster,
		"namespace": dep.Namespace,
		"name":      dep.Name,
	}
	_, err := handler.deploys.DeleteOne(context.Background(), filter)
	return err
}

// InsertPod Function
func (handler *DBHandler) InsertPod(pod *protobuf.Pod) error {
	_, err := handler.pods.InsertOne(context.Background(), pod)
	return err
}

// UpdatePod Function
func (handler *DBHandler) UpdatePod(pod *protobuf.Pod) error {
	filter := bson.M{
		"cluster":   pod.Cluster,
		"namespace": pod.Namespace,
		"name":      pod.Name,
	}
	update := bson.M{"$set": pod}

	opts := options.Update().SetUpsert(true)

	_, err := handler.pods.UpdateOne(context.Background(), filter, update, opts)
	return err
}

// DeletePod Function
func (handler *DBHandler) DeletePod(pod *protobuf.Pod) error {
	filter := bson.M{
		"cluster":   pod.Cluster,
		"namespace": pod.Namespace,
		"name":      pod.Name,
	}
	_, err := handler.pods.DeleteOne(context.Background(), filter)
	return err
}

// InsertService Function
func (handler *DBHandler) InsertService(svc *protobuf.Service) error {
	_, err := handler.services.InsertOne(context.Background(), svc)
	return err
}

// UpdateService Function
func (handler *DBHandler) UpdateService(svc *protobuf.Service) error {
	filter := bson.M{
		"cluster":   svc.Cluster,
		"namespace": svc.Namespace,
		"name":      svc.Name,
	}
	update := bson.M{"$set": svc}

	opts := options.Update().SetUpsert(true)

	_, err := handler.services.UpdateOne(context.Background(), filter, update, opts)
	return err
}

// DeleteService Function
func (handler *DBHandler) DeleteService(svc *protobuf.Service) error {
	filter := bson.M{
		"cluster":   svc.Cluster,
		"namespace": svc.Namespace,
		"name":      svc.Name,
	}
	_, err := handler.services.DeleteOne(context.Background(), filter)
	return err
}
