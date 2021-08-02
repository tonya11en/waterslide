package db

import (
	"context"
	"flag"

	"github.com/boltdb/bolt"
	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"allen.gg/waterslide/internal/util"
)

type DatabaseHandle interface {
	Put(resource *discovery.Resource, typeURL string) error
	Get(resourceName string, typeURL string) (*discovery.Resource, error)
	GetAll(typeURL string) ([]*discovery.Resource, error)
	ForEach(fn func(k, v []byte) error, typeURL string)
}

type DatabaseHandleConfig struct {
	// Path to the database file. If one does not exist, it will be created.
	Filepath string

	Log *zap.SugaredLogger
}

type dbHandle struct {
	ctx context.Context

	// Handle for the BoltDB.
	db *bolt.DB

	log *zap.SugaredLogger
}

func NewDatabaseHandle(ctx context.Context, config DatabaseHandleConfig) (DatabaseHandle, error) {
	var err error

	if config.Log == nil {
		l, err := zap.NewDevelopment()
		if err != nil {
			panic(err.Error())
		}
		config.Log = l.Sugar()
	}

	handle := &dbHandle{
		ctx: ctx,
		log: config.Log,
	}

	handle.db, err = bolt.Open(config.Filepath, 0600, nil)
	return handle, err
}

var incrementingVersionScheme = flag.Bool("incrementing_versions", true, "if set to true, assumes that the xDS resource versions encountered are representations of integers and we will only update a resource if one with a \"newer\" verison is encountered")

func (handle *dbHandle) Put(resource *discovery.Resource, typeURL string) error {
	name := resource.GetName()
	return handle.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(typeURL))
		if err != nil {
			handle.log.Error("error creating when conditionally creating bucket", "error", err.Error())
			return err
		}

		// If using the incrementing version scheme, we'll want to conditionally write the new entry.
		if *incrementingVersionScheme {
			handle.log.Debugw("reading old value from db", "resource_name", name)
			valBytes := bucket.Get([]byte(name))
			existing, err := util.CreateResourceFromBytes(valBytes)
			if err != nil || util.IsNewerVersion(resource.GetVersion(), existing.GetVersion()) {
				handle.log.Debugw("put operation did not result in a mutation", "error", err.Error(), "resource_name", name)
				return err
			}
		}

		b, err := proto.Marshal(resource)
		if err != nil {
			handle.log.Errorw("error marshaling resource proto", "proto", resource.String(), "error", err.Error())
			return err
		}

		err = bucket.Put([]byte(name), b)
		handle.log.Debugw("write attempt complete", "resource_name", name, "type", resource.GetVersion())
		return err
	})
}

func (handle *dbHandle) Get(resourceName string, typeURL string) (*discovery.Resource, error) {
	var res discovery.Resource

	err := handle.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(typeURL))
		valBytes := bucket.Get([]byte(resourceName))
		return proto.Unmarshal(valBytes, &res)
	})

	return &res, err
}

func (handle *dbHandle) GetAll(typeURL string) ([]*discovery.Resource, error) {
	return nil, nil

}

func (handle *dbHandle) ForEach(fn func(k, v []byte) error, typeURL string) {

}
