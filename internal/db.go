package db

import (
	"context"
	"flag"

	badger "github.com/dgraph-io/badger/v3"
	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"allen.gg/waterslide/internal/util"
)

// Whether the xDS resource version represents an ASCII integer (not actually part of the protocol).
// Normally, the version is just an arbitrary string.
var incrementingVersionScheme = flag.Bool("incrementing_versions", true, "if set to true, assumes that the xDS resource versions encountered are representations of integers and we will only update a resource if one with a \"newer\" verison is encountered")

type DatabaseHandle interface {
	Put(ctx context.Context, resource *discovery.Resource, typeURL string) error
	Get(ctx context.Context, resourceName string, typeURL string) (*discovery.Resource, error)
	GetAll(ctx context.Context, typeURL string) ([]*discovery.Resource, error)
	ForEach(ctx context.Context, fn func(k, v []byte) error, typeURL string)
}

type DatabaseHandleConfig struct {
	// Path to the database file. If one does not exist, it will be created.
	Filepath string

	Log *zap.SugaredLogger
}

type dbHandle struct {
	ctx context.Context

	// Handle for the BadgerDB.
	db *badger.DB

	log *zap.SugaredLogger
}

// Creates a database handle.
func NewDatabaseHandle(ctx context.Context, config DatabaseHandleConfig) (DatabaseHandle, error) {
	handle := &dbHandle{
		ctx: ctx,
		log: config.Log,
	}

	// TODO: Use actual options, not default. Ought to limit number of goroutines.

	var err error
	handle.db, err = badger.Open(badger.DefaultOptions(config.Filepath))
	return handle, err
}

// Makes the appropriate key syntax.
func makeKey(resourceName string, typeURL string) []byte {
	return []byte("//" + typeURL + "/" + resourceName)
}

// If there is an incrementing version scheme, returns "false" if the Put op is not for a newer
// resource. Otherwise, returns true.
func (handle *dbHandle) continueWithPutOp(
	ctx context.Context, tx *badger.Txn, key []byte, resource *discovery.Resource) (bool, error) {

	if !*incrementingVersionScheme {
		handle.log.Debugw("operating without incrementing version scheme")
		return true, nil
	}

	handle.log.Debugw("reading old value from db", "resource", resource.String())
	item, err := tx.Get(key)

	// If the key doesn't exist, we should definitely continue with the Put.
	if err == badger.ErrKeyNotFound {
		handle.log.Debugw("key not found", "key", key)
		return true, nil
	}

	if err != nil {
		handle.log.Errorw("error encountered during conditional write", "error", err.Error())
		return false, err
	}

	handle.log.Debugw("found key", "key", string(key))
	var existing discovery.Resource
	err = item.Value(func(val []byte) error {
		existing, err = util.CreateResourceFromBytes(val)
		return err
	})

	isNewer := util.IsNewerVersion(resource.GetVersion(), existing.GetVersion())
	handle.log.Debugw("@tallen", "is newer", isNewer, "resrouce", resource.String(), "existing", existing.String())
	return isNewer, err
}

// If 'incrementingVersionScheme' is true, this is a conditional write if the 'resource' is a newer
// version than what exists already in the DB. Otherwise, it's a straight-forward Put operation.
func (handle *dbHandle) Put(ctx context.Context, resource *discovery.Resource, typeURL string) error {
	key := makeKey(resource.GetName(), typeURL)
	return handle.db.Update(func(tx *badger.Txn) error {
		if cont, err := handle.continueWithPutOp(ctx, tx, key, resource); !cont {
			handle.log.Debugw("not continuing with put op", "error", err)
			return err
		}

		b, err := proto.Marshal(resource)
		if err != nil {
			handle.log.Errorw("error marshaling resource proto", "proto", resource.String(), "error", err.Error())
			return err
		}

		err = tx.Set(key, b)
		if err != nil {
			handle.log.Errorw("db write failed", "error", err.Error(), "key", string(key))
			return err
		}

		handle.log.Debugw("write attempt complete", "key", key)
		return err
	})
}

// Pulls a resource from the database by the given name/type. If the key does not exist, an error is
// returned (badger.ErrKeyNotFound).
func (handle *dbHandle) Get(ctx context.Context, resourceName string, typeURL string) (*discovery.Resource, error) {
	var res discovery.Resource
	key := makeKey(resourceName, typeURL)
	return &res, handle.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}

		valBytes, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		return proto.Unmarshal(valBytes, &res)
	})
}

// Gets all resources of a given type.
func (handle *dbHandle) GetAll(ctx context.Context, typeURL string) ([]*discovery.Resource, error) {
	all := []*discovery.Resource{}
	prefix := []byte("//" + typeURL + "/")

	handle.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			err := item.Value(func(val []byte) error {
				var res discovery.Resource
				err := proto.Unmarshal(val, &res)
				if err != nil {
					handle.log.Errorw("failed to unmarshal item", "error", err.Error())
					return err
				}

				all = append(all, &res)
				return nil
			})

			if err != nil {
				handle.log.Errorw("prefix scan failed", "prefix", prefix, "error", err.Error())
				return err
			}
		}

		return nil
	})

	return nil, nil
}

func (handle *dbHandle) ForEach(ctx context.Context, fn func(k, v []byte) error, typeURL string) {

}
