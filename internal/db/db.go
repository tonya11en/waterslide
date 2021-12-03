package db

import (
	"bytes"
	"context"
	"fmt"

	badger "github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/pb"
	"github.com/dgraph-io/ristretto/z"
	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	flatbuffers "github.com/google/flatbuffers/go"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"allen.gg/waterslide/internal/db/flatbuffers/waterslide_bufs"
)

const (
	// Flatbuffer builders' initial buffer size.
	initialBufferSizeBytes = 1024

	// The number of goroutines to use for iteration during streaming scans.
	streamGoroutineCount = 16

	// Pre-allocated capacity for the scan array.
	preallocatedScanCapacity = 4096
)

type DatabaseHandle interface {
	// Puts a resource into the DB. Returns the GSN of the operation.
	Put(ctx context.Context, namespace string, typeURL string, resource *discovery.Resource) (uint64, error)

	// Conditionally puts a resource into the DB if |fn| returns true. Returns the GSN of the
	// operation if mutated and 0 if there was no mutation.
	ConditionalPut(ctx context.Context, namespace string, typeURL string, resource *discovery.Resource, fn func(resourceVersion string) bool) (uint64, error)

	// Gets a resource.
	Get(ctx context.Context, namespace string, typeURL string, resourceName string) (*waterslide_bufs.Resource, error)

	// Gets all resources of a particular type.
	GetAll(ctx context.Context, namespace string, typeURL string) ([]*waterslide_bufs.Resource, error)

	// Subscriptions.
	WildcardSubscribe(ctx context.Context, namespace string, typeURL string, cb func(key string, fbuf *waterslide_bufs.Resource) error)
	ResourceSubscribe(ctx context.Context, namespace string, typeURL string, resourceName string, cb func(key string, fbuf *waterslide_bufs.Resource) error)
}

type DatabaseHandleConfig struct {
	// Path to the database file. If one does not exist, it will be created.
	Filepath string

	Log *zap.SugaredLogger

	// If true, no mutations will be persisted to disk.
	InMemoryMode bool
}

// Implementation of the DatabaseHandle interface.
type dbHandle struct {
	ctx    context.Context
	db     *badger.DB
	gsnSeq *badger.Sequence
	log    *zap.SugaredLogger
}

// Creates a database handle. If the DB file is an empty string, the database will be configured as
// in-memory only.
func NewDatabaseHandle(ctx context.Context, config DatabaseHandleConfig) (DatabaseHandle, error) {
	handle := &dbHandle{
		ctx: ctx,
		log: config.Log,
	}

	// TODO: Use actual options, not default. Ought to limit number of goroutines.

	var err error
	opts := badger.DefaultOptions(config.Filepath)
	opts.InMemory = config.InMemoryMode
	handle.db, err = badger.Open(opts)
	if err != nil {
		config.Log.Errorw("failed to open db", "error", err.Error())
		return nil, err
	}

	handle.gsnSeq, err = handle.db.GetSequence([]byte("gsn"), 1000)
	if err != nil {
		config.Log.Errorw("failed to get gsn sequence", "error", err.Error())
		return nil, err
	}

	// Let's burn the first GSN, since we rely on a GSN of 0 to indicate a mutation did not occur for
	// a conditional write.
	_, err = handle.gsnSeq.Next()
	if err != nil {
		handle.log.Errorw("unable to get next gsn", "error", err.Error())
		return nil, err
	}

	return handle, err
}

// Makes the appropriate key syntax.
//
// Syntax: //<namespace>/<type URL>/<name>
func makeKey(namespace string, typeURL string, resourceName string) []byte {
	var b bytes.Buffer
	b.Grow(len(namespace) + len(typeURL) + len(resourceName) + 4)
	b.WriteString(fmt.Sprintf("//%s/%s/%s", namespace, typeURL, resourceName))
	return b.Bytes()
}

func makePrefix(namespace string, typeURL string) []byte {
	var b bytes.Buffer
	b.Grow(len(namespace) + len(typeURL) + 4)
	b.WriteString(fmt.Sprintf("//%s/%s/", namespace, typeURL))
	return b.Bytes()
}

func (handle *dbHandle) Put(ctx context.Context, namespace string, typeURL string, resource *discovery.Resource) (uint64, error) {
	return handle.ConditionalPut(ctx, namespace, typeURL, resource, nil)
}

func (handle *dbHandle) ConditionalPut(
	ctx context.Context,
	namespace string,
	typeURL string,
	resource *discovery.Resource,
	fn func(resourceVersion string) bool) (uint64, error) {

	key := makeKey(namespace, typeURL, resource.GetName())
	handle.log.Debugw("starting conditional put", "key", string(key), "resource", resource.String())

	// Create the resource fbuffer builder.
	builder := flatbuffers.NewBuilder(initialBufferSizeBytes)

	b, err := proto.Marshal(resource)
	if err != nil {
		handle.log.Errorw("error marshaling resource proto", "proto", resource.String(), "error", err.Error())
		return 0, err
	}

	gsn := uint64(0)

	err = handle.db.Update(func(tx *badger.Txn) error {
		if fn != nil {
			var mutate bool
			found := true

			item, err := tx.Get(key)
			if err == badger.ErrKeyNotFound {
				mutate = true
				found = false
			} else if err != nil {
				handle.log.Errorw("error reading val", "key", string(key), "error", err.Error())
				return err
			}

			if found {
				err = item.Value(func(val []byte) error {
					fres := waterslide_bufs.GetRootAsResource(val, 0)
					v := string(fres.Version())
					mutate = fn(v) || v == ""
					return nil
				})
				if err != nil {
					handle.log.Errorw("error reading val", "key", key, "error", err.Error())
					return err
				}
			}

			// Condition did not pass, so no PUT will occur.
			if !mutate {
				handle.log.Debugw("not mutating", "gsn", gsn)
				return nil
			}
		}

		handle.log.Debugw("creating flatbuffer")

		// Set the serialized resource value and version.
		bv := builder.CreateByteString(b)
		v := builder.CreateString(resource.GetVersion())
		waterslide_bufs.ResourceStart(builder)
		waterslide_bufs.ResourceAddResourceProto(builder, bv)
		waterslide_bufs.ResourceAddVersion(builder, v)

		// Create the unique GSN.
		gsn, err = handle.gsnSeq.Next()
		if err != nil {
			handle.log.Errorw("unable to get next gsn", "error", err.Error())
			return err
		}
		waterslide_bufs.ResourceAddGsn(builder, gsn)
		re := waterslide_bufs.ResourceEnd(builder)
		builder.Finish(re)

		handle.log.Debugw("created flatbuffer", "gsn", gsn)

		// Set the flatbuffer value.
		err = tx.Set(key, builder.FinishedBytes())
		if err != nil {
			handle.log.Errorw("db write failed", "error", err.Error(), "key", string(key))
			return err
		}

		handle.log.Debugw("write attempt complete",
			"key", string(key),
			"gsn", gsn,
			"bytes_written", len(builder.FinishedBytes()))
		return err
	})

	handle.log.Debugw("conditional write complete", "error", err)

	return gsn, err
}

// Pulls a resource from the database by the given name/type. If the key does not exist, nil resource is returned.
func (handle *dbHandle) Get(ctx context.Context, namespace string, typeURL string, resourceName string) (*waterslide_bufs.Resource, error) {
	var resFlatbuf *waterslide_bufs.Resource
	key := makeKey(namespace, typeURL, resourceName)

	err := handle.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		handle.log.Debugw("starting GET transaction", "key", string(key), "item", item, "error", err)
		if err != nil {
			return err
		}

		valBytes, err := item.ValueCopy(nil)
		if err != nil {
			handle.log.Errorw("error encountered when copying value in GET op", "error", err.Error())
			return err
		}

		resFlatbuf = waterslide_bufs.GetRootAsResource(valBytes, 0)
		return nil
	})
	if err == badger.ErrKeyNotFound {
		handle.log.Debugw("key not found for GET operation", "key", string(key))
		return nil, nil
	}

	return resFlatbuf, err
}

func (handle *dbHandle) GetAll(ctx context.Context, namespace string, typeURL string) ([]*waterslide_bufs.Resource, error) {
	return handle.getAllStreaming(ctx, namespace, typeURL)
}

// A Scan operation that uses iterator streaming.
func (handle *dbHandle) getAllStreaming(ctx context.Context, namespace string, typeURL string) ([]*waterslide_bufs.Resource, error) {
	// Pre-allocating slots for extra perf. This may not be necessary.
	// TODO: Benchmark this and see if it matters.
	all := make([]*waterslide_bufs.Resource, 0, preallocatedScanCapacity)

	stream := handle.db.NewStream()
	stream.NumGo = streamGoroutineCount
	stream.Prefix = makePrefix(namespace, typeURL)

	handle.log.Debugw("performing streaming prefix GET", "prefix", string(stream.Prefix))

	// Send is called serially while Stream.Orchestrate is running, so we don't need to lock |all|.
	stream.Send = func(buf *z.Buffer) error {
		kvlist, err := badger.BufferToKVList(buf)
		if err != nil {
			return err
		}

		for _, kv := range kvlist.GetKv() {
			all = append(all, waterslide_bufs.GetRootAsResource(kv.GetValue(), 0))
		}

		return nil
	}

	// Run the stream
	err := stream.Orchestrate(ctx)
	if err != nil {
		handle.log.Errorw("error orchestrating streaming scan", "error", err.Error())
	}
	return all, err
}

func (handle *dbHandle) subscribeInternal(ctx context.Context, prefix []byte, cb func(key string, fbuf *waterslide_bufs.Resource) error) {
	match := pb.Match{
		Prefix: prefix,
	}
	go handle.db.Subscribe(ctx, func(kvl *badger.KVList) error {
		for _, kv := range kvl.GetKv() {
			key := make([]byte, len(kv.GetKey()))
			copy(key, kv.GetKey())
			val := make([]byte, len(kv.GetValue()))
			copy(val, kv.GetValue())
			resFlatbuf := waterslide_bufs.GetRootAsResource(val, 0)
			err := cb(string(key), resFlatbuf)
			if err != nil {
				handle.log.Errorw("error during subscriber callback", "error", err.Error())
				return err
			}
		}
		return nil
	}, []pb.Match{match})
}

func (handle *dbHandle) WildcardSubscribe(ctx context.Context, namespace string, typeURL string, cb func(key string, fbuf *waterslide_bufs.Resource) error) {
	prefix := makePrefix(namespace, typeURL)
	handle.subscribeInternal(ctx, prefix, cb)
}

func (handle *dbHandle) ResourceSubscribe(ctx context.Context, namespace string, typeURL string, resourceName string, cb func(key string, fbuf *waterslide_bufs.Resource) error) {
	key := makeKey(namespace, typeURL, resourceName)
	handle.subscribeInternal(ctx, key, cb)
}
