package watcher

import (
	"context"
	"io/ioutil"

	"github.com/golang/protobuf/jsonpb"

	"allen.gg/waterslide/internal/db"
	"allen.gg/waterslide/internal/util"
	"allen.gg/waterslide/pkg/server/protocol"

	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"github.com/fsnotify/fsnotify"
	"go.uber.org/zap"
)

type ResourceWatcher struct {
	ctx      context.Context
	dbhandle db.DatabaseHandle
	watcher  *fsnotify.Watcher
	log      *zap.SugaredLogger
	filepath string
}

func NewFilesystemResourceWatcher(ctx context.Context, log *zap.SugaredLogger, dbhandle db.DatabaseHandle) (*ResourceWatcher, error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Info("creating new fs resource watcher")
		return nil, err
	}

	rw := &ResourceWatcher{
		ctx:      ctx,
		watcher:  watcher,
		log:      log,
		dbhandle: dbhandle,
	}

	return rw, nil
}

func (rw *ResourceWatcher) readFromFile() error {
	rw.log.Info("handling filesystem write")

	data, err := ioutil.ReadFile(rw.filepath)
	if err != nil {
		rw.log.Errorw("error handling fs write", "error", err.Error())
		return err
	}

	var resources discovery.DiscoveryResponse
	err = jsonpb.UnmarshalString(string(data), &resources)
	if err != nil {
		rw.log.Errorw("error unmarshaling proto from file data", "error", err.Error(), "file_data", data)
		return err
	}

	for _, resourceAny := range resources.Resources {
		res, err := util.CreateResource(resourceAny)
		if err != nil {
			return err
		}

		rw.dbhandle.ConditionalPut(
			rw.ctx, protocol.CommonNamespace, res.GetResource().GetTypeUrl(), res,
			func(resourceVersion string) bool {
				return resourceVersion != res.GetVersion()
			})
	}

	return err
}

func (rw *ResourceWatcher) handleEvent(event fsnotify.Event) error {
	rw.log.Infow("handling event", "op", event.Op.String())

	var err error
	switch event.Op {
	case fsnotify.Write:
		fallthrough
	case fsnotify.Create:
		err = rw.readFromFile()
	default:
		rw.log.Infow("disregarding op", "op", event.Op)
	}

	return err
}

func (rw *ResourceWatcher) Start(filepath string) error {
	rw.log.Infow("starting resource watcher", "filepath", filepath)

	if rw.filepath != "" {
		rw.log.Fatalw("resource watcher already started", "prev_filepath", rw.filepath, "new_filepath", filepath)
	}

	err := rw.watcher.Add(filepath)
	if err != nil {
		rw.log.Errorw("failed to start resource watcher", "error", err.Error())
		return err
	}
	rw.filepath = filepath

	go func() {
		for {
			select {
			case event, ok := <-rw.watcher.Events:
				if !ok {
					return
				}
				rw.log.Infow("filesystem event received", "event", event)
				err = rw.handleEvent(event)
				if err != nil {
					rw.log.Errorw("error handling filesystem event", "error", err.Error())
					return
				}

			case err, ok := <-rw.watcher.Errors:
				if !ok {
					return
				}
				rw.log.Errorw("encountered watcher error", "error", err.Error())
			}
		}
	}()

	return rw.readFromFile()
}
