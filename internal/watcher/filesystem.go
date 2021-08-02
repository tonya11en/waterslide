package watcher

import (
	"io/ioutil"

	"github.com/golang/protobuf/jsonpb"

	"allen.gg/waterslide/pkg/server/util"

	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"github.com/fsnotify/fsnotify"
	"go.uber.org/zap"
)

type ResourceWatcher struct {
	resources      discovery.DiscoveryResponse
	consumerStream chan *discovery.Resource

	watcher  *fsnotify.Watcher
	log      *zap.SugaredLogger
	filepath string
}

func NewFilesystemResourceWatcher(log *zap.SugaredLogger, consumerStream chan *discovery.Resource) (*ResourceWatcher, error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Info("creating new fs resource watcher")
		return nil, err
	}

	rw := &ResourceWatcher{
		watcher:        watcher,
		log:            log,
		consumerStream: consumerStream,
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

	err = jsonpb.UnmarshalString(string(data), &rw.resources)
	if err != nil {
		rw.log.Errorw("error unmarshaling proto from file data", "error", err.Error(), "file_data", data)
		return err
	}

	for _, resourceAny := range rw.resources.Resources {
		res, err := util.CreateResource(resourceAny)
		if err != nil {
			return err
		}
		rw.consumerStream <- &res
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
