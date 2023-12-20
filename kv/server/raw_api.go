package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}

	val, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return nil, err
	}

	response := &kvrpcpb.RawGetResponse{
		Value:    val,
		NotFound: false, // if key notfound in DB, set NotFound as true
	}
	if val == nil {
		response.NotFound = true
	}

	return response, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	key, val, cf := req.Key, req.Value, req.Cf
	put := storage.Put{
		Key:   key,
		Value: val,
		Cf:    cf,
	}
	modify := storage.Modify{Data: put}

	err := server.storage.Write(req.Context, []storage.Modify{modify})
	if err != nil {
		return nil, err
	}

	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	del := storage.Delete{
		Key: req.Key,
		Cf:  req.Cf,
	}
	modify := storage.Modify{Data: del}
	err := server.storage.Write(req.Context, []storage.Modify{modify})
	if err != nil {
		return nil, err
	}

	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	var kvs []*kvrpcpb.KvPair
	startkey := req.StartKey

	sreader, err := server.storage.Reader(req.Context)
	if err != nil {
		return &kvrpcpb.RawScanResponse{}, err
	}

	it := sreader.IterCF(req.Cf)
	it.Seek(startkey)
	defer it.Close()

	for i := 0; i < int(req.Limit); i++ {
		if it.Valid() {
			item := it.Item()
			key := item.Key()
			val, err := item.Value()
			if err != nil {
				return &kvrpcpb.RawScanResponse{Kvs: kvs}, err
			} else {
				kvs = append(kvs, &kvrpcpb.KvPair{
					Key:   key,
					Value: val,
				})
			}

			it.Next()
		}
		// 上层测试不认为给的 Limit 超过迭代器范围算错误
		// } else {
		// 	err := errors.New("request's Limit is over iterator")
		// 	return &kvrpcpb.RawScanResponse{Kvs: kvs}, err
		// }
	}

	return &kvrpcpb.RawScanResponse{Kvs: kvs}, nil
}
