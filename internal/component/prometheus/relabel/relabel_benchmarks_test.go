package relabel

import (
	"net"
	"testing"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/go-redis/redis"
	"github.com/grafana/alloy/internal/component"
	alloy_relabel "github.com/grafana/alloy/internal/component/common/relabel"
	"github.com/grafana/alloy/internal/component/prometheus"
	"github.com/grafana/alloy/internal/service/cache"
	"github.com/grafana/alloy/internal/service/labelstore"
	"github.com/ory/dockertest"
	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/relabel"
	"github.com/prometheus/prometheus/storage"
)

func BenchmarkLRU(b *testing.B) {
	b.StopTimer()

	ls := labelstore.New(nil, prom.DefaultRegisterer)
	fanout := prometheus.NewInterceptor(nil, ls, prometheus.WithAppendHook(func(ref storage.SeriesRef, l labels.Labels, _ int64, _ float64, _ storage.Appender) (storage.SeriesRef, error) {
		return ref, nil
	}))

	relabeller, _ := New(component.Options{
		ID:             "1",
		Logger:         nil,
		OnStateChange:  func(e component.Exports) {},
		Registerer:     prom.NewRegistry(),
		GetServiceData: getServiceData,
	}, Arguments{
		ForwardTo: []storage.Appendable{fanout},
		MetricRelabelConfigs: []*alloy_relabel.Config{
			{
				SourceLabels: []string{"__address__"},
				Regex:        alloy_relabel.Regexp(relabel.MustNewRegexp("(.+)")),
				TargetLabel:  "new_label",
				Replacement:  "new_value",
				Action:       "replace",
			},
		},
		InMemoryCacheSizeDeprecated: 100_000,
	})

	lbls := labels.FromStrings("__address__", "localhost")

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		relabeller.relabel(float64(i), lbls)
	}
}

func BenchmarkRedis(b *testing.B) {
	b.StopTimer()

	addr, destroyFunc := startRedis(b)
	defer destroyFunc()

	ls := labelstore.New(nil, prom.DefaultRegisterer)
	fanout := prometheus.NewInterceptor(nil, ls, prometheus.WithAppendHook(func(ref storage.SeriesRef, l labels.Labels, _ int64, _ float64, _ storage.Appender) (storage.SeriesRef, error) {
		return ref, nil
	}))

	relabeller, _ := New(component.Options{
		ID:             "1",
		Logger:         nil,
		OnStateChange:  func(e component.Exports) {},
		Registerer:     prom.NewRegistry(),
		GetServiceData: getServiceData,
	}, Arguments{
		ForwardTo: []storage.Appendable{fanout},
		MetricRelabelConfigs: []*alloy_relabel.Config{
			{
				SourceLabels: []string{"__address__"},
				Regex:        alloy_relabel.Regexp(relabel.MustNewRegexp("(.+)")),
				TargetLabel:  "new_label",
				Replacement:  "new_value",
				Action:       "replace",
			},
		},
		CacheConfig: cache.CacheConfig{
			Backend: cache.Redis,
			Redis: cache.RedisConf{
				Endpoint:            []string{addr}, //"172.17.0.1:6379"},
				Username:            "default",
				MaxAsyncBufferSize:  10000,
				MaxAsyncConcurrency: 10,
			},
		},
	})

	lbls := labels.FromStrings("__address__", "localhost")

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		relabeller.relabel(float64(i), lbls)
	}
}

func BenchmarkMemcached(b *testing.B) {
	b.StopTimer()

	addr, destroyFunc := startMemcached(b)
	defer destroyFunc()

	ls := labelstore.New(nil, prom.DefaultRegisterer)
	fanout := prometheus.NewInterceptor(nil, ls, prometheus.WithAppendHook(func(ref storage.SeriesRef, l labels.Labels, _ int64, _ float64, _ storage.Appender) (storage.SeriesRef, error) {
		return ref, nil
	}))

	relabeller, _ := New(component.Options{
		ID:             "1",
		Logger:         nil,
		OnStateChange:  func(e component.Exports) {},
		Registerer:     prom.NewRegistry(),
		GetServiceData: getServiceData,
	}, Arguments{
		ForwardTo: []storage.Appendable{fanout},
		MetricRelabelConfigs: []*alloy_relabel.Config{
			{
				SourceLabels: []string{"__address__"},
				Regex:        alloy_relabel.Regexp(relabel.MustNewRegexp("(.+)")),
				TargetLabel:  "new_label",
				Replacement:  "new_value",
				Action:       "replace",
			},
		},
		CacheConfig: cache.CacheConfig{
			Backend: cache.Memcached,
			Memcached: cache.MemcachedConfig{
				Addresses:            []string{addr},
				WriteBufferSizeBytes: 1500,
				ReadBufferSizeBytes:  1500,
				MaxAsyncBufferSize:   100,
				MaxAsyncConcurrency:  2,
			},
		},
	})

	lbls := labels.FromStrings("__address__", "localhost")

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		relabeller.relabel(float64(i), lbls)
	}
}

func startRedis(t *testing.B) (string, func()) {
	t.Helper()
	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Fatalf("Failed to start Dockertest: %+v", err)
	}

	resource, err := pool.Run("redis", "alpine3.20", nil)
	if err != nil {
		t.Fatalf("Failed to start redis: %+v", err)
	}
	addr := net.JoinHostPort("localhost", resource.GetPort("6379/tcp"))

	err = pool.Retry(func() error {
		var e error
		client := redis.NewClient(&redis.Options{Addr: addr})
		defer client.Close()

		_, e = client.Ping().Result()
		return e
	})

	if err != nil {
		t.Fatalf("Failed to ping Redis: %+v", err)
	}

	destroyFunc := func() {
		pool.Purge(resource)
	}

	return addr, destroyFunc
}

func startMemcached(t *testing.B) (string, func()) {
	t.Helper()

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Fatalf("Failed to start Dockertest: %+v", err)
	}

	resource, err := pool.Run("memcached", "latest", nil)
	if err != nil {
		t.Fatalf("Failed to start memcached: %+v", err)
	}

	addr := net.JoinHostPort("localhost", resource.GetPort("11211/tcp"))

	err = pool.Retry(func() error {
		var e error
		client := memcache.New(addr)
		e = client.Ping()
		return e
	})

	if err != nil {
		t.Fatalf("Failed to ping Redis: %+v", err)
	}

	destroyFunc := func() {
		pool.Purge(resource)
	}

	return addr, destroyFunc
}
