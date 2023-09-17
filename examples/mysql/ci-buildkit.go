package main

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/linclin/fastflow"
	mysqlKeeper "github.com/linclin/fastflow/keeper/mysql"
	"github.com/linclin/fastflow/pkg/entity"
	"github.com/linclin/fastflow/pkg/exporter"
	"github.com/linclin/fastflow/pkg/mod"
	mysqlStore "github.com/linclin/fastflow/store/mysql"
	"gorm.io/gorm"
)

func ensureDagCreated() error {
	dag := &entity.Dag{
		BaseInfo: entity.BaseInfo{
			ID: "ci-buildkit",
		},
		Name: "ci-buildkit",
		Vars: entity.DagVars{
			"var": {DefaultValue: "default-var"},
		},
		Status: entity.DagStatusNormal,
		Tasks: []entity.Task{
			{ID: "task1", ActionName: "http", Params: map[string]interface{}{
				"method": "GET",
				"url":    "http://127.0.0.1:9090/metrics",
			}, TimeoutSecs: 60},
			// {ID: "task2", ActionName: "ssh", DependOn: []string{"task1"}, Params: map[string]interface{}{
			// 	"user":    "lc",
			// 	"ip":      "172.17.115.244",
			// 	"key":     "id_rsa",
			// 	"cmd":     "touch 111",
			// 	"timeout": 5,
			// }, TimeoutSecs: 10},
			// {ID: "task2", ActionName: "go-git", DependOn: []string{"task1"}, Params: map[string]interface{}{
			// 	"git-url": "https://gitee.com/linclin/go-gin-rest-api.git",
			// 	//"git-token":    "15600ecc0f105f9a0d8dbad651d5cb2a",
			// 	"git-username": "lc13579443@qq.com",
			// 	"git-password": "xxxxx",
			// 	"git-branch":   "develop",
			// }, TimeoutSecs: 120},
			{ID: "task3", ActionName: "go-git", DependOn: []string{"task1"}, Params: map[string]interface{}{
				"git-url":    "git@gitee.com:linclin/go-gin-rest-api.git",
				"git-key":    "id_rsa",
				"git-branch": "develop",
			}, TimeoutSecs: 600},
			{ID: "task4", ActionName: "buildkit", DependOn: []string{"task3"}, Params: map[string]interface{}{
				"git-url":         "git@gitee.com:linclin/go-gin-rest-api.git",
				"image":           "registry.cn-shenzhen.aliyuncs.com/dev-ops/go-gin-rest-api",
				"version":         "1.0.0",
				"registry-secret": "acr-regcred",
				"cluster":         "rke2",
				"namespace":       "default",
			}, TimeoutSecs: 600},
		},
	}
	oldDag, err := mod.GetStore().GetDag(dag.ID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			if err := mod.GetStore().CreateDag(dag); err != nil {
				return err
			}
		} else {
			return err
		}
	}
	if oldDag != nil {
		if err := mod.GetStore().UpdateDag(dag); err != nil {
			return err
		}
	}
	return nil
}

func main() {
	// init keeper
	keeper := mysqlKeeper.NewKeeper(&mysqlKeeper.KeeperOption{
		Key:     "worker-1",
		ConnStr: "root:mysql@tcp(172.31.116.212:32427)/fastflow?charset=utf8mb4&parseTime=True&loc=Local&timeout=10000ms",
		Prefix:  "test",
	})
	if err := keeper.Init(); err != nil {
		log.Fatal(fmt.Errorf("init keeper failed: %w", err))
	}
	// init store
	st := mysqlStore.NewStore(&mysqlStore.StoreOption{
		ConnStr: "root:mysql@tcp(172.31.116.212:32427)/fastflow?charset=utf8mb4&parseTime=True&loc=Local&timeout=10000ms",
		Prefix:  "test",
	})
	if err := st.Init(); err != nil {
		log.Fatal(fmt.Errorf("init store failed: %w", err))
	}

	// init fastflow
	if err := fastflow.Init(&fastflow.InitialOption{
		Keeper:            keeper,
		Store:             st,
		ParserWorkersCnt:  10,
		ExecutorWorkerCnt: 50,
		ExecutorTimeout:   600,
	}); err != nil {
		panic(fmt.Sprintf("init fastflow failed: %s", err))
	}

	// create a dag as template
	if err := ensureDagCreated(); err != nil {
		panic(err.Error())
	}
	// run dag interval
	go runInstance()

	// listen a http endpoint to serve metrics
	if err := http.ListenAndServe(":9090", exporter.HttpHandler()); err != nil {
		panic(fmt.Sprintf("metrics serve failed: %s", err))
	}
}

func runInstance() {
	// wait init completed
	time.Sleep(2 * time.Second)
	dag, err := mod.GetStore().GetDag("ci-buildkit")
	if err != nil {
		panic(err)
	}
	runVar := map[string]string{
		"var": "run-var",
	}
	dagIns, err := dag.Run(entity.TriggerManually, runVar)
	if err != nil {
		panic(err)
	}
	err = mod.GetStore().CreateDagIns(dagIns)
	if err != nil {
		panic(err)
	}
}
