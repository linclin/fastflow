package mongo

import (
	"fmt"
	"time"

	"github.com/linclin/fastflow/pkg/entity"
	"github.com/linclin/fastflow/pkg/event"
	"github.com/linclin/fastflow/pkg/mod"
	"github.com/shiningrush/goevent"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

// StoreOption
type StoreOption struct {
	// mysql connection string
	ConnStr string
	// the prefix will append to the database
	Prefix string
}

// Store
type Store struct {
	opt            *StoreOption
	dagClsName     string
	dagInsClsName  string
	taskInsClsName string
	db             *gorm.DB
}

// NewStore
func NewStore(option *StoreOption) *Store {
	return &Store{
		opt: option,
	}
}

// Init store
func (s *Store) Init() error {
	db, err := gorm.Open(mysql.New(mysql.Config{
		DSN:                       s.opt.ConnStr, // DSN data source name
		DefaultStringSize:         256,           // string 类型字段的默认长度
		DisableDatetimePrecision:  true,          // 禁用 datetime 精度，MySQL 5.6 之前的数据库不支持
		DontSupportRenameIndex:    true,          // 重命名索引时采用删除并新建的方式，MySQL 5.7 之前的数据库和 MariaDB 不支持重命名索引
		DontSupportRenameColumn:   true,          // 用 `change` 重命名列，MySQL 8 之前的数据库和 MariaDB 不支持重命名列
		SkipInitializeWithVersion: false,         // 根据当前 MySQL 版本自动配置
	}), &gorm.Config{Logger: gormlogger.Default.LogMode(gormlogger.Info)})
	if err != nil {
		return fmt.Errorf("connect client failed: %w", err)
	}
	sqlDB, _ := db.DB()
	// SetMaxIdleCons 设置连接池中的最大闲置连接数。
	sqlDB.SetMaxIdleConns(10)
	// SetMaxOpenCons 设置数据库的最大连接数量。
	sqlDB.SetMaxOpenConns(500)
	// SetConnMaxLifetiment 设置连接的最大可复用时间。
	sqlDB.SetConnMaxLifetime(time.Hour)
	s.db = db
	//自动建表
	s.db.Table(s.opt.Prefix + "_dag").AutoMigrate(&entity.Dag{})
	s.db.Table(s.opt.Prefix + "_task").AutoMigrate(&entity.Task{})
	s.db.Table(s.opt.Prefix + "_dag_instance").AutoMigrate(&entity.DagInstance{})
	s.db.Table(s.opt.Prefix + "_task_instance").AutoMigrate(&entity.TaskInstance{})
	return nil
}

// Close component when we not use it anymore
func (s *Store) Close() {

}

// CreateDag
func (s *Store) CreateDag(dag *entity.Dag) error {
	// check task's connection
	_, err := mod.BuildRootNode(mod.MapTasksToGetter(dag.Tasks))
	if err != nil {
		return err
	}
	baseInfo := dag.GetBaseInfo()
	baseInfo.Initial()
	err = s.db.Create(&dag).Error
	if err != nil {
		return fmt.Errorf("insert Dag failed: %w", err)
	}
	return nil
}

// CreateDagIns
func (s *Store) CreateDagIns(dagIns *entity.DagInstance) error {
	baseInfo := dagIns.GetBaseInfo()
	baseInfo.Initial()
	err := s.db.Create(&dagIns).Error
	if err != nil {
		return fmt.Errorf("insert DagInstance failed: %w", err)
	}
	return nil
}

// CreateTaskIns
func (s *Store) CreateTaskIns(taskIns *entity.TaskInstance) error {
	baseInfo := taskIns.GetBaseInfo()
	baseInfo.Initial()
	err := s.db.Create(&taskIns).Error
	if err != nil {
		return fmt.Errorf("insert TaskInstance failed: %w", err)
	}
	return nil
}

// BatchCreatTaskIns
func (s *Store) BatchCreatTaskIns(taskIns []*entity.TaskInstance) error {
	for i := range taskIns {
		taskIns[i].Initial()
		err := s.db.Create(&taskIns).Error
		if err != nil {
			return fmt.Errorf("insert TaskInstance failed: %w", err)
		}
	}
	return nil
}

// PatchTaskIns
func (s *Store) PatchTaskIns(taskIns *entity.TaskInstance) error {
	if taskIns.ID == "" {
		return fmt.Errorf("id cannot be empty")
	}
	taskIns.Update()
	err := s.db.Where("taskId = ?", taskIns.ID).Updates(&taskIns).Error
	if err != nil {
		return fmt.Errorf("patch TaskInstance failed: %w", err)
	}
	return nil
}

// PatchDagIns
func (s *Store) PatchDagIns(dagIns *entity.DagInstance, mustsPatchFields ...string) error {
	dagIns.Update()
	err := s.db.Where("dagId = ?", dagIns.ID).Updates(&dagIns).Error
	if err != nil {
		return fmt.Errorf("patch DagInstance failed: %w", err)
	}
	goevent.Publish(&event.DagInstancePatched{
		Payload:         dagIns,
		MustPatchFields: mustsPatchFields,
	})
	return nil
}

// UpdateDag
func (s *Store) UpdateDag(dag *entity.Dag) error {
	// check task's connection
	_, err := mod.BuildRootNode(mod.MapTasksToGetter(dag.Tasks))
	if err != nil {
		return err
	}
	dag.Update()
	err = s.db.Where("id = ?", dag.ID).Updates(&dag).Error
	if err != nil {
		return fmt.Errorf("patch Dag failed: %w", err)
	}
	return nil
}

// UpdateDagIns
func (s *Store) UpdateDagIns(dagIns *entity.DagInstance) error {
	dagIns.Update()
	err := s.db.Where("dagId = ?", dagIns.ID).Updates(&dagIns).Error
	if err != nil {
		return fmt.Errorf("patch DagInstance failed: %w", err)
	}
	goevent.Publish(&event.DagInstanceUpdated{Payload: dagIns})
	return nil
}

// UpdateTaskIns
func (s *Store) UpdateTaskIns(taskIns *entity.TaskInstance) error {
	taskIns.Update()
	err := s.db.Where("taskId = ?", taskIns.ID).Updates(&taskIns).Error
	if err != nil {
		return fmt.Errorf("patch DagInstance failed: %w", err)
	}
	return nil
}

// BatchUpdateDagIns
func (s *Store) BatchUpdateDagIns(dagIns []*entity.DagInstance) error {
	for i := range dagIns {
		dagIns[i].Update()
		err := s.db.Where("dagId = ?", dagIns[i].ID).Updates(&dagIns[i]).Error
		if err != nil {
			return fmt.Errorf("patch DagInstance failed: %w", err)
		}
	}
	return nil
}

// BatchUpdateTaskIns
func (s *Store) BatchUpdateTaskIns(taskIns []*entity.TaskInstance) error {
	for i := range taskIns {
		taskIns[i].Update()
		err := s.db.Where("taskId = ?", taskIns[i].ID).Updates(&taskIns[i]).Error
		if err != nil {
			return fmt.Errorf("patch TaskInstance failed: %w", err)
		}
	}
	return nil
}

// GetTaskIns
func (s *Store) GetTaskIns(taskInsId string) (*entity.TaskInstance, error) {
	ret := new(entity.TaskInstance)
	err := s.db.Where("taskId = ?", taskInsId).First(&ret).Error
	if err != nil {
		return nil, err
	}
	return ret, nil
}

// GetDag
func (s *Store) GetDag(dagId string) (*entity.Dag, error) {
	ret := new(entity.Dag)
	err := s.db.Where("id = ?", dagId).First(&ret).Error
	if err != nil {
		return nil, err
	}
	return ret, nil
}

// GetDagInstance
func (s *Store) GetDagInstance(dagInsId string) (*entity.DagInstance, error) {
	ret := new(entity.DagInstance)
	err := s.db.Where("dagId = ?", dagInsId).First(&ret).Error
	if err != nil {
		return nil, err
	}
	return ret, nil
}

// ListDag
func (s *Store) ListDag(input *mod.ListDagInput) ([]*entity.Dag, error) {
	var ret []*entity.Dag
	err := s.db.Find(&ret).Error
	if err != nil {
		return nil, err
	}
	return ret, nil
}

// ListDagInstance
func (s *Store) ListDagInstance(input *mod.ListDagInstanceInput) ([]*entity.DagInstance, error) {
	filterExp := "1=1 "
	filterArgs := []interface{}{}
	if len(input.Status) > 0 {
		filterExp = "AND status in (?) "
		filterArgs = append(filterArgs, input.Status)
	}
	if input.Worker != "" {
		filterExp = "AND worker= ? "
		filterArgs = append(filterArgs, input.Status)
	}
	if input.UpdatedEnd > 0 {
		filterExp = "AND updatedAt<= ? "
		filterArgs = append(filterArgs, input.UpdatedEnd)
	}
	if input.HasCmd {
		filterExp = "AND cmd IS NOT NULL "
	}
	limit := 10
	if input.Limit > 0 {
		limit = int(input.Limit)
	}
	var ret []*entity.DagInstance
	err := s.db.Where(filterExp, filterArgs...).Limit(limit).Order("updatedAt DESC").Find(&ret).Error
	if err != nil {
		return nil, err
	}
	return ret, nil
}

// ListTaskInstance
func (s *Store) ListTaskInstance(input *mod.ListTaskInstanceInput) ([]*entity.TaskInstance, error) {
	filterExp := "1=1 "
	filterArgs := []interface{}{}
	if len(input.IDs) > 0 {
		filterExp = "AND id in (?) "
		filterArgs = append(filterArgs, input.IDs)
	}
	if len(input.Status) > 0 {
		filterExp = "AND status in (?) "
		filterArgs = append(filterArgs, input.Status)
	}
	if input.Expired {
		filterExp = "AND updatedAt<=?  "
		filterArgs = append(filterArgs, time.Now().Unix()-5)
	}
	if input.DagInsID != "" {
		filterExp = "AND dagInsId =? "
		filterArgs = append(filterArgs, input.DagInsID)
	}
	var ret []*entity.TaskInstance
	err := s.db.Where(filterExp, filterArgs...).Order("updatedAt DESC").Find(&ret).Error
	if err != nil {
		return nil, err
	}
	return ret, nil
}

// BatchDeleteDag
func (s *Store) BatchDeleteDag(ids []string) error {
	err := s.db.Delete(&entity.Dag{}, ids).Error
	if err != nil {
		return err
	}
	return nil
}

// BatchDeleteDagIns
func (s *Store) BatchDeleteDagIns(ids []string) error {
	err := s.db.Delete(&entity.DagInstance{}, ids).Error
	if err != nil {
		return err
	}
	return nil
}

// BatchDeleteTaskIns
func (s *Store) BatchDeleteTaskIns(ids []string) error {
	err := s.db.Delete(&entity.TaskInstance{}, ids).Error
	if err != nil {
		return err
	}
	return nil
}
