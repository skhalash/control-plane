package postsql

import (
	"fmt"
	"strings"

	"github.com/kyma-project/control-plane/components/kyma-environment-broker/common/orchestration"
	"github.com/pkg/errors"

	"github.com/kyma-project/control-plane/components/kyma-environment-broker/internal/storage/dberr"
	"github.com/kyma-project/control-plane/components/kyma-environment-broker/internal/storage/dbmodel"
	"github.com/kyma-project/control-plane/components/kyma-environment-broker/internal/storage/predicate"

	"github.com/gocraft/dbr"
	"github.com/pivotal-cf/brokerapi/v7/domain"
)

type readSession struct {
	session *dbr.Session
}

func (r readSession) getInstancesJoinedWithOperationStatement() *dbr.SelectStmt {
	join := fmt.Sprintf("%s.instance_id = %s.instance_id", InstancesTableName, OperationTableName)
	stmt := r.session.
		Select("instances.instance_id, instances.runtime_id, instances.global_account_id, instances.service_id,"+
			" instances.service_plan_id, instances.dashboard_url, instances.provisioning_parameters, instances.created_at,"+
			" instances.updated_at, instances.deleted_at, instances.sub_account_id, instances.service_name, instances.service_plan_name,"+
			" instances.provider_region, operations.state, operations.description, operations.type").
		From(InstancesTableName).
		LeftJoin(OperationTableName, join)
	return stmt
}

func (r readSession) FindAllInstancesJoinedWithOperation(prct ...predicate.Predicate) ([]dbmodel.InstanceWithOperationDTO, dberr.Error) {
	var instances []dbmodel.InstanceWithOperationDTO

	stmt := r.getInstancesJoinedWithOperationStatement()
	for _, p := range prct {
		p.ApplyToPostgres(stmt)
	}

	if _, err := stmt.Load(&instances); err != nil {
		return nil, dberr.Internal("Failed to fetch all instances: %s", err)
	}

	return instances, nil
}

func (r readSession) GetInstanceByID(instanceID string) (dbmodel.InstanceDTO, dberr.Error) {
	var instance dbmodel.InstanceDTO

	err := r.session.
		Select("*").
		From(InstancesTableName).
		Where(dbr.Eq("instance_id", instanceID)).
		LoadOne(&instance)

	if err != nil {
		if err == dbr.ErrNotFound {
			return dbmodel.InstanceDTO{}, dberr.NotFound("Cannot find Instance for instanceID:'%s'", instanceID)
		}
		return dbmodel.InstanceDTO{}, dberr.Internal("Failed to get Instance: %s", err)
	}

	return instance, nil
}

func (r readSession) FindAllInstancesForRuntimes(runtimeIdList []string) ([]dbmodel.InstanceDTO, dberr.Error) {
	var instances []dbmodel.InstanceDTO

	err := r.session.
		Select("*").
		From(InstancesTableName).
		Where("runtime_id IN ?", runtimeIdList).
		LoadOne(&instances)

	if err != nil {
		if err == dbr.ErrNotFound {
			return []dbmodel.InstanceDTO{}, dberr.NotFound("Cannot find Instances for runtime ID list: '%v'", runtimeIdList)
		}
		return []dbmodel.InstanceDTO{}, dberr.Internal("Failed to get Instances: %s", err)
	}
	return instances, nil
}

func (r readSession) FindAllInstancesForSubAccounts(subAccountslist []string) ([]dbmodel.InstanceDTO, dberr.Error) {
	var instances []dbmodel.InstanceDTO

	err := r.session.
		Select("*").
		From(InstancesTableName).
		Where("sub_account_id IN ?", subAccountslist).
		LoadOne(&instances)

	if err != nil {
		if err == dbr.ErrNotFound {
			return []dbmodel.InstanceDTO{}, nil
		}
		return []dbmodel.InstanceDTO{}, dberr.Internal("Failed to get Instances: %s", err)
	}
	return instances, nil
}

func (r readSession) GetLastOperation(instanceID string) (dbmodel.OperationDTO, dberr.Error) {
	inst := dbr.Eq("instance_id", instanceID)
	state := dbr.Neq("state", orchestration.Pending)
	condition := dbr.And(inst, state)
	operation, err := r.getLastOperation(condition)
	if err != nil {
		switch {
		case dberr.IsNotFound(err):
			return dbmodel.OperationDTO{}, dberr.NotFound("for instance ID: %s %s", instanceID, err)
		default:
			return dbmodel.OperationDTO{}, err
		}
	}
	return operation, nil
}

func (r readSession) GetOperationByID(opID string) (dbmodel.OperationDTO, dberr.Error) {
	condition := dbr.Eq("id", opID)
	operation, err := r.getOperation(condition)
	if err != nil {
		switch {
		case dberr.IsNotFound(err):
			return dbmodel.OperationDTO{}, dberr.NotFound("for ID: %s %s", opID, err)
		default:
			return dbmodel.OperationDTO{}, err
		}
	}
	return operation, nil
}

func (r readSession) ListOperations(filter dbmodel.OperationFilter) ([]dbmodel.OperationDTO, int, int, error) {
	var operations []dbmodel.OperationDTO

	stmt := r.session.Select("*").
		From(OperationTableName).
		OrderBy(CreatedAtField)

	// Add pagination if provided
	if filter.Page > 0 && filter.PageSize > 0 {
		stmt.Paginate(uint64(filter.Page), uint64(filter.PageSize))
	}

	// Apply filtering if provided
	addOperationFilters(stmt, filter)

	_, err := stmt.Load(&operations)

	totalCount, err := r.getOperationCount(filter)
	if err != nil {
		return nil, -1, -1, err
	}

	return operations,
		len(operations),
		totalCount,
		nil
}

func (r readSession) GetOrchestrationByID(oID string) (dbmodel.OrchestrationDTO, dberr.Error) {
	condition := dbr.Eq("orchestration_id", oID)
	operation, err := r.getOrchestration(condition)
	if err != nil {
		switch {
		case dberr.IsNotFound(err):
			return dbmodel.OrchestrationDTO{}, dberr.NotFound("for ID: %s %s", oID, err)
		default:
			return dbmodel.OrchestrationDTO{}, err
		}
	}
	return operation, nil
}

func (r readSession) ListOrchestrations(filter dbmodel.OrchestrationFilter) ([]dbmodel.OrchestrationDTO, int, int, error) {
	var orchestrations []dbmodel.OrchestrationDTO

	stmt := r.session.Select("*").
		From(OrchestrationTableName).
		OrderBy(CreatedAtField)

	// Add pagination if provided
	if filter.Page > 0 && filter.PageSize > 0 {
		stmt.Paginate(uint64(filter.Page), uint64(filter.PageSize))
	}

	// Apply filtering if provided
	addOrchestrationFilters(stmt, filter)

	_, err := stmt.Load(&orchestrations)

	totalCount, err := r.getOrchestrationCount(filter)
	if err != nil {
		return nil, -1, -1, err
	}

	return orchestrations,
		len(orchestrations),
		totalCount,
		nil
}

func (r readSession) GetNotFinishedOperationsByType(operationType dbmodel.OperationType) ([]dbmodel.OperationDTO, dberr.Error) {
	stateInProgress := dbr.Eq("state", domain.InProgress)
	statePending := dbr.Eq("state", orchestration.Pending)
	stateCondition := dbr.Or(statePending, stateInProgress)
	typeCondition := dbr.Eq("type", operationType)
	var operations []dbmodel.OperationDTO

	_, err := r.session.
		Select("*").
		From(OperationTableName).
		Where(stateCondition).
		Where(typeCondition).
		Load(&operations)
	if err != nil {
		return nil, dberr.Internal("Failed to get operations: %s", err)
	}
	return operations, nil
}

func (r readSession) GetOperationByTypeAndInstanceID(inID string, opType dbmodel.OperationType) (dbmodel.OperationDTO, dberr.Error) {
	idCondition := dbr.Eq("instance_id", inID)
	typeCondition := dbr.Eq("type", string(opType))
	var operation dbmodel.OperationDTO

	err := r.session.
		Select("*").
		From(OperationTableName).
		Where(idCondition).
		Where(typeCondition).
		OrderDesc(CreatedAtField).
		LoadOne(&operation)

	if err != nil {
		if err == dbr.ErrNotFound {
			return dbmodel.OperationDTO{}, dberr.NotFound("cannot find operation: %s", err)
		}
		return dbmodel.OperationDTO{}, dberr.Internal("Failed to get operation: %s", err)
	}
	return operation, nil
}

func (r readSession) GetOperationsByTypeAndInstanceID(inID string, opType dbmodel.OperationType) ([]dbmodel.OperationDTO, dberr.Error) {
	idCondition := dbr.Eq("instance_id", inID)
	typeCondition := dbr.Eq("type", string(opType))
	var operations []dbmodel.OperationDTO

	_, err := r.session.
		Select("*").
		From(OperationTableName).
		Where(idCondition).
		Where(typeCondition).
		OrderDesc(CreatedAtField).
		Load(&operations)

	if err != nil {
		return []dbmodel.OperationDTO{}, dberr.Internal("Failed to get operations: %s", err)
	}
	return operations, nil
}

func (r readSession) GetOperationsForIDs(opIDlist []string) ([]dbmodel.OperationDTO, dberr.Error) {
	var operations []dbmodel.OperationDTO

	_, err := r.session.
		Select("*").
		From(OperationTableName).
		Where("id IN ?", opIDlist).
		Load(&operations)
	if err != nil {
		return nil, dberr.Internal("Failed to get operations: %s", err)
	}
	return operations, nil
}

func (r readSession) ListOperationsByType(operationType dbmodel.OperationType) ([]dbmodel.OperationDTO, dberr.Error) {
	typeCondition := dbr.Eq("type", operationType)
	var operations []dbmodel.OperationDTO

	_, err := r.session.
		Select("*").
		From(OperationTableName).
		Where(typeCondition).
		Load(&operations)
	if err != nil {
		return nil, dberr.Internal("Failed to get operations: %s", err)
	}
	return operations, nil
}

func (r readSession) ListOperationsByOrchestrationID(orchestrationID string, filter dbmodel.OperationFilter) ([]dbmodel.OperationDTO, int, int, error) {
	var ops []dbmodel.OperationDTO
	condition := dbr.Eq("orchestration_id", orchestrationID)

	stmt := r.session.
		Select("*").
		From(OperationTableName).
		Where(condition).
		OrderBy(CreatedAtField)

	// Add pagination if provided
	if filter.Page > 0 && filter.PageSize > 0 {
		stmt.Paginate(uint64(filter.Page), uint64(filter.PageSize))
	}

	// Apply filtering if provided
	addOperationFilters(stmt, filter)

	_, err := stmt.Load(&ops)
	if err != nil {
		return nil, -1, -1, dberr.Internal("Failed to get operations: %s", err)
	}

	totalCount, err := r.getUpgradeOperationCount(orchestrationID, filter)
	if err != nil {
		return nil, -1, -1, err
	}

	return ops,
		len(ops),
		totalCount,
		nil
}

func (r readSession) GetRuntimeStateByOperationID(operationID string) (dbmodel.RuntimeStateDTO, dberr.Error) {
	var state dbmodel.RuntimeStateDTO

	err := r.session.
		Select("*").
		From(RuntimeStateTableName).
		Where(dbr.Eq("operation_id", operationID)).
		LoadOne(&state)

	if err != nil {
		if err == dbr.ErrNotFound {
			return dbmodel.RuntimeStateDTO{}, dberr.NotFound("cannot find runtime state: %s", err)
		}
		return dbmodel.RuntimeStateDTO{}, dberr.Internal("Failed to get runtime state: %s", err)
	}
	return state, nil
}

func (r readSession) ListRuntimeStateByRuntimeID(runtimeID string) ([]dbmodel.RuntimeStateDTO, dberr.Error) {
	stateCondition := dbr.Eq("runtime_id", runtimeID)
	var states []dbmodel.RuntimeStateDTO

	_, err := r.session.
		Select("*").
		From(RuntimeStateTableName).
		Where(stateCondition).
		Load(&states)
	if err != nil {
		return nil, dberr.Internal("Failed to get states: %s", err)
	}
	return states, nil
}

func (r readSession) getOperation(condition dbr.Builder) (dbmodel.OperationDTO, dberr.Error) {
	var operation dbmodel.OperationDTO

	err := r.session.
		Select("*").
		From(OperationTableName).
		Where(condition).
		LoadOne(&operation)

	if err != nil {
		if err == dbr.ErrNotFound {
			return dbmodel.OperationDTO{}, dberr.NotFound("cannot find operation: %s", err)
		}
		return dbmodel.OperationDTO{}, dberr.Internal("Failed to get operation: %s", err)
	}
	return operation, nil
}

func (r readSession) getLastOperation(condition dbr.Builder) (dbmodel.OperationDTO, dberr.Error) {
	var operation dbmodel.OperationDTO

	count, err := r.session.
		Select("*").
		From(OperationTableName).
		Where(condition).
		OrderDesc(CreatedAtField).
		Limit(1).
		Load(&operation)
	if err != nil {
		if err == dbr.ErrNotFound {
			return dbmodel.OperationDTO{}, dberr.NotFound("cannot find operation: %s", err)
		}
		return dbmodel.OperationDTO{}, dberr.Internal("Failed to get operation: %s", err)
	}
	if count == 0 {
		return dbmodel.OperationDTO{}, dberr.NotFound("cannot find operation: %s", err)
	}

	return operation, nil
}

func (r readSession) getOrchestration(condition dbr.Builder) (dbmodel.OrchestrationDTO, dberr.Error) {
	var operation dbmodel.OrchestrationDTO

	err := r.session.
		Select("*").
		From(OrchestrationTableName).
		Where(condition).
		LoadOne(&operation)

	if err != nil {
		if err == dbr.ErrNotFound {
			return dbmodel.OrchestrationDTO{}, dberr.NotFound("cannot find operation: %s", err)
		}
		return dbmodel.OrchestrationDTO{}, dberr.Internal("Failed to get operation: %s", err)
	}
	return operation, nil
}

func (r readSession) GetLMSTenant(name, region string) (dbmodel.LMSTenantDTO, dberr.Error) {
	var dto dbmodel.LMSTenantDTO
	err := r.session.
		Select("*").
		From(LMSTenantTableName).
		Where(dbr.Eq("name", name)).
		Where(dbr.Eq("region", region)).
		LoadOne(&dto)

	if err != nil {
		if err == dbr.ErrNotFound {
			return dbmodel.LMSTenantDTO{}, dberr.NotFound("Cannot find lms tenant for name/region: '%s/%s'", name, region)
		}
		return dbmodel.LMSTenantDTO{}, dberr.Internal("Failed to get operation: %s", err)
	}
	return dto, nil
}

func (r readSession) GetCLSInstanceByGlobalAccountID(globalAccountID string) ([]dbmodel.CLSInstanceDTO, dberr.Error) {
	var dtos []dbmodel.CLSInstanceDTO
	_, err := r.session.
		Select("cls_instances.id, cls_instances.version, cls_instances.global_account_id, cls_instances.region, cls_instances.created_at, cls_instances.removed_by_skr_instance_id, cls_instance_references.skr_instance_id").
		From(CLSInstanceTableName).
		Where(dbr.Eq("global_account_id", globalAccountID)).
		Where(dbr.Eq("removed_by_skr_instance_id", nil)).
		Join(CLSInstanceReferenceTableName, fmt.Sprintf("%s.cls_instance_id = %s.id", CLSInstanceReferenceTableName, CLSInstanceTableName)).
		Load(&dtos)

	if err != nil {
		if err == dbr.ErrNotFound {
			return nil, dberr.NotFound("cannot find a cls instance for global account id %s", globalAccountID)
		}
		return nil, dberr.Internal("failed to find a cls instance: %s", err)
	}
	return dtos, nil
}

func (r readSession) GetCLSInstanceByID(clsInstanceID string) ([]dbmodel.CLSInstanceDTO, dberr.Error) {
	var dtos []dbmodel.CLSInstanceDTO
	_, err := r.session.
		Select("cls_instances.id, cls_instances.version, cls_instances.global_account_id, cls_instances.region, cls_instances.created_at, cls_instance_references.skr_instance_id").
		From(CLSInstanceTableName).
		Where(dbr.Eq("cls_instances.id", clsInstanceID)).
		Join(CLSInstanceReferenceTableName, fmt.Sprintf("%s.cls_instance_id = %s.id", CLSInstanceReferenceTableName, CLSInstanceTableName)).
		Load(&dtos)

	if err != nil {
		if err == dbr.ErrNotFound {
			return nil, dberr.NotFound("cannot find a cls instance with id %s", clsInstanceID)
		}
		return nil, dberr.Internal("failed to find a cls instance: %s", err)
	}
	return dtos, nil
}

func (r readSession) GetOperationStats() ([]dbmodel.OperationStatEntry, error) {
	var rows []dbmodel.OperationStatEntry
	_, err := r.session.SelectBySql(fmt.Sprintf("select type, state, provisioning_parameters ->> 'plan_id' AS plan_id from %s",
		OperationTableName)).Load(&rows)
	return rows, err
}

func (r readSession) GetOperationStatsForOrchestration(orchestrationID string) ([]dbmodel.OperationStatEntry, error) {
	var rows []dbmodel.OperationStatEntry
	_, err := r.session.SelectBySql(fmt.Sprintf("select type, state, provisioning_parameters ->> 'plan_id' AS plan_id from %s where orchestration_id='%s'",
		OperationTableName, orchestrationID)).Load(&rows)
	return rows, err
}

func (r readSession) GetInstanceStats() ([]dbmodel.InstanceByGlobalAccountIDStatEntry, error) {
	var rows []dbmodel.InstanceByGlobalAccountIDStatEntry
	_, err := r.session.SelectBySql(fmt.Sprintf("select global_account_id, count(*) as total from %s group by global_account_id",
		InstancesTableName)).Load(&rows)
	return rows, err
}

func (r readSession) GetNumberOfInstancesForGlobalAccountID(globalAccountID string) (int, error) {
	var res struct {
		Total int
	}
	err := r.session.Select("count(*) as total").
		From(InstancesTableName).
		Where(dbr.Eq("global_account_id", globalAccountID)).
		LoadOne(&res)

	return res.Total, err
}

func (r readSession) ListInstances(filter dbmodel.InstanceFilter) ([]dbmodel.InstanceDTO, int, int, error) {
	var instances []dbmodel.InstanceDTO

	// Base select and order by created at
	stmt := r.session.
		Select("*").
		From(InstancesTableName).
		OrderBy(CreatedAtField)

	// Add pagination
	if filter.Page > 0 && filter.PageSize > 0 {
		stmt = stmt.Paginate(uint64(filter.Page), uint64(filter.PageSize))
	}

	addInstanceFilters(stmt, filter)

	_, err := stmt.Load(&instances)
	if err != nil {
		return nil, -1, -1, errors.Wrap(err, "while fetching instances")
	}

	totalCount, err := r.getInstanceCount(filter)
	if err != nil {
		return nil, -1, -1, err
	}

	return instances,
		len(instances),
		totalCount,
		nil
}

func (r readSession) getInstanceCount(filter dbmodel.InstanceFilter) (int, error) {
	var res struct {
		Total int
	}
	stmt := r.session.Select("count(*) as total").From(InstancesTableName)
	addInstanceFilters(stmt, filter)
	err := stmt.LoadOne(&res)

	return res.Total, err
}

func addInstanceFilters(stmt *dbr.SelectStmt, filter dbmodel.InstanceFilter) {
	if len(filter.GlobalAccountIDs) > 0 {
		stmt.Where("global_account_id IN ?", filter.GlobalAccountIDs)
	}
	if len(filter.SubAccountIDs) > 0 {
		stmt.Where("sub_account_id IN ?", filter.SubAccountIDs)
	}
	if len(filter.InstanceIDs) > 0 {
		stmt.Where("instance_id IN ?", filter.InstanceIDs)
	}
	if len(filter.RuntimeIDs) > 0 {
		stmt.Where("runtime_id IN ?", filter.RuntimeIDs)
	}
	if len(filter.Regions) > 0 {
		stmt.Where("provider_region IN ?", filter.Regions)
	}
	if len(filter.Plans) > 0 {
		stmt.Where("service_plan_name IN ?", filter.Plans)
	}
	if len(filter.Domains) > 0 {
		// Preceeding character is either a . or / (after protocol://)
		// match subdomain inputs
		// match any .upperdomain zero or more times
		domainMatch := fmt.Sprintf(`[./](%s)(\.[0-9A-Za-z-]+)*$`, strings.Join(filter.Domains, "|"))
		stmt.Where("dashboard_url ~ ?", domainMatch)
	}
}

func addOrchestrationFilters(stmt *dbr.SelectStmt, filter dbmodel.OrchestrationFilter) {
	if len(filter.States) > 0 {
		stmt.Where("state IN ?", filter.States)
	}
}

func addOperationFilters(stmt *dbr.SelectStmt, filter dbmodel.OperationFilter) {
	if len(filter.States) > 0 {
		stmt.Where("state IN ?", filter.States)
	}
}

func (r readSession) getOperationCount(filter dbmodel.OperationFilter) (int, error) {
	var res struct {
		Total int
	}
	stmt := r.session.Select("count(*) as total").
		From(OperationTableName)
	addOperationFilters(stmt, filter)
	err := stmt.LoadOne(&res)

	return res.Total, err
}

func (r readSession) getUpgradeOperationCount(orchestrationID string, filter dbmodel.OperationFilter) (int, error) {
	var res struct {
		Total int
	}
	stmt := r.session.Select("count(*) as total").
		From(OperationTableName).
		Where(dbr.Eq("orchestration_id", orchestrationID))
	addOperationFilters(stmt, filter)
	err := stmt.LoadOne(&res)

	return res.Total, err
}

func (r readSession) getOrchestrationCount(filter dbmodel.OrchestrationFilter) (int, error) {
	var res struct {
		Total int
	}
	stmt := r.session.Select("count(*) as total").From(OrchestrationTableName)
	addOrchestrationFilters(stmt, filter)
	err := stmt.LoadOne(&res)

	return res.Total, err
}
