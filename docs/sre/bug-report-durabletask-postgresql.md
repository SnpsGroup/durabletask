# Bug Report: DurableTask.PostgreSQL 1.0.0-alpha.1

**Package:** `DurableTask.PostgreSQL`  
**Version:** `1.0.0-alpha.1`  
**Repository:** https://github.com/SnpsGroup/durableTask-postgresql  
**Reported by:** Menshen team  
**Date:** 2026-06-15  
**Severity:** High — orchestrations cannot execute past the first `ScheduleTask` call

---

## 1. Executive Summary

While integrating `DurableTask.PostgreSQL` as the Durable Task Framework (DTFx) storage provider for the Menshen workflow host, we observed that **every orchestration is abandoned immediately after it attempts to schedule the first activity**. The worker starts, deploys the schema, locks the work item, but no history events or activity tasks are persisted. A consumer-side query of orchestration state also throws `IndexOutOfRangeException`.

Root-cause analysis of the provider source and the deployed PostgreSQL schema identified **three confirmed defects** and one **open runtime failure** that needs maintainer input:

| # | Defect | Location | Impact |
|---|--------|----------|--------|
| 1 | `dt.query_single_orchestration` does not return `parent_instance_id` while C# reader expects it | `Scripts/logic.postgresql.sql` | `GetOrchestrationStateAsync` throws `IndexOutOfRangeException` |
| 2 | `TaskHubMode = 1` is hard-coded in schema and `TaskHubName` from C# settings is never propagated to PostgreSQL | `Scripts/schema.postgresql.sql`, `PostgreSqlOrchestrationService.cs` | Instances are always tagged with `CURRENT_USER` as task hub, regardless of configured `TaskHubName` |
| 3 | `DeploySchemaAsync` uses raw `CREATE OR REPLACE FUNCTION` and cannot apply changes that alter a function's return type | `PostgreSqlOrchestrationService.cs` | Schema updates fail with `42P13: cannot change return type of existing function` |
| 4 | Orchestration work items are abandoned after `ScheduleTask` | Runtime path through `TaskOrchestrationDispatcher` | Activities never run; no `dt.history` or `dt.new_tasks` rows are written by the runtime |

Defects 1–3 are directly fixable in the provider. Defect 4 is the observable symptom; our evidence suggests it is triggered by the SQL/parameter mismatch between the C# runtime and the stored procedures, but we could not capture the exact inner exception because the DTFx dispatcher swallows it and only logs the abandonment warning.

---

## 2. Environment & Reproduction

### 2.1 Versions

- `DurableTask.PostgreSQL`: `1.0.0-alpha.1` (NuGet)
- `Microsoft.Azure.DurableTask.Core`: `2.16.2`
- .NET: `10.0.201`
- PostgreSQL: `17.4` (Docker `postgres:17-alpine`)
- Npgsql: resolved transitively by `DurableTask.PostgreSQL`

### 2.2 Minimal Reproduction

A self-contained reproducer can be built from the provider's own sample shape:

```csharp
using DurableTask.Core;
using DurableTask.PostgreSQL;
using Microsoft.Extensions.DependencyInjection;

var services = new ServiceCollection();

services.AddDurableTaskPostgreSql(new PostgreSqlOrchestrationServiceSettings
{
    ConnectionString = "Host=localhost;Port=5432;Database=durabletask;Username=postgres;Password=postgres;Search Path=dt,public",
    TaskHubName = "sample-hub",   // <-- intentionally different from DB user
    SchemaName = "dt",
    AutoDeploySchema = true,
    WorkerId = "repro-worker"
});

services.AddSingleton<TaskHubWorker>(sp =>
{
    var service = sp.GetRequiredService<IOrchestrationService>();
    var worker = new TaskHubWorker(service);
    worker.AddTaskOrchestrations(typeof(HelloOrchestration));
    worker.AddTaskActivities(typeof(HelloActivity));
    return worker;
});

var provider = services.BuildServiceProvider();
var worker = provider.GetRequiredService<TaskHubWorker>();
await worker.StartAsync();

var client = new TaskHubClient(provider.GetRequiredService<IOrchestrationServiceClient>());
var instance = await client.CreateOrchestrationInstanceAsync(typeof(HelloOrchestration), "repro-1", "World");

// Wait and then query state
await Task.Delay(TimeSpan.FromSeconds(10));
var state = await client.GetOrchestrationStateAsync("repro-1", false);
```

With the following trivial orchestration/activity:

```csharp
public class HelloOrchestration : TaskOrchestration<string, string>
{
    public override async Task<string> RunTask(OrchestrationContext context, string input)
    {
        var result = await context.ScheduleTask<string>(typeof(HelloActivity), input);
        return result;
    }
}

public class HelloActivity : AsyncTaskActivity<string, string>
{
    protected override Task<string> ExecuteAsync(TaskContext context, string input)
    {
        return Task.FromResult($"Hello {input}!");
    }
}
```

### 2.3 Observed Behavior

1. Worker starts and logs `Schema deployed successfully`.
2. Worker logs `Connected to PostgreSQL. TaskHub=sample-hub`.
3. Orchestration instance is created.
4. Worker locks the instance (`locked_by = repro-worker`).
5. Worker logs `Abandoning orchestration work item repro-1` repeatedly.
6. No rows appear in `dt.history` or `dt.new_tasks`.
7. Calling `GetOrchestrationStateAsync` throws:

```
System.IndexOutOfRangeException: Field not found in row: parent_instance_id
   at Npgsql.ThrowHelper.ThrowIndexOutOfRangeException(String message)
   at Npgsql.NpgsqlDataReader.GetOrdinal(String name)
   at DurableTask.PostgreSQL.PostgreSqlUtils.GetStringOrNull(NpgsqlDataReader reader, String columnName)
   at DurableTask.PostgreSQL.PostgreSqlUtils.GetOrchestrationState(NpgsqlDataReader reader)
   at DurableTask.PostgreSQL.PostgreSqlOrchestrationService.GetOrchestrationStateAsync(...)
```

---

## 3. Detailed Diagnosis

### 3.1 Defect 1 — `query_single_orchestration` Missing Column

**C# reader** (`src/DurableTask.PostgreSQL/PostgreSqlUtils.cs`, line 230):

```csharp
public static OrchestrationState GetOrchestrationState(NpgsqlDataReader reader)
{
    ParentInstance? parentInstance = null;
    string? parentInstanceId = GetStringOrNull(reader, "parent_instance_id");
    ...
}
```

The method is invoked by:

- `GetOrchestrationStateAsync(string instanceId, string? executionId)` at `PostgreSqlOrchestrationService.cs:856`  
  SQL: `SELECT * FROM dt.query_single_orchestration($1)`
- `GetManyOrchestrationsAsync(...)` at line 974  
  SQL: `SELECT * FROM dt.query_many_orchestrations(...)`
- `QueryOrchestrationsAsync(...)` at line 1014  
  SQL: `SELECT * FROM dt.query_many_orchestrations(...)`

**SQL function** (`src/DurableTask.PostgreSQL/Scripts/logic.postgresql.sql`, `query_single_orchestration`):

```sql
RETURNS TABLE (
    instance_id VARCHAR(100),
    execution_id VARCHAR(50),
    name VARCHAR(300),
    version VARCHAR(100),
    created_time TIMESTAMP WITH TIME ZONE,
    last_updated_time TIMESTAMP WITH TIME ZONE,
    completed_time TIMESTAMP WITH TIME ZONE,
    runtime_status VARCHAR(20),
    input_text JSONB,
    output_text JSONB,
    custom_status_text JSONB,
    trace_context VARCHAR(800)   -- <-- parent_instance_id is missing
)
```

In contrast, `query_many_orchestrations` correctly returns `parent_instance_id`.

#### Suggested Fix

Add `parent_instance_id VARCHAR(100)` to the `RETURNS TABLE` clause and to the inner `SELECT`:

```sql
CREATE OR REPLACE FUNCTION dt.query_single_orchestration(
    p_instance_id VARCHAR(100)
)
RETURNS TABLE (
    instance_id VARCHAR(100),
    execution_id VARCHAR(50),
    name VARCHAR(300),
    version VARCHAR(100),
    created_time TIMESTAMP WITH TIME ZONE,
    last_updated_time TIMESTAMP WITH TIME ZONE,
    completed_time TIMESTAMP WITH TIME ZONE,
    runtime_status VARCHAR(20),
    input_text JSONB,
    output_text JSONB,
    custom_status_text JSONB,
    parent_instance_id VARCHAR(100),   -- ADD
    trace_context VARCHAR(800)
)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_task_hub VARCHAR(50);
BEGIN
    v_task_hub := dt.current_task_hub();

    RETURN QUERY
    SELECT
        i.instance_id,
        i.execution_id,
        i.name,
        i.version,
        i.created_time,
        i.last_updated_time,
        i.completed_time,
        i.runtime_status,
        p_input.text AS input_text,
        p_output.text AS output_text,
        p_custom.text AS custom_status_text,
        i.parent_instance_id,              -- ADD
        i.trace_context
    FROM dt.instances i
    ...
END;
$$;
```

---

### 3.2 Defect 2 — `TaskHubName` from C# Settings Is Ignored

The C# settings class exposes `TaskHubName`:

```csharp
public sealed class PostgreSqlOrchestrationServiceSettings
{
    ...
    public string? TaskHubName { get; init; }
    ...
}
```

The constructor logs it as the active task hub:

```csharp
_logger.LogInformation(
    "PostgreSqlOrchestrationService initialized with TaskHub={TaskHub}, WorkerId={WorkerId}",
    _settings.TaskHubName ?? "default",
    _settings.WorkerId);
```

However, **the value is never propagated to PostgreSQL**. The schema hard-codes `TaskHubMode = 1` in `dt.global_settings`:

```sql
INSERT INTO dt.global_settings (name, value)
VALUES ('TaskHubMode', '1')
ON CONFLICT (name) DO NOTHING;
```

And `dt.current_task_hub()` resolves the task hub as follows:

```sql
IF v_task_hub_mode = '1' THEN
    v_task_hub := CURRENT_USER;
END IF;
```

Consequences:

- All instances in `dt.instances` have `task_hub = CURRENT_USER` (e.g. `root`, `postgres`) regardless of the configured `TaskHubName`.
- Multi-tenant or multi-hub scenarios are impossible because the C# setting is effectively a no-op.
- The log message `TaskHub=sample-hub` is misleading.

#### Suggested Fix

Option A (recommended): When `TaskHubName` is provided, append `Application Name=<TaskHubName>` to the connection string and use `TaskHubMode = 0`. Update the schema default or expose a setting to control `TaskHubMode`.

Option B: Keep mode 1 but ensure the PostgreSQL user name matches `TaskHubName`. This is operationally impractical for most consumers.

Option C: Add an explicit `SET dt.task_hub_name = '<TaskHubName>'` or similar session variable, and change `current_task_hub()` to read it before falling back to `CURRENT_USER` / `application_name`.

A minimal change would be in `PostgreSqlOrchestrationService` constructor:

```csharp
var builder = new NpgsqlConnectionStringBuilder(_settings.ConnectionString);
if (!string.IsNullOrEmpty(_settings.TaskHubName))
{
    builder.ApplicationName = _settings.TaskHubName;
}
_dataSource = NpgsqlDataSource.Create(builder.ConnectionString);
```

And in `DeploySchemaAsync`, set `TaskHubMode = '0'` when `TaskHubName` is configured, or simply change the default to `0` so `application_name` is authoritative.

---

### 3.3 Defect 3 — `DeploySchemaAsync` Cannot Update Functions with Return-Type Changes

`PostgreSqlOrchestrationService.DeploySchemaAsync` reads the embedded SQL files and executes them verbatim:

```csharp
await using (var cmd = new NpgsqlCommand(schemaSql, connection))
{
    await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
}
await using (var cmd = new NpgsqlCommand(logicSql, connection))
{
    await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
}
```

When a function already exists and the new definition changes the `RETURNS TABLE` shape, PostgreSQL rejects `CREATE OR REPLACE FUNCTION`:

```
Npgsql.PostgresException (0x80004005): 42P13: cannot change return type of existing function
DETAIL: Use DROP FUNCTION dt.query_single_orchestration(character varying) first.
```

This means `AutoDeploySchema = true` cannot be relied upon for upgrades.

#### Suggested Fix

Use idempotent deployment. The simplest robust pattern is to drop the schema (or at least the functions/types) before recreating them, guarded by `recreateInstanceStore`. A safer migration-friendly approach is to version the schema and apply delta scripts. At minimum, the provider should catch `42P13` and either:

- Drop each function before `CREATE OR REPLACE` when `AutoDeploySchema = true`, or
- Document that `AutoDeploySchema` only works for fresh databases and require manual schema migration.

Example snippet:

```csharp
await using var cmd = new NpgsqlCommand(
    $@"DROP FUNCTION IF EXISTS {schemaName}.query_single_orchestration(VARCHAR);",
    connection);
await cmd.ExecuteNonQueryAsync();
// then execute logic.postgresql.sql
```

---

### 3.4 Defect 4 — Orchestration Work Items Are Abandoned

This is the runtime failure that motivated the investigation. After the worker locks an orchestration and executes its `RunTask`, the dispatcher aborts the session and the provider logs:

```
[WRN] Abandoning orchestration work item <instance-id>
```

We verified:

- The orchestration code reaches `context.ScheduleTask<T>(...)`. We added logging around the call and it is executed.
- No exception is surfaced in the application logs from the orchestration or activity code.
- After abandonment, `dt.history` and `dt.new_tasks` remain empty.
- The instance row stays in `runtime_status = 'Pending'` with `locked_by` set.

Because the DTFx dispatcher catches exceptions internally and only emits the abandonment warning, the **inner exception is not visible to consumers**. Our hypothesis is that one of the following stored-procedure interactions fails inside the dispatcher:

1. `dt.checkpoint_orchestration` receives an event/parameter shape it does not expect (e.g. missing `parent_instance_id` in composite types, mismatched `version` field, or JSONB serialization issue).
2. `dt.lock_next_orchestration` returns a row shape that the C# side does not fully consume, causing a later read to fail.
3. The mismatch between C# `TaskHubName` and SQL `task_hub` causes the checkpoint to fail with "instance does not exist".

We were able to call `dt.checkpoint_orchestration` manually with synthetic parameters and it succeeded, so the procedure itself is not fundamentally broken. The failure is in the **data passed by the C# runtime** or in an **intermediate read** during the dispatcher loop.

#### Recommended Next Step for Maintainers

Add defensive logging inside `PostgreSqlOrchestrationService` around the checkpoint/complete paths (or expose an opt-in verbose mode). The inner exception must be captured and re-thrown or logged; the current behavior hides the real failure behind a generic abandonment warning.

---

## 4. Suggested Pull Request Checklist

A PR that fixes the observed issues should include:

1. **SQL fix**: Add `parent_instance_id` to `query_single_orchestration`.
2. **Task hub propagation**: Honor `TaskHubName` from C# settings by setting `Application Name` on the connection string and defaulting `TaskHubMode` to `0`, OR expose `TaskHubMode` as a first-class setting.
3. **Schema deployment robustness**: Make `AutoDeploySchema` idempotent for return-type changes (drop functions first or introduce versioning).
4. **Runtime diagnostics**: Wrap `checkpoint_orchestration`, `complete_tasks`, and similar calls in try/catch that logs the full PostgreSQL exception before rethrowing, so consumers can diagnose abandonment.
5. **Tests**:
   - Query state of an existing orchestration via `GetOrchestrationStateAsync`.
   - Create + complete an orchestration that schedules an activity.
   - Create two task hubs with different `TaskHubName` values on the same database and verify isolation.
   - Upgrade an existing schema (return-type change) with `AutoDeploySchema = true`.

---

## 5. Evidence Logs

### 5.1 Schema inspection

```sql
SELECT * FROM dt.global_settings;
--  name        | value | timestamp | last_modified_by
--  TaskHubMode | 1     | ...       | root

SELECT dt.current_task_hub();
--  current_task_hub
--  root

SELECT DISTINCT task_hub FROM dt.instances;
--  task_hub
--  root
```

### 5.2 Abandonment logs

```
[WRN] Abandoning orchestration work item ping-test-17bcae5ac1044e4a9d8fb914926854a4
[WRN] Abandoning orchestration work item cleanup-202606151859
[WRN] Abandoning orchestration work item reconciliation-202606151859
```

### 5.3 `GetOrchestrationStateAsync` exception

```
[ERR] An unhandled exception has occurred while executing the request.
System.IndexOutOfRangeException: Field not found in row: parent_instance_id
   at Npgsql.ThrowHelper.ThrowIndexOutOfRangeException(String message)
   at Npgsql.NpgsqlDataReader.GetOrdinal(String name)
   at DurableTask.PostgreSQL.PostgreSqlUtils.GetStringOrNull(NpgsqlDataReader reader, String columnName)
   at DurableTask.PostgreSQL.PostgreSqlUtils.GetOrchestrationState(NpgsqlDataReader reader)
   at DurableTask.PostgreSQL.PostgreSqlOrchestrationService.GetOrchestrationStateAsync(String instanceId, String executionId)
```

### 5.4 Schema upgrade failure

```
Npgsql.PostgresException (0x80004005): 42P13: cannot change return type of existing function
Hint: Use DROP FUNCTION dt.query_single_orchestration(character varying) first.
File: pg_proc.c
Line: 441
Routine: ProcedureCreate
```

---

## 6. Appendix — Files Involved

| File | Reason |
|------|--------|
| `src/DurableTask.PostgreSQL/Scripts/logic.postgresql.sql` | `query_single_orchestration` definition |
| `src/DurableTask.PostgreSQL/Scripts/schema.postgresql.sql` | `global_settings` seed and `current_task_hub()` |
| `src/DurableTask.PostgreSQL/PostgreSqlUtils.cs` | `GetOrchestrationState` column reader |
| `src/DurableTask.PostgreSQL/PostgreSqlOrchestrationService.cs` | Connection creation, schema deployment, checkpoint calls |
| `src/DurableTask.Core/TaskOrchestrationDispatcher.cs` | DTFx side that swallows the inner exception |

---

## 7. Contact

This report was produced during the Menshen Phase 1 Docker QA effort. We are happy to provide a reproducer repository, packet captures, or direct access to the failing environment if helpful.
