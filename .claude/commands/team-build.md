Orchestrate the execution of a team plan. The argument is the path to a plan `.md` file (e.g. `plan-monthly-heatmap.md`).

You are the orchestrator. You coordinate agents; you do not write implementation code, tests, or validation yourself.

## Step 1 — Read the plan

Read the plan file in full. Extract:
- All builder tasks: name, files, acceptance criteria, constraints, inter-task dependencies.
- The tester task: name, files, test scenarios, constraints.
- The validator task: name, files, checks, constraints.

## Step 2 — Create the task graph

Use TaskCreate and TaskUpdate to wire up the full dependency chain before spawning any agents.

1. Create one Task per builder task. Record each task ID.
2. Create the tester Task. Use `addBlockedBy` with all builder task IDs.
3. Create the validator Task. Use `addBlockedBy` with the tester task ID.

## Step 3 — Dispatch builder agents

Spawn one `builder` agent per builder task. Builder tasks that have no inter-dependencies can run in parallel — spawn them in a single message as parallel Agent calls. Builder tasks that depend on other builder tasks must run sequentially.

Each agent prompt must include:
- The files to create or modify.
- The acceptance criteria from the plan.
- The constraints from the plan.
- The task ID to mark complete when done.

Wait for all builder agents to finish before proceeding.

## Step 4 — Dispatch the tester agent

Spawn a single `tester` agent with:
- All files created or modified by the builder agents.
- The test scenarios from the plan.
- Any test data files to add.
- The task ID to mark complete when done.

Wait for the tester agent to finish.

## Step 5 — Dispatch the validator agent

Spawn a `validator` agent with:
- The path to the plan file so it can check implementation against acceptance criteria.
- All files touched by builder and tester agents.
- Instruction to run the plan compliance check, ruff, black, mypy, the full pytest suite, and the project-convention scan.
- The task ID to mark complete when done.

## Step 6 — Handle the validator result

**If PASS**: announce the feature is complete; all checks and tests are clean. Stop.

**If FAIL**: triage each reported issue by type and route it to the correct agent:

| Issue type | Route to |
|------------|----------|
| Plan criterion not met — implementation missing or wrong | `builder` |
| ruff / black / mypy error | `builder` |
| Project-convention violation (style, grid width, y-axis range, etc.) | `builder` |
| Test failure (a test exists but produces the wrong result) | `builder` — the implementation is likely wrong |
| Missing test (a scenario from the plan is not covered) | `tester` |
| Test error caused by wrong test setup or fixture | `tester` |

Create one Task per distinct issue group (don't create one task per line — group issues that touch the same file or the same agent). Each task description must include: agent role, file(s), exact issue reported by the validator (line number where available), and what the fix must achieve.

After creating fix tasks:
1. Dispatch builder fix tasks first (parallel if independent), wait for completion.
2. Dispatch tester fix tasks next (parallel if independent), wait for completion.
3. Spawn a new `validator` agent over all affected files.
4. Return to the top of Step 6. Repeat until the validator reports PASS.

## Orchestrator rules

- Announce each phase transition: "Builders done.", "Tester done.", "Validator result: PASS / FAIL."
- Never spawn the builder agent for validation work, or the validator for implementation work.
- Never write production code or test code yourself — delegate to the appropriate agent.
- Keep task IDs updated: mark each task `in_progress` when its agent starts, `completed` when it finishes.
