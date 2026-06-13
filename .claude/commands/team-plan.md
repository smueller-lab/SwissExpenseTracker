Create a structured team plan for the feature described in the arguments. Write the plan as a `.md` file in the project root.

## Output file

Name: `plan-<feature-slug>.md` where `<feature-slug>` is a short kebab-case name derived from the feature description (e.g. `plan-neon-data-source.md`, `plan-monthly-heatmap.md`).

## Before writing the plan

1. Read the relevant source files to understand what already exists and what must be created.
2. Identify the natural component boundaries: data model, adapter, pipeline stage, figure, layout+callback, etc.
3. Check `tests/` to understand what test infrastructure exists and what needs to be added.

## Plan file structure

### 1. Feature Overview

One paragraph: what is being built, why it's needed, which part of the app it touches.

### 2. Builder Tasks

One entry per distinct implementation component. Components that touch different files can run in parallel; components with dependencies must be sequenced. For each task:

```
#### Builder Task: <name>

- **Description**: Short description what the builder is doing and why it is doing that.
- **Files to create/modify**: exact relative paths from project root
- **Acceptance criteria**: measurable outcomes (e.g. "adapter sets amount = abs(value)", "layout row widths sum to 12")
- **Constraints**: what must NOT be done (e.g. "do not touch test files", "do not modify any DB schema file without user approval")
- **Blocked by**: other builder task names that must complete first (or "none")
```

### 3. Tester Task

One task covering all builder output:

```
#### Tester Task: Write tests for <feature>

- **Files to create/modify**: exact relative test file paths
- **Test scenarios**: bulleted list (e.g. "happy path parse", "nullable field receives empty string", "idempotency")
- **Test data**: sample files to add under `tests/test_data/` if needed (or "none")
- **Constraints**: do not modify any production code
- **Blocked by**: all builder task names
```

### 4. Validator Task

```
#### Validator Task: Validate <feature>

- **Files to check**: all files touched by builder and tester tasks
- **Checks**: ruff, black, mypy on all changed files; full pytest suite; project-convention scan
- **Constraints**: report issues only — do not modify any file
- **Blocked by**: tester task name
```

## After writing the plan

Tell the user:
- The path to the plan file.
- Run `/team-build plan-<feature-slug>.md` to create the task graph and dispatch the agents.
