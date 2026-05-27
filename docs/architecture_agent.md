# PIPELINE ARCHITECTURE v5 — AGENT REFERENCE
# SecFlow Entry Analyse
# Format: Structured for agent consumption. Use this as authoritative reference.
# Sections: STAGES | EXECUTION_FLOW | PARALLEL | SYNC | DB_SCHEMA | SKIP_CONDITIONS

---
## STAGES

---
STAGE: R1-W
ROLE: Worker
GRANULARITY: file
TRIGGER: task start, once per file
PURPOSE: Extract complete function list from source file. Use ctags for initial extraction, then LLM scans gap regions (line ranges not covered by ctags) to find missed function bodies. Write all functions to FuncDB.
INPUTS:
  - source file (read via tools)
  - prior R1-J feedback in FileState.r1_feedback (if retry)
DB_WRITES:
  - FuncDB ({file_hash}_functions.db): INSERT functions with name, signature, start_line, end_line, body, body_lines
FILE_WRITES:
  - r1-functions/{file_hash}_gaps.json: list of gap regions checked
  - stage-results/r1_w-worker-{fh}-a{n}.json/.txt: stage result index entry
STATE: FileState.r1_w_state=PASSED, r1_attempts++
RETRY_FROM: R1-J feedback (FileState.r1_feedback)
CONFIG: r1_max_rounds

---
STAGE: R1-J
ROLE: Judge
GRANULARITY: file
TRIGGER: R1-W completes
PURPOSE: Verify R1-W coverage: are all functions extracted, are gap regions properly handled? Fact-check against source file.
INPUTS:
  - FuncDB (current function list)
  - r1-functions/{file_hash}_gaps.json
  - stage-results/r1_w-worker-{fh}-a{n}.json (W result)
FILE_WRITES:
  - stage-results/r1_j-judge-{fh}-a{n}.json/.txt
  - feedback text → FileState.r1_feedback
ON_FAIL: feedback → R1-W retry
ON_PASS: function-level pipelines unlock
VERDICT_FORMAT: "通过: 是/否\n反馈: <text>"

---
STAGE: R2-J
ROLE: Judge (J-FIRST — no W prerequisite)
GRANULARITY: function
TRIGGER: R1-J passes, immediately per function
PURPOSE: Fact-check: verify that FuncDB start_line, end_line, name match the actual source file. This is verification only, not analysis.
INPUTS:
  - func_hash, name, start_line, end_line from FuncDB
  - source file (read to verify line content)
FILE_WRITES:
  - stage-results/r2_j-judge-{func}-a{n}.json/.txt
  - r1-functions/{func}_r2j_a{n}.txt: feedback detail
  - feedback inline → FunctionState.r2_j_feedback, r2_j_feedback_path
SPECIAL_VERDICT: "通过: 删除" → function is macro/non-existent → delete from FuncDB → force pass
ON_FAIL: feedback → R2-W
ON_PASS: R2 complete for this function (r2_done_count++)
CONFIG: r2_max_rounds

---
STAGE: R2-W
ROLE: Worker (on-demand only, triggered by R2-J failure)
GRANULARITY: function
TRIGGER: R2-J fails
PURPOSE: Using R2-J feedback, locate the correct function boundaries in source file and write corrected values back to FuncDB. Sync corrections to FunctionState.
INPUTS:
  - func_hash, current wrong start_line/end_line/name
  - R2-J feedback: r1-functions/{func}_r2j_a{n}.txt
  - source file
DB_WRITES:
  - FuncDB: UPDATE start_line, end_line, name (corrected values)
  - FunctionState.start_line, end_line, name synced after write
FILE_WRITES:
  - stage-results/r2_w-worker-{func}-a{n}.json/.txt
ON_COMPLETE: R2-J re-verify
CONFIG: r2_max_rounds

---
STAGE: R3-W
ROLE: Worker
GRANULARITY: function
TRIGGER: R2-J passes (runs concurrently with CC — does NOT wait for CC)
PURPOSE: Analyze whether function receives external input. Determine: has_external_input, decision (keep/filter), taints (parameter names carrying external data), tag (P=passive param / A=active read), entry_role, entry_reason, taint_details. This is the primary entry-point identification stage.
INPUTS:
  - func_hash, name, signature, start_line, end_line from FuncDB
  - source file body
  - prior R3-J feedback: r1-functions/{func}_r3j_a{n}.txt (if retry)
DB_WRITES (PRIMARY):
  - FuncDB {file_hash}_functions.db: UPDATE analysis(JSON), has_external_input, entry_role, r3_decision (own row only)
  - FuncDB: UPDATE analysis field
  - FunctionState.has_external_input, r4_decision set
FILE_WRITES:
  - stage-results/r3_w-worker-{func}-a{n}.json/.txt
RESULT_SCHEMA (written to stage-result and FuncDB.analysis):
  has_external_input: bool  [REQUIRED]
  decision: "keep" | "filter"  [REQUIRED]
  tag: "P" | "A"  [REQUIRED if decision=keep]
  entry_role: "boundary"|"callback"|"dispatch_target"|"ipc_handler"  [REQUIRED if decision=keep]
  taints: [str]  [REQUIRED if decision=keep AND function has parameters]
  entry_source_lines: [int]
  function_description: str
  entry_reason: str
  taint_details: [{param, source, description}]
  justification: str
CONFIG: r2_max_rounds (NOTE: to be renamed r3_max_rounds)

---
STAGE: R3-J
ROLE: Judge
GRANULARITY: function
TRIGGER: R3-W completes
PURPOSE: Validate R3-W analysis result. Key checks: (1) has_external_input consistent with decision, (2) taints non-empty for parameterized functions with has_external_input=true, (3) tag in {P,A}, (4) entry_role valid, (5) decision field present.
INPUTS:
  - func_hash, FuncDB.analysis (R3-W result, read from this function's FuncDB)
  - FuncDB (signature for parameter check)
  - source file
  - stage-results/r3_w-worker-{func}-a{n}.json
ENGINE_HARD_VALIDATION: has_external_input=true AND taints=[] AND function has params → force fail regardless of J output
FILE_WRITES:
  - stage-results/r3_j-judge-{func}-a{n}.json/.txt
  - r1-functions/{func}_r3j_a{n}.txt: feedback
  - FunctionState.r3_j_feedback_path
ON_FAIL: r3_w_state=PENDING, r3_w_feedback=feedback_path → R3-W retry
ON_PASS: r3_j_state=PASSED
CONFIG: r2_j_max_rounds (NOTE: to be renamed r3_j_max_rounds)

---
STAGE: CC
ROLE: Static analysis (no LLM)
GRANULARITY: module, once globally
TRIGGER: all_r2_done_event (all_r1_done=True AND r2_done_count >= total_funcs)
PURPOSE: Build call graph for entire module. Extract call edges from source using static analysis (regex). Build: nodes, directed edges, transitive closure, entry_trees. No LLM involved.
INPUTS:
  - all source files in module
  - all r1-functions/*.db (FuncDB, for function inventory)
DB_WRITES (PRIMARY):
  - CallchainDB (callchain/callchain.db):
    nodes: func_hash, name, signature, file_hash, is_r3_entry, entry_role, entry_confidence
    edges: caller_hash, callee_hash, call_site_line, call_type
    closure: ancestor, descendant, depth
    entry_trees: root_hash, node_hash, depth, path_json
EMITS: cc_done_event → unlocks all pending R4-W
STATE: PipelineState.cc_state=PASSED

---
STAGE: R4-W
ROLE: Worker
GRANULARITY: function
TRIGGER: R3-J passes (r4_decision=="keep") AND cc_done_event received
SKIP_CONDITION: r4_decision=="filter" from R3
NOTE: Applies to ALL modules including single-file. Within a single file, if A→B and both are R3-kept, R4 must determine B is an internal callee, not an independent entry.
PURPOSE: Using CallchainDB, determine if this function is a genuine independent entry point or is already covered by an outer R3-kept entry. If an ancestor entry calls this function (A→B, A is kept), this function should be filtered.
INPUTS:
  - FuncDB analysis fields for this function (from R3, read from its FuncDB)
  - CallchainDB: get_callers(), get_ancestors(), get_r3_callers()
  - List of all current R3-kept function names (for context)
DB_WRITES (PRIMARY):
  - FuncDB {file_hash}_functions.db: UPDATE r4_decision ('keep' or 'filter') (own row only)
FILE_WRITES:
  - r4-module/r4-func-{func_hash}.json: {decision, reason} (for R4-J to read)
  - stage-results/r4_func_w-worker-{func}-a{n}.json/.txt
STATE: FunctionState.r4_decision may update to 'filter'
CONFIG: r4_func_max_rounds

---
STAGE: R4-J
ROLE: Judge
GRANULARITY: function
TRIGGER: R4-W completes
SKIP_CONDITION: same as R4-W
STATUS: ⚠️ NOT YET IMPLEMENTED — required by architecture
PURPOSE: Validate R4-W decision. A 'filter' verdict must cite the specific ancestor entry that covers this function. A 'keep' verdict must show no R3-kept ancestor calls this function.
INPUTS:
  - r4-module/r4-func-{func_hash}.json (R4-W result)
  - CallchainDB: get_r3_callers(), get_ancestors() (to verify claim)
FILE_WRITES:
  - stage-results/r4_j-judge-{func}-a{n}.json/.txt
  - feedback inline
ON_FAIL: feedback → R4-W retry (r4_w_state reset)
ON_PASS: FunctionState.r4_state=PASSED → R5 unlocks
CONFIG: r4_func_j_max_rounds (new config key, to be added)

---
STAGE: R5-W
ROLE: Worker
GRANULARITY: function
TRIGGER: r4_decision=="keep" AND r4_state==PASSED
PURPOSE: Generate detailed Markdown report for this entry point. Contents: function purpose, how external input enters, taint parameter details, caller/callee context from callchain.
INPUTS:
  - FuncDB.analysis (R3 analysis data, read from this function's own FuncDB)
  - CallchainDB: get_callers(), get_callees()
  - prior R5-J feedback (if retry)
FILE_WRITES (PRIMARY):
  - output/reports/{func_hash}.md
  - stage-results/r5_w-worker-{func}-a{n}.json/.txt
STATE: FunctionState.r5_state=RUNNING, r5_attempts++
CONFIG: report_func_max_rounds

---
STAGE: R5-J
ROLE: Judge
GRANULARITY: function
TRIGGER: R5-W completes
PURPOSE: Validate report quality: file exists, required sections present, taint analysis matches FuncDB.analysis data, function description accurate.
INPUTS:
  - output/reports/{func_hash}.md
  - stage-results/r5_w-worker-{func}-a{n}.json
FILE_WRITES:
  - stage-results/r5_j-judge-{func}-a{n}.json/.txt
  - feedback
ON_FAIL: feedback → R5-W retry
ON_PASS: FunctionState.r5_state=PASSED, r5_path=reports/{func_hash}.md
CONFIG: report_func_max_rounds

---
STAGE: R6
ROLE: Script (no LLM)
GRANULARITY: module, once after ALL _func_pipeline coroutines complete
PURPOSE: Iterate all FuncDBs (r1-functions/*.db), collect keep entries, validate field completeness, generate three output artifacts. No LLM. No retries.
INPUTS (DB PRIMARY):
  - All FuncDBs (r1-functions/*.db): iterate each file's FuncDB, collect rows where r3_decision='keep' AND (r4_decision IS NULL OR r4_decision='keep')
  - output/reports/*.md (R5 per-function reports, for final_report.md assembly)
FILE_WRITES (ALL PRIMARY):
  - output/functions.list: final entry JSON array (canonical delivery format)
  - output/entry-details.json: same content, for frontend
  - output/final_report.md: assembled from R5 reports + module summary header
STATE: PipelineState.r6_state=PASSED
NOTE: Each function's pipeline writes only its own FuncDB row. R6 aggregates by iterating all FuncDBs.

---
## EXECUTION_FLOW

FUNCTION_SEQUENTIAL_ORDER:
  1. R2-J  (J-first, fact-check)
     └─ fail → R2-W → R2-J  (loop)
  2. R3-W → R3-J
     └─ fail → R3-W → R3-J  (loop)
  [CONCURRENT WITH CC — no wait here]
  3. await cc_done_event  ← only blocking point
  4. R4-W → R4-J
     └─ fail → R4-W → R4-J  (loop)
  5. R5-W → R5-J
     └─ fail → R5-W → R5-J  (loop)

FILE_SEQUENTIAL_ORDER:
  R1-W → R1-J  (loop on fail)
  └─ pass → launch all functions concurrently

MODULE_SEQUENTIAL_ORDER:
  all _complete_file_pipeline done → R6 → delivery artifacts

---
## PARALLEL

TOP_LEVEL:
  asyncio.gather(
    cc_phase,          # global, waits for all_r2_done_event
    file_pipeline_1,   # fully independent per-file coroutines
    file_pipeline_2,
    ...
  )

FILE_INTERNAL:
  after R1-J passes:
  asyncio.gather(func_1, func_2, ..., func_N)
  all functions in same file run concurrently

FUNCTION_INTERNAL:
  fully sequential — no internal concurrency within _func_pipeline

R3_VS_CC:
  R3-W and CC run simultaneously. R3 does NOT wait for CC.
  Each function independently awaits cc_done_event after its own R3-J passes.

---
## SYNC_SIGNALS

SIGNAL: all_r2_done_event
  SET_WHEN: all_r1_done_flag=True AND r2_done_count >= total_funcs
  NOTE: r2_done_count increments immediately after each function's R2-J completes (pass or force-pass)
  EFFECT: CC phase begins building callchain.db

SIGNAL: cc_done_event
  SET_WHEN: CC build completes OR resume (callchain.db already exists)
  EFFECT: each function's _func_pipeline unblocks at 'await cc_done_event'
  NOTE: all functions independently await same event; no ordering among them after unblock

SIGNAL: asyncio.gather() returns
  SET_WHEN: all _complete_file_pipeline and _cc_phase coroutines return
  EFFECT: R6 script begins execution

---
## DB_SCHEMA

FuncDB (per-file, r1-functions/{file_hash}_functions.db):
  functions: func_hash PK, file_hash, name, signature, start_line, end_line,
             body TEXT, body_lines, analysis JSON, has_external_input,
             entry_role, entry_confidence, updated_at
  file_meta: file_hash PK, original_path, rel_path, basename, total_funcs

# ModuleDB: DEPRECATED — removed from pipeline. Use FuncDB only.

CallchainDB (global, callchain/callchain.db):
  nodes: func_hash PK, name, signature, file_hash, start_line, is_r3_entry,
         is_external, entry_role, entry_confidence, created_at
  edges: caller_hash, callee_hash, call_site_line, call_type
         call_type: 'direct'|'ptr'|'extern_table'
  closure: ancestor, descendant, depth (transitive closure, O(1) reachability)
  entry_trees: root_hash, node_hash, depth, path_json (subtree from R3 entries)
  build_status: phase, total_nodes, total_edges, has_cycles

StageResultIndex (MySQL, AppEaStageResultIndex):
  task_id, stage_key, role_kind(worker/judge), scope_kind(file/func/module),
  attempt, file_hash, func_hash, status, passed, summary,
  result_file_path, raw_file_path

---
## SKIP_CONDITIONS

R2-W: skip if R2-J passes on first attempt
R4-W, R4-J: skip if r4_decision=="filter" (set by R3-W/J)
R5-W, R5-J: skip if r4_decision != "keep" OR r4_state != PASSED
R6: if no FuncDB rows have r3_decision='keep' → r6_state=PASSED, return []

---
## STATE_MACHINE

All stages: PENDING → RUNNING → PASSED | FAILED → (retry or force-pass)
Force-pass on max_rounds exceeded: prevents blocking downstream ("no false negatives")

FUNCTION_STATE_FIELDS:
  r2_j_state, r2_j_attempts, r2_j_feedback, r2_j_feedback_path
  r2_w_state, r2_w_attempts
  r3_w_state, r3_w_attempts, r3_w_feedback, has_external_input
  r3_j_state, r3_j_attempts, r3_j_feedback_path, r3_j_feedback_summary
  entry_role
  r4_decision (set by R3-W: "keep"/"filter", may be updated by R4-W/J)
  r4_note, r4_state, r4_attempts  [r4_state=PASSED only after R4-J passes]
  r5_state, r5_attempts, r5_path

FILE_STATE_FIELDS:
  r1_w_state, r1_j_state, r1_attempts, r1_feedback

MODULE_STATE_FIELDS:
  cc_state, cc_attempts
  r6_state, r6_attempts, r6_feedback

---
## ARTIFACTS

### STAGE_ARTIFACTS

STAGE: R1-W
  DB_PRIMARY:
    FuncDB {file_hash}_functions.db: INSERT functions(name, signature, start_line, end_line, body, body_lines)
  FILE_PRIMARY:
    r1-functions/{fh}_gaps.json: gap region list
  STAGE_RESULT: stage-results/r1_w-worker-{fh}-a{n}.json/.txt
  DOWNSTREAM: R1-J reads FuncDB + gaps.json + stage-result

STAGE: R1-J
  DB_PRIMARY: none
  FILE_PRIMARY: feedback text → FileState.r1_feedback (inline)
  STAGE_RESULT: stage-results/r1_j-judge-{fh}-a{n}.json/.txt
  DOWNSTREAM: R1-W retry uses FileState.r1_feedback

STAGE: R2-J
  DB_PRIMARY: none
  FILE_PRIMARY: r1-functions/{func}_r2j_a{n}.txt (feedback detail)
  STATE_WRITES: FunctionState.r2_j_feedback (inline), r2_j_feedback_path
  STAGE_RESULT: stage-results/r2_j-judge-{func}-a{n}.json/.txt
  DOWNSTREAM: R2-W reads r2_j_feedback_path

STAGE: R2-W
  DB_PRIMARY:
    FuncDB: UPDATE start_line, end_line, name (corrected)
    FunctionState: sync start_line, end_line, name
  FILE_PRIMARY: none
  STAGE_RESULT: stage-results/r2_w-worker-{func}-a{n}.json/.txt
  DOWNSTREAM: R2-J re-reads FuncDB to verify corrected values

STAGE: R3-W
  DB_PRIMARY:
    FuncDB {file_hash}_functions.db: UPDATE analysis(JSON), has_external_input, entry_role, r3_decision
    NOTE: each function writes only its own row in its own FuncDB
  STATE_WRITES: FunctionState.has_external_input, r4_decision
  FILE_PRIMARY: none
  STAGE_RESULT: stage-results/r3_w-worker-{func}-a{n}.json/.txt
  DOWNSTREAM:
    R3-J reads stage-result + FuncDB.analysis + FuncDB(signature)
    R4-W reads FuncDB (analysis, r3_decision) for this function
    R5-W reads FuncDB (analysis) for this function
    R6 iterates all FuncDBs to aggregate final entries

STAGE: R3-J
  DB_PRIMARY: none
  FILE_PRIMARY: r1-functions/{func}_r3j_a{n}.txt (feedback)
  STATE_WRITES: FunctionState.r3_j_feedback_path, r3_j_feedback_summary
  STAGE_RESULT: stage-results/r3_j-judge-{func}-a{n}.json/.txt
  DOWNSTREAM: R3-W retry reads r3_j_feedback_path

STAGE: CC
  DB_PRIMARY:
    CallchainDB callchain/callchain.db:
      nodes(func_hash, name, signature, file_hash, is_r3_entry, entry_role)
      edges(caller_hash, callee_hash, call_site_line, call_type)
      closure(ancestor, descendant, depth)
      entry_trees(root_hash, node_hash, depth, path_json)
  FILE_PRIMARY: none
  STAGE_RESULT: none (no LLM)
  DOWNSTREAM:
    R4-W reads CallchainDB.get_callers(), get_ancestors(), get_r3_callers()
    R4-J reads CallchainDB to verify R4-W claims
    R5-W reads CallchainDB.get_callers(), get_callees()

STAGE: R4-W
  DB_PRIMARY:
    FuncDB {file_hash}_functions.db: UPDATE r4_decision ('keep' or 'filter')
    NOTE: each function writes only its own row
  FILE_PRIMARY:
    r4-module/r4-func-{func_hash}.json: {decision, reason}  ← for R4-J to read
  STAGE_RESULT: stage-results/r4_func_w-worker-{func}-a{n}.json/.txt
  DOWNSTREAM:
    R4-J reads r4-func-*.json + CallchainDB
    R6 reads FuncDB.r4_decision when iterating all FuncDBs

STAGE: R4-J
  DB_PRIMARY: none
  FILE_PRIMARY: feedback inline → FunctionState.r4_j_feedback
  STATE_WRITES: FunctionState.r4_j_state; r4_state=PASSED only when J passes
  STAGE_RESULT: stage-results/r4_j-judge-{func}-a{n}.json/.txt
  DOWNSTREAM: R4-W retry uses r4_j_feedback; R5-W unlocks when r4_state==PASSED

STAGE: R5-W
  DB_PRIMARY: none
  FILE_PRIMARY: output/reports/{func_hash}.md  ← PRIMARY ARTIFACT
  STAGE_RESULT: stage-results/r5_w-worker-{func}-a{n}.json/.txt
  DOWNSTREAM: R5-J reads report file + stage-result

STAGE: R5-J
  DB_PRIMARY: none
  FILE_PRIMARY: feedback inline
  STATE_WRITES: FunctionState.r5_state=PASSED, r5_path
  STAGE_RESULT: stage-results/r5_j-judge-{func}-a{n}.json/.txt
  DOWNSTREAM: R5-W retry uses feedback; R6 reads output/reports/*.md

STAGE: R6
  INPUTS:
    All FuncDBs (r1-functions/*.db): iterate, collect r3_decision='keep' AND (r4_decision IS NULL OR r4_decision='keep')
    output/reports/*.md: R5 per-function reports
  FILE_PRIMARY (ALL FINAL DELIVERY):
    output/functions.list: final entry JSON array
    output/entry-details.json: same, for frontend
    output/final_report.md: assembled R5 reports + module summary
  DOWNSTREAM: external consumers only (frontend / downstream services)

---
### W_TO_J_TRANSFER

R1: W→J: FuncDB + gaps.json + stage-result-file
    J→W: FileState.r1_feedback (inline text)

R2: W→J: FuncDB (corrected values) + stage-result-file
    J→W: r2j_a{n}.txt (path) + FunctionState.r2_j_feedback (inline)

R3: W→J: FuncDB.analysis + stage-result-file + FuncDB (signature for param check)
    J→W: r3j_a{n}.txt (path) + FunctionState.r3_j_feedback_path

R4: W→J: r4-func-{func}.json + CallchainDB (shared read)
    J→W: FunctionState.r4_j_feedback (inline)

R5: W→J: output/reports/{func}.md + stage-result-file
    J→W: FunctionState feedback inline + r5_j-judge-{func}-a{n}.json

---
### CROSS_STAGE_DATA_FLOW

FuncDB({file_hash}_functions.db):
  WRITE: R1-W (create: name/sig/lines/body)
         R2-W (correct: start_line/end_line/name)
         R3-W (analysis/has_external_input/entry_role/r3_decision — own row only)
         R4-W (r4_decision — own row only)
  READ:  R2-J (verify), R3-W (body/signature), R3-J (signature), CC (inventory)
         R4-W (this function's info), R5-W (analysis data)
         R6 (iterate all FuncDBs to aggregate final entries)

# ModuleDB: DEPRECATED — removed from pipeline

CallchainDB(callchain/callchain.db):
  WRITE: CC (nodes, edges, closure, entry_trees)
  READ:  R4-W (callers/ancestors), R4-J (verify), R5-W (call context)

output/reports/{func_hash}.md:
  WRITE: R5-W
  READ:  R6 (assemble final_report.md)

output/functions.list + entry-details.json + final_report.md:
  WRITE: R6 (final delivery, immutable after write)

---
### SESSION_FILES

NOTE: Worker sessions reuse across retries (append to same file).
      Judge sessions create new file per attempt.

r1-w-{fh}.jsonl          R1-W   reuse across retries
r1-j-{fh}-a{n}.jsonl     R1-J   new per attempt
r2-j-{func}-a{n}.jsonl   R2-J   new per attempt
r2-w-{func}.jsonl         R2-W   reuse across retries
r3-w-{fh}-{func}.jsonl   R3-W   reuse across retries
r3-j-{func}-a{n}.jsonl   R3-J   new per attempt
r4-func-w-{func}.jsonl   R4-W   reuse across retries
r4-func-j-{func}-a{n}.jsonl  R4-J  new per attempt
r5-w-{func}.jsonl         R5-W   reuse across retries
r5-j-{func}-a{n}.jsonl   R5-J   new per attempt
