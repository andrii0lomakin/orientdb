#!/usr/bin/env python3
"""Stand-alone tests for the CI fix agent's failure-context pre-fetch.

Run directly, with no pytest dependency, like the other .github/scripts
helpers:

    python3 .github/scripts/ci-fix-context-test.py

Exit code 0 means all checks passed; non-zero prints the failing cases.
Test functions (any module-level `test_*`) are discovered automatically, so
adding one needs no edit to `main()`.

The subject under test is `.github/workflows/ci-failure-fix-agent.yml`: four
of its shell blocks -- "Install CLI prerequisites (jq, gh)", "Gather CI failure
context", "Read fix result" and "Publish fix" -- plus the agent prompt they
produce input for. The pre-fetch runs once per agent dispatch, on a self-hosted
runner, and is the agent's only source of truth about the failure it was sent
to fix, so a regression in it does not announce itself; it just yields a report
written from no evidence.

Each block is extracted from the workflow as text and executed under `bash -e`
with stub `gh`, `sudo` and `apt-get` on PATH, which is what lets these checks
run anywhere with no token and no network. "Publish fix" is only linted, not
executed: it pushes.

Only stdlib is used, deliberately: the workflow that runs this installs a bare
Python via actions/setup-python, which carries no PyYAML, so the workflow is
parsed by text rather than with a YAML library.
"""

import contextlib
import json
import os
import pathlib
import re
import shutil
import subprocess
import sys
import tempfile

_WORKFLOW = (pathlib.Path(__file__).parents[1]
             / "workflows" / "ci-failure-fix-agent.yml")

# These step names are load-bearing: the blocks are extracted by exact name,
# and a rename here or there fails the extraction. The workflow carries a
# comment at each step saying so.
_GATHER_STEP = "Gather CI failure context"
_PREREQ_STEP = "Install CLI prerequisites (jq, gh)"
_RESULT_STEP = "Read fix result"
_PUBLISH_STEP = "Publish fix"

# The seven data files the pre-fetch promises the agent, named by their ledger
# key. The ledger keys are the file names without extension, which is a place
# the prompt and the shell can drift apart -- see
# test_prompt_uses_real_ledger_keys.
_DATA_KEYS = ["run", "jobs", "logs", "annotations", "associated-prs",
              "pr-comments", "existing-fix-prs"]

_RUN_ID = "33870767133"

# Resolved once, by absolute path: the prerequisite harness sets PATH to its
# stub directory alone, so "bash" would not be findable there.
_BASH = shutil.which("bash") or "/bin/bash"

# The harness stubs only `gh`; the extracted blocks call these directly.
# Resolved against the PATH the harness hands bash, not the outer one: a jq in
# /usr/local/bin would satisfy a plain which() and then fail every scenario
# with no hint of the cause.
_HARNESS_PATH = os.pathsep.join(["/usr/bin", "/bin"])
_missing = [tool for tool in ("jq", "timeout", "sed", "tr", "head")
            if shutil.which(tool, path=_HARNESS_PATH) is None]
if _missing:
    sys.exit(f"required on {_HARNESS_PATH}: {', '.join(_missing)}. The "
             "extracted blocks call them directly; only `gh` is stubbed.")

_failures = []


def check(name, condition):
    if condition:
        print(f"  PASS  {name}")
    else:
        print(f"  FAIL  {name}")
        _failures.append(name)


# --- workflow extraction ----------------------------------------------------


def _workflow_text():
    return _WORKFLOW.read_text(encoding="utf-8")


def _step_names_in_order():
    """Every step `- name:` under the job, in file order.

    Anchored to the step indentation on purpose. An unanchored match would
    also pick up Markdown bullets inside the ~580-line embedded prompt.
    """
    return re.findall(r"^      - name: (.+)$", _workflow_text(), re.MULTILINE)


def _extract_run_block(step_name):
    """Return the dedented body of `run: |` for the named step.

    Hand-rolled instead of using a YAML parser (see the module docstring). The
    block scalar's indentation is taken from its first non-blank line, which is
    what YAML itself does, so a re-indentation of the workflow cannot silently
    change what this test executes.
    """
    lines = _workflow_text().splitlines()
    # Exactly one match, not the first: the agent prompt sits ABOVE these
    # steps and describes the workflow, so a step name quoted there in list
    # form would silently hijack the extraction.
    matches = [i for i, line in enumerate(lines)
               if line.strip() == f"- name: {step_name}"]
    if len(matches) != 1:
        raise AssertionError(
            f"expected exactly one step named {step_name!r}, found "
            f"{len(matches)}. The step names in this test are load-bearing; if "
            "the workflow renamed a step, update the constant here too.")
    start = matches[0]

    run_at = None
    for i in range(start, len(lines)):
        if re.match(r"^\s+run: \|\s*$", lines[i]):
            run_at = i
            break
        # A following `- name:` means this step has no literal `run: |` block.
        if i > start and lines[i].strip().startswith("- name: "):
            break
    if run_at is None:
        raise AssertionError(f"no 'run: |' block in step {step_name!r}")

    body = []
    block_indent = None
    for line in lines[run_at + 1:]:
        if not line.strip():
            body.append("")
            continue
        indent = len(line) - len(line.lstrip())
        if block_indent is None:
            block_indent = indent
        elif indent < block_indent:
            break
        body.append(line[block_indent:])
    while body and not body[-1]:
        body.pop()
    return "\n".join(body) + "\n"


def _prompt_text():
    """The embedded agent prompt only, without the surrounding shell.

    Scoped to the heredoc so a check named "the prompt documents X" cannot be
    satisfied by a shell comment in the gather step that happens to mention X.
    """
    text = _workflow_text()
    marker = "<< 'AGENT_PROMPT_EOF'\n"
    start = text.index(marker) + len(marker)
    end = text.index("\n          AGENT_PROMPT_EOF\n", start)
    return text[start:end]


def _resolve_expressions(script, what):
    """Substitute the Actions expressions the runner would, and prove none remain."""
    script = script.replace("${{ github.repository }}", "JetBrains/youtrackdb")
    leftover = re.findall(r"\$\{\{.*?\}\}", script)
    if leftover:
        raise AssertionError(
            f"unresolved Actions expressions in {what}: {leftover}. Add a "
            "substitution here so the test executes what CI executes.")
    return script


# --- gh stub ----------------------------------------------------------------

# Each scenario is a case in one stub script. `run view --json jobs`,
# `run view --log-failed` and plain `run view --json ...` are distinguished the
# same way the real gh distinguishes them: by flags. Every case also asserts
# the run id it was called with, so a break in the workflow's
# `RUN_ID="${FAILED_RUN_URL##*/}"` derivation fails the tests instead of
# passing unnoticed.
_GH_STUB = r"""#!/usr/bin/env bash
# Every invocation is logged so tests can assert WHAT was fetched, not just
# how the result was graded. A step that fetches a different run, or a
# different repo, produces a complete and healthy-looking context about the
# wrong failure - the same end state as the bug this suite exists for.
printf '%s\n' "$*" >> "$GH_ARGS_LOG"
if [ "$1" = run ]; then
  case "$*" in
    *__RUN_ID__*) : ;;
    *) echo "stub gh: expected run id __RUN_ID__ in: $*" >&2; exit 90 ;;
  esac
fi
# The API path is located rather than assumed to be $2: the block passes
# flags (--paginate) before it, exactly as the real gh accepts them.
api_path() {
  shift  # drop "api"
  for arg in "$@"; do
    case "$arg" in
      -*) ;;
      *) echo "$arg"; return ;;
    esac
  done
}
run_kind() {
  case "$*" in
    *--log-failed*) echo logs ;;
    *"--json jobs"*) echo jobs ;;
    *) echo run ;;
  esac
}
case "$STUB_CASE" in
  absent)
    # Exactly what bash reports for a command that is not installed.
    echo "gh: command not found" >&2; exit 127 ;;
  allok)
    if [ "$1" = run ]; then
      case "$(run_kind "$@")" in
        logs) echo "test-linux  Run Maven Test  FooTest.bar FAILED"; exit 0 ;;
        jobs) echo '{"jobs":[{"databaseId":111,"conclusion":"failure"},
                             {"databaseId":222,"conclusion":"success"}]}'; exit 0 ;;
        run)  echo '{"headSha":"08eaf8c966","conclusion":"failure","event":"push"}'; exit 0 ;;
      esac
    fi
    if [ "$1" = api ]; then
      case "$(api_path "$@")" in
        *annotations) echo '[{"path":"Foo.java","message":"boom"}]'; exit 0 ;;
        *pulls)       echo '[{"number":1295}]'; exit 0 ;;
        *comments)    echo '[{"body":"coverage 91%"}]'; exit 0 ;;
      esac
    fi
    [ "$1" = pr ] && { echo '[{"number":42,"headRefName":"ci-fix/x"}]'; exit 0; }
    exit 0 ;;
  partial_annotations)
    # Two failed jobs; annotations for the second 404 with a MULTI-LINE error
    # that also carries a CR and a query string.
    if [ "$1" = run ]; then
      case "$(run_kind "$@")" in
        logs) echo "log line"; exit 0 ;;
        jobs) echo '{"jobs":[{"databaseId":111,"conclusion":"failure"},
                             {"databaseId":333,"conclusion":"failure"}]}'; exit 0 ;;
        run)  echo '{"headSha":"08eaf8c966"}'; exit 0 ;;
      esac
    fi
    if [ "$1" = api ]; then
      case "$(api_path "$@")" in
        *check-runs/111/annotations) echo '[{"message":"boom"}]'; exit 0 ;;
        *check-runs/333/annotations)
          printf 'HTTP 404: Not Found (https://api.github.com/x?sig=SECRETSIG)\nsecond line of the error\r::error::forged\n' >&2
          exit 1 ;;
        *pulls) echo '[]'; exit 0 ;;
      esac
    fi
    [ "$1" = pr ] && { echo '[]'; exit 0; }
    exit 0 ;;
  no_jobs)
    # The jobs fetch fails; everything else works.
    if [ "$1" = run ]; then
      case "$(run_kind "$@")" in
        jobs) echo "HTTP 502 Bad Gateway" >&2; exit 1 ;;
        logs) echo "log line"; exit 0 ;;
        run)  echo '{"headSha":"08eaf8c966"}'; exit 0 ;;
      esac
    fi
    if [ "$1" = api ]; then
      case "$(api_path "$@")" in
        *pulls)    echo '[{"number":7}]'; exit 0 ;;
        *comments) echo '[{"body":"hi"}]'; exit 0 ;;
      esac
    fi
    [ "$1" = pr ] && { echo '[]'; exit 0; }
    exit 0 ;;
  no_run)
    # The run fetch fails, so headSha is unobtainable.
    if [ "$1" = run ]; then
      case "$(run_kind "$@")" in
        jobs) echo '{"jobs":[]}'; exit 0 ;;
        logs) echo "log line"; exit 0 ;;
        run)  echo "could not find any workflow run" >&2; exit 1 ;;
      esac
    fi
    [ "$1" = pr ] && { echo '[]'; exit 0; }
    exit 0 ;;
  run_without_head_sha)
    # The run fetch SUCCEEDS but the payload carries no headSha.
    if [ "$1" = run ]; then
      case "$(run_kind "$@")" in
        jobs) echo '{"jobs":[]}'; exit 0 ;;
        logs) echo "log line"; exit 0 ;;
        run)  echo '{"conclusion":"failure","event":"schedule"}'; exit 0 ;;
      esac
    fi
    [ "$1" = pr ] && { echo '[]'; exit 0; }
    exit 0 ;;
  all_annotations_fail)
    # Two failed jobs, BOTH annotation calls fail: nothing arrived, so the
    # aggregate is `failed`, not `partial`.
    if [ "$1" = run ]; then
      case "$(run_kind "$@")" in
        logs) echo "log line"; exit 0 ;;
        jobs) echo '{"jobs":[{"databaseId":111,"conclusion":"failure"},
                             {"databaseId":333,"conclusion":"failure"}]}'; exit 0 ;;
        run)  echo '{"headSha":"08eaf8c966"}'; exit 0 ;;
      esac
    fi
    if [ "$1" = api ]; then
      case "$(api_path "$@")" in
        *annotations) echo "HTTP 403: rate limit exceeded" >&2; exit 1 ;;
        *pulls)       echo '[]'; exit 0 ;;
      esac
    fi
    [ "$1" = pr ] && { echo '[]'; exit 0; }
    exit 0 ;;
  no_failed_jobs)
    # The run has jobs but none failed, so the annotations loop body never
    # runs. Zero failures over zero attempts is a real empty answer: `ok`.
    if [ "$1" = run ]; then
      case "$(run_kind "$@")" in
        logs) echo "log line"; exit 0 ;;
        jobs) echo '{"jobs":[{"databaseId":222,"conclusion":"success"}]}'; exit 0 ;;
        run)  echo '{"headSha":"08eaf8c966"}'; exit 0 ;;
      esac
    fi
    if [ "$1" = api ]; then
      case "$(api_path "$@")" in
        *annotations) echo "should never be called" >&2; exit 1 ;;
        *pulls)       echo '[]'; exit 0 ;;
      esac
    fi
    [ "$1" = pr ] && { echo '[]'; exit 0; }
    exit 0 ;;
  partial_pr_comments)
    # Two associated PRs, one comments call fails: the pr-comments aggregate
    # takes the same three-way rule as annotations.
    if [ "$1" = run ]; then
      case "$(run_kind "$@")" in
        logs) echo "log line"; exit 0 ;;
        jobs) echo '{"jobs":[]}'; exit 0 ;;
        run)  echo '{"headSha":"08eaf8c966"}'; exit 0 ;;
      esac
    fi
    if [ "$1" = api ]; then
      case "$(api_path "$@")" in
        *pulls)              echo '[{"number":11},{"number":22}]'; exit 0 ;;
        *issues/11/comments) echo '[{"body":"coverage 91%"}]'; exit 0 ;;
        *issues/22/comments) echo "HTTP 404: Not Found" >&2; exit 1 ;;
      esac
    fi
    [ "$1" = pr ] && { echo '[]'; exit 0; }
    exit 0 ;;
  malformed_json)
    # Every call "succeeds" but the annotations payload is not JSON, so the
    # jq merge fails. The aggregate must report `failed`, not `ok`.
    if [ "$1" = run ]; then
      case "$(run_kind "$@")" in
        logs) echo "log line"; exit 0 ;;
        jobs) echo '{"jobs":[{"databaseId":111,"conclusion":"failure"}]}'; exit 0 ;;
        run)  echo '{"headSha":"08eaf8c966"}'; exit 0 ;;
      esac
    fi
    if [ "$1" = api ]; then
      case "$(api_path "$@")" in
        *annotations) echo 'this is not json at all {{{'; exit 0 ;;
        *pulls)       echo '[]'; exit 0 ;;
      esac
    fi
    [ "$1" = pr ] && { echo '[]'; exit 0; }
    exit 0 ;;
  realistic_api_404)
    # Real `gh api` prints the HTTP error BODY to stdout and its message to
    # stderr before exiting 1. Job 111 succeeds, job 333 404s that way.
    if [ "$1" = run ]; then
      case "$(run_kind "$@")" in
        logs) echo "log line"; exit 0 ;;
        jobs) echo '{"jobs":[{"databaseId":111,"conclusion":"failure"},
                             {"databaseId":333,"conclusion":"failure"}]}'; exit 0 ;;
        run)  echo '{"headSha":"08eaf8c966"}'; exit 0 ;;
      esac
    fi
    if [ "$1" = api ]; then
      case "$(api_path "$@")" in
        *check-runs/111/annotations) echo '[{"message":"boom"}]'; exit 0 ;;
        *check-runs/333/annotations)
          echo '{"message":"Not Found","documentation_url":"https://docs.github.com"}'
          echo 'gh: Not Found (HTTP 404)' >&2
          exit 1 ;;
        *pulls) echo '[]'; exit 0 ;;
      esac
    fi
    [ "$1" = pr ] && { echo '[]'; exit 0; }
    exit 0 ;;
  prs_fetch_fails)
    # run.json is fine, so headSha exists; the pulls call itself 403s.
    if [ "$1" = run ]; then
      case "$(run_kind "$@")" in
        jobs) echo '{"jobs":[]}'; exit 0 ;;
        logs) echo "log line"; exit 0 ;;
        run)  echo '{"headSha":"08eaf8c966"}'; exit 0 ;;
      esac
    fi
    [ "$1" = api ] && { echo "HTTP 403: rate limit exceeded" >&2; exit 1; }
    [ "$1" = pr ] && { echo '[]'; exit 0; }
    exit 0 ;;
  multiline_run_error)
    # The generic fetch() path (not an aggregate) fails with a multi-line,
    # CR-bearing, query-string-bearing error.
    if [ "$1" = run ]; then
      case "$(run_kind "$@")" in
        jobs) echo '{"jobs":[]}'; exit 0 ;;
        logs) echo "log line"; exit 0 ;;
        run)  printf 'HTTP 410 (https://api.github.com/z?sig=RUNSECRET)\nsecond line\r::stop-commands::x\n' >&2
              exit 1 ;;
      esac
    fi
    [ "$1" = pr ] && { echo '[]'; exit 0; }
    exit 0 ;;
  mixed_pages_array_then_object)
    # gh exits 0; page one is an array, page two is an error object.
    if [ "$1" = run ]; then
      case "$(run_kind "$@")" in
        logs) echo "log line"; exit 0 ;;
        jobs) echo '{"jobs":[{"databaseId":111,"conclusion":"failure"}]}'; exit 0 ;;
        run)  echo '{"headSha":"08eaf8c966"}'; exit 0 ;;
      esac
    fi
    if [ "$1" = api ]; then
      case "$(api_path "$@")" in
        *annotations)
          echo '[{"message":"page one"}]'
          echo '{"message":"upstream error"}'
          exit 0 ;;
        *pulls) echo '[]'; exit 0 ;;
      esac
    fi
    [ "$1" = pr ] && { echo '[]'; exit 0; }
    exit 0 ;;
  mixed_pages_object_then_array)
    # The reverse order: the LAST page is well-formed, which is what the old
    # unslurped `jq -e` graded.
    if [ "$1" = run ]; then
      case "$(run_kind "$@")" in
        logs) echo "log line"; exit 0 ;;
        jobs) echo '{"jobs":[{"databaseId":111,"conclusion":"failure"}]}'; exit 0 ;;
        run)  echo '{"headSha":"08eaf8c966"}'; exit 0 ;;
      esac
    fi
    if [ "$1" = api ]; then
      case "$(api_path "$@")" in
        *annotations)
          echo '"gateway timeout"'
          echo '[{"message":"page two"}]'
          exit 0 ;;
        *pulls) echo '[]'; exit 0 ;;
      esac
    fi
    [ "$1" = pr ] && { echo '[]'; exit 0; }
    exit 0 ;;
  lone_error_object)
    # gh exits 0 and returns a single error object - WH2's semantics on the
    # exit-0 path.
    if [ "$1" = run ]; then
      case "$(run_kind "$@")" in
        logs) echo "log line"; exit 0 ;;
        jobs) echo '{"jobs":[{"databaseId":111,"conclusion":"failure"}]}'; exit 0 ;;
        run)  echo '{"headSha":"08eaf8c966"}'; exit 0 ;;
      esac
    fi
    if [ "$1" = api ]; then
      case "$(api_path "$@")" in
        *annotations) echo '{"message":"Not Found"}'; exit 0 ;;
        *pulls)       echo '[]'; exit 0 ;;
      esac
    fi
    [ "$1" = pr ] && { echo '[]'; exit 0; }
    exit 0 ;;
  truncated_pr_list)
    # The open-PR list comes back at exactly the --limit, so it is truncated.
    if [ "$1" = run ]; then
      case "$(run_kind "$@")" in
        logs) echo "log line"; exit 0 ;;
        jobs) echo '{"jobs":[]}'; exit 0 ;;
        run)  echo '{"headSha":"08eaf8c966"}'; exit 0 ;;
      esac
    fi
    if [ "$1" = api ]; then
      case "$(api_path "$@")" in *pulls) echo '[]'; exit 0 ;; esac
    fi
    if [ "$1" = pr ]; then
      # $PR_LIST_SIZE entries, so the test can sit the list exactly on the
      # workflow's declared limit and one below it.
      printf '['
      i=0
      while [ "$i" -lt "$PR_LIST_SIZE" ]; do
        [ "$i" -gt 0 ] && printf ','
        printf '{"number":%d,"headRefName":"ci-fix/%d"}' "$i" "$i"
        i=$((i + 1))
      done
      printf ']\n'
      exit 0
    fi
    exit 0 ;;
  hanging_logs)
    # The log fetch never returns. `timeout` must kill it and the fetch must
    # grade `failed` with a reason, rather than stalling the whole dispatch.
    if [ "$1" = run ]; then
      case "$(run_kind "$@")" in
        logs) sleep 30; exit 0 ;;
        jobs) echo '{"jobs":[]}'; exit 0 ;;
        run)  echo '{"headSha":"08eaf8c966"}'; exit 0 ;;
      esac
    fi
    if [ "$1" = api ]; then
      case "$(api_path "$@")" in *pulls) echo '[]'; exit 0 ;; esac
    fi
    [ "$1" = pr ] && { echo '[]'; exit 0; }
    exit 0 ;;
  long_error)
    # A multi-megabyte error body must not land whole in the ledger or the log.
    if [ "$1" = run ]; then
      case "$(run_kind "$@")" in
        jobs) echo '{"jobs":[]}'; exit 0 ;;
        logs) echo "log line"; exit 0 ;;
        run)  head -c 200000 /dev/zero | tr '\0' 'x' >&2; exit 1 ;;
      esac
    fi
    [ "$1" = pr ] && { echo '[]'; exit 0; }
    exit 0 ;;
  nonnumeric_job_ids)
    # jobs.json parses, but one failed job's databaseId is not numeric, so the
    # guard refuses to use it and nothing is fetched for that job. An id this
    # step declined is not evidence that GitHub has no annotations.
    if [ "$1" = run ]; then
      case "$(run_kind "$@")" in
        logs) echo "log line"; exit 0 ;;
        jobs) echo '{"jobs":[{"databaseId":"111; rm -rf /","conclusion":"failure"},
                             {"databaseId":222,"conclusion":"failure"}]}'; exit 0 ;;
        run)  echo '{"headSha":"08eaf8c966"}'; exit 0 ;;
      esac
    fi
    if [ "$1" = api ]; then
      case "$(api_path "$@")" in
        *check-runs/222/annotations) echo '[{"message":"boom"}]'; exit 0 ;;
        *pulls) echo '[]'; exit 0 ;;
      esac
    fi
    [ "$1" = pr ] && { echo '[]'; exit 0; }
    exit 0 ;;
  tab_in_error)
    # A TAB in the error text would add a field to the status ledger's TSV, and
    # a NEWLINE would add a row. Both must be neutralised before `record`.
    if [ "$1" = run ]; then
      case "$(run_kind "$@")" in
        jobs) echo '{"jobs":[]}'; exit 0 ;;
        logs) echo "log line"; exit 0 ;;
        run)  printf 'HTTP 500\tinjected\tstatus\tok\nrun\tok\n' >&2; exit 1 ;;
      esac
    fi
    [ "$1" = pr ] && { echo '[]'; exit 0; }
    exit 0 ;;
  paginated_annotations)
    # `gh api --paginate` emits ONE JSON ARRAY PER PAGE, concatenated on
    # stdout. Both pages must survive the per-item parse check and the merge.
    if [ "$1" = run ]; then
      case "$(run_kind "$@")" in
        logs) echo "log line"; exit 0 ;;
        jobs) echo '{"jobs":[{"databaseId":111,"conclusion":"failure"}]}'; exit 0 ;;
        run)  echo '{"headSha":"08eaf8c966"}'; exit 0 ;;
      esac
    fi
    if [ "$1" = api ]; then
      case "$(api_path "$@")" in
        *annotations)
          echo '[{"message":"page one"}]'
          echo '[{"message":"page two"}]'
          exit 0 ;;
        *pulls) echo '[]'; exit 0 ;;
      esac
    fi
    [ "$1" = pr ] && { echo '[]'; exit 0; }
    exit 0 ;;
  unenumerable_jobs)
    # The jobs fetch exits 0 with a truncated body, so the id enumeration
    # fails. The annotations aggregate must not grade the empty result `ok`.
    if [ "$1" = run ]; then
      case "$(run_kind "$@")" in
        logs) echo "log line"; exit 0 ;;
        jobs) echo '{"jobs":[{"databaseId":111,'; exit 0 ;;
        run)  echo '{"headSha":"08eaf8c966"}'; exit 0 ;;
      esac
    fi
    if [ "$1" = api ]; then
      case "$(api_path "$@")" in *pulls) echo '[]'; exit 0 ;; esac
    fi
    [ "$1" = pr ] && { echo '[]'; exit 0; }
    exit 0 ;;
esac
exit 0
""".replace("__RUN_ID__", _RUN_ID)


class GatherRun:
    """Result of executing the gather block once against a stub-gh scenario."""

    def __init__(self, workdir, returncode, stdout, stderr, ledger, files,
                 gh_calls, exported):
        self.workdir = workdir
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        self.ledger = ledger
        self.files = files
        # Every `gh` argv the block issued, in order.
        self.gh_calls = gh_calls
        # What the block wrote to $GITHUB_ENV, as a dict.
        self.exported = exported

    # Workflow commands are only honored on stdout, so warnings are asserted
    # there and not on the merged streams.
    def warnings(self):
        return [ln for ln in self.stdout.splitlines() if ln.startswith("::warning::")]

    def errors(self):
        return [ln for ln in self.stdout.splitlines() if ln.startswith("::error::")]

    def degraded_warning(self):
        for line in self.warnings():
            if "INCOMPLETE" in line:
                return line
        return None

    def status(self, key):
        return (self.ledger.get("data", {}).get(key) or {}).get("status")

    def usable(self, key):
        return (self.ledger.get("data", {}).get(key) or {}).get("usable")

    def complete(self, key):
        return (self.ledger.get("data", {}).get(key) or {}).get("complete")

    def reason(self, key):
        return (self.ledger.get("data", {}).get(key) or {}).get("reason")


def _write_stub(bindir, name, body):
    path = os.path.join(bindir, name)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(body)
    os.chmod(path, 0o755)


@contextlib.contextmanager
def gather(case, run_url=None, break_ledger_build=False, pr_list_size=0,
           gh_timeout=None, stale_files=None, drop_ledger_row=None):
    """Execute the gather block under `bash -e` with the stub gh for `case`.

    A context manager so the temp dir is removed even when a check fails and
    the test function raises.
    """
    workdir = tempfile.mkdtemp(prefix=f"ci-fix-context-{case}-")
    try:
        bindir = os.path.join(workdir, "bin")
        os.makedirs(bindir)
        _write_stub(bindir, "gh", _GH_STUB)
        if drop_ledger_row:
            # A jq wrapper that strips one key's row out of the status TSV on
            # its way into the ledger builder, modelling a code path that
            # forgot to `record`. It rewrites jq's file argument in place and
            # then delegates, so it needs no knowledge of the filter itself.
            _write_stub(bindir, "jq", "#!/bin/bash\n"
                        'if [ "$1" = "-Rn" ]; then\n'
                        '  for arg in "$@"; do :; done\n'
                        '  grep -v "^%s\t" "$arg" > "$arg.keep" || true\n'
                        '  mv "$arg.keep" "$arg"\n'
                        'fi\n'
                        'exec %s "$@"\n' % (drop_ledger_row, shutil.which("jq")))
        if break_ledger_build:
            # Fail ONLY the `-Rn` call that builds the ledger, delegating every
            # other jq invocation to the real binary, so the test isolates the
            # ledger-publication failure from the fetches above it.
            _write_stub(bindir, "jq", "#!/bin/bash\n"
                        'if [ "$1" = "-Rn" ]; then echo "jq: simulated failure" >&2; exit 5; fi\n'
                        'exec %s "$@"\n' % shutil.which("jq"))

        runner_temp = os.path.join(workdir, "runner-temp")
        os.makedirs(runner_temp)
        if stale_files:
            # Model a persistent self-hosted runner where an earlier dispatch
            # left files in the context dir.
            ctx_dir = pathlib.Path(runner_temp) / "ci-fix-context"
            ctx_dir.mkdir(parents=True)
            for name, body in stale_files.items():
                (ctx_dir / name).write_text(body, encoding="utf-8")
        github_env = os.path.join(workdir, "github_env")
        open(github_env, "w", encoding="utf-8").close()

        script = os.path.join(workdir, "gather.sh")
        with open(script, "w", encoding="utf-8") as fh:
            fh.write(_resolve_expressions(_extract_run_block(_GATHER_STEP),
                                          "the gather block"))

        gh_args_log = os.path.join(workdir, "gh-args.log")
        # An allowlist, not os.environ: when this suite runs INSIDE Actions the
        # ambient GITHUB_* / RUNNER_* variables would otherwise leak in, and
        # the executed environment would stop matching the configured one.
        env = {
            "PATH": bindir + os.pathsep + "/usr/bin" + os.pathsep + "/bin",
            "HOME": workdir,
            "RUNNER_TEMP": runner_temp,
            "GITHUB_ENV": github_env,
            "GH_ARGS_LOG": gh_args_log,
            "FAILED_RUN_URL": run_url or (
                "https://github.com/JetBrains/youtrackdb/actions/runs/" + _RUN_ID),
            "STUB_CASE": case,
            "PR_LIST_SIZE": str(pr_list_size),
        }
        if gh_timeout is not None:
            env["GH_TIMEOUT"] = str(gh_timeout)
            env["GH_BULK_TIMEOUT"] = str(gh_timeout)
        proc = subprocess.run([_BASH, "-e", script], env=env,
                              capture_output=True, text=True, timeout=30)

        ctx = pathlib.Path(runner_temp) / "ci-fix-context"
        ledger = {}
        ledger_file = ctx / "fetch-status.json"
        if ledger_file.exists():
            try:
                ledger = json.loads(ledger_file.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                ledger = {"__unparseable__": ledger_file.read_text(encoding="utf-8")}
        files = {p.name: p.read_text(encoding="utf-8")
                 for p in sorted(ctx.glob("*"))} if ctx.is_dir() else {}
        gh_calls = []
        if os.path.exists(gh_args_log):
            with open(gh_args_log, encoding="utf-8") as fh:
                gh_calls = [ln.rstrip("\n") for ln in fh if ln.strip()]
        exported = {}
        with open(github_env, encoding="utf-8") as fh:
            for line in fh:
                if "=" in line:
                    k, v = line.rstrip("\n").split("=", 1)
                    exported[k] = v
        yield GatherRun(workdir, proc.returncode, proc.stdout, proc.stderr,
                        ledger, files, gh_calls, exported)
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


@contextlib.contextmanager
def prereq(present_tools, apt_rc=0):
    """Execute the prerequisite block with only `present_tools` on PATH.

    `sudo` and `apt-get` are stubbed, so the block cannot install anything:
    whatever is absent at entry stays absent, which is what makes the
    verification loop's behavior observable. `apt_rc` is the stub apt's exit
    status -- non-zero models a held dpkg lock or a mirror 503.
    """
    workdir = tempfile.mkdtemp(prefix="ci-fix-prereq-")
    try:
        bindir = os.path.join(workdir, "bin")
        os.makedirs(bindir)
        for tool in present_tools:
            _write_stub(bindir, tool, "#!/bin/bash\necho '%s 1.0'\n" % tool)
        _write_stub(bindir, "sudo", '#!/bin/bash\nexec "$@"\n')
        _write_stub(bindir, "apt-get",
                    "#!/bin/bash\necho 'E: Could not get lock' >&2\nexit %d\n" % apt_rc)
        # Symlink in only the coreutils the block itself calls. PATH is then
        # bindir ALONE, so a tool is absent exactly when this test says it is;
        # putting /usr/bin on PATH would let a real jq or gh installed on the
        # machine satisfy `command -v` and silently pass every failure case.
        for tool in ("head",):
            src = shutil.which(tool)
            if src:
                os.symlink(src, os.path.join(bindir, tool))

        script = os.path.join(workdir, "prereq.sh")
        with open(script, "w", encoding="utf-8") as fh:
            fh.write(_resolve_expressions(_extract_run_block(_PREREQ_STEP),
                                          "the prerequisite block"))

        env = dict(os.environ)
        env["PATH"] = bindir
        proc = subprocess.run([_BASH, "-e", script], env=env,
                              capture_output=True, text=True, timeout=30)
        yield proc
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


@contextlib.contextmanager
def read_result(sentinel=None, raw=None):
    """Execute the "Read fix result" block against a sentinel dict.

    Yields the parsed `$GITHUB_OUTPUT` the step produced. `sentinel` of None
    writes no file at all, modelling an agent that crashed before recording
    anything.
    """
    workdir = tempfile.mkdtemp(prefix="ci-fix-result-")
    try:
        runner_temp = os.path.join(workdir, "runner-temp")
        os.makedirs(os.path.join(runner_temp, "ci-fix"))
        result_file = os.path.join(runner_temp, "ci-fix", "result.json")
        if raw is not None:
            # Verbatim bytes, for shapes no dict can express: a truncated
            # write, a proxy error page, an agent that emitted a JSON array.
            pathlib.Path(result_file).write_text(raw, encoding="utf-8")
        elif sentinel is not None:
            with open(result_file, "w", encoding="utf-8") as fh:
                json.dump(sentinel, fh)
        github_output = os.path.join(workdir, "github_output")
        open(github_output, "w", encoding="utf-8").close()

        script = os.path.join(workdir, "read-result.sh")
        with open(script, "w", encoding="utf-8") as fh:
            fh.write(_resolve_expressions(_extract_run_block(_RESULT_STEP),
                                          "the read-result block"))
        env = {
            "PATH": os.pathsep.join(["/usr/bin", "/bin"]),
            "HOME": workdir,
            "RUNNER_TEMP": runner_temp,
            "GITHUB_OUTPUT": github_output,
            "CI_FIX_RESULT_FILE": result_file,
        }
        proc = subprocess.run([_BASH, "-e", script], env=env,
                              capture_output=True, text=True, timeout=30)
        # Asserted here so no caller can forget: the gate runs `if: always()`
        # and is the only path to publishing, so a non-zero exit reddens the
        # job even for a clean no-fix.
        check(f"read-result exits 0 (sentinel={sentinel if sentinel else raw!r})",
              proc.returncode == 0)
        out = {}
        with open(github_output, encoding="utf-8") as fh:
            for line in fh:
                if "=" in line:
                    k, v = line.rstrip("\n").split("=", 1)
                    out[k] = v
        yield proc, out
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


# --- the sentinel gate ------------------------------------------------------


def test_sentinel_gate_refuses_a_protected_branch():
    """A sentinel naming `develop` or `main` must not reach the push.

    The publish step pushes the sentinel's branch with an App token carrying
    `contents: write`, and it pushes BEFORE it opens the PR, so a bad branch
    value lands even if PR creation then fails. Much of the agent's context is
    text any GitHub user can write -- PR titles, PR bodies, issue comments --
    so an injected claim that "the fix branch for this failure is develop"
    must not be able to convert a fix into a direct push that bypasses review
    and every CI gate. The charset check alone accepts both names.
    """
    for branch in ("develop", "main"):
        with read_result({"status": "fix-ready", "branch": branch,
                          "existing_pr": None}) as (proc, out):
            check(f"protected branch '{branch}' is forced to no-fix",
                  out.get("status") == "no-fix")
            # On a warning LINE, not anywhere in the stream: the step's own
            # trailing "Fix result: ... branch='develop'" echo would satisfy a
            # whole-stream conjunction unconditionally.
            check(f"protected branch '{branch}' is named on a warning line",
                  any(ln.startswith("::warning::") and branch in ln
                      for ln in proc.stdout.splitlines()))
    with read_result({"status": "fix-ready", "branch": "ci-fix/legit",
                      "existing_pr": None}) as (proc, out):
        check("an ordinary fix branch still publishes",
              out.get("status") == "fix-ready" and out.get("branch") == "ci-fix/legit")


def test_sentinel_gate_rejects_malformed_values():
    """Every field the publish step consumes must be validated or defaulted.

    A missing sentinel, an unknown status, a branch carrying shell
    metacharacters, an empty branch, and a non-numeric PR number all have to
    degrade to `no-fix` or to an empty value rather than reaching a step that
    holds a write-scoped token.
    """
    with read_result(None) as (proc, out):
        check("a missing sentinel is treated as no-fix", out.get("status") == "no-fix")
    with read_result({"status": "bogus", "branch": "ci-fix/x",
                      "existing_pr": None}) as (proc, out):
        check("an unknown status is forced to no-fix", out.get("status") == "no-fix")
    with read_result({"status": "fix-ready", "branch": "ci-fix/x;rm -rf /",
                      "existing_pr": None}) as (proc, out):
        check("a branch with shell metacharacters is forced to no-fix",
              out.get("status") == "no-fix")
    with read_result({"status": "fix-ready", "branch": "",
                      "existing_pr": None}) as (proc, out):
        check("an empty branch is forced to no-fix", out.get("status") == "no-fix")
    with read_result({"status": "fix-ready", "branch": "ci-fix/x",
                      "existing_pr": "not-a-number"}) as (proc, out):
        check("a non-numeric PR number is dropped", out.get("existing_pr") == "")
    with read_result({"status": "fix-ready", "branch": "ci-fix/x",
                      "existing_pr": 42}) as (proc, out):
        check("a numeric PR number is preserved", out.get("existing_pr") == "42")
    with read_result({"status": "fix-ready\nstatus=fix-ready", "branch": "ci-fix/x",
                      "existing_pr": None}) as (proc, out):
        check("an embedded newline cannot inject a second status line",
              out.get("status") == "no-fix")


# --- the prerequisite step --------------------------------------------------


def test_prerequisite_step_fails_when_gh_is_absent():
    """A missing prerequisite must fail the job here, not degrade the agent later.

    The original defect: only `jq` was installed, so every `gh` call in the
    gather step exited 127 and the step's `||` fallbacks quietly wrote empty
    placeholders. With apt stubbed to a no-op, `gh` stays missing, and the
    block's verification loop is the only thing that can notice.
    """
    with prereq(["jq"]) as p:
        check("prereq: exits non-zero when gh is missing", p.returncode != 0)
        check("prereq: emits an ::error:: naming gh",
              any(ln.startswith("::error::") and "gh" in ln
                  for ln in p.stdout.splitlines()))
    with prereq(["gh"]) as p:
        check("prereq: exits non-zero when jq is missing", p.returncode != 0)
        check("prereq: emits an ::error:: naming jq",
              any(ln.startswith("::error::") and "jq" in ln
                  for ln in p.stdout.splitlines()))
    with prereq([]) as p:
        check("prereq: names both tools when both are missing",
              p.returncode != 0
              and any("jq" in ln and "gh" in ln
                      for ln in p.stdout.splitlines() if ln.startswith("::error::")))


def test_prerequisite_step_succeeds_when_both_tools_are_present():
    """With both tools on PATH the block must exit 0 and report their versions.

    The complement of the failure test: a verification loop that rejected a
    healthy runner would block every future dispatch.
    """
    with prereq(["jq", "gh"]) as p:
        check("prereq: exits 0 when both tools are present", p.returncode == 0)
        check("prereq: emits no ::error::",
              not any(ln.startswith("::error::") for ln in p.stdout.splitlines()))
        check("prereq: reports both versions",
              "jq:" in p.stdout and "gh:" in p.stdout)


def test_prerequisite_step_runs_before_the_gather_step():
    """Installing gh after the step that calls it would fix nothing."""
    names = _step_names_in_order()
    check("prereq step is present", _PREREQ_STEP in names)
    check("gather step is present", _GATHER_STEP in names)
    check("prereq step runs before the gather step",
          _PREREQ_STEP in names and _GATHER_STEP in names
          and names.index(_PREREQ_STEP) < names.index(_GATHER_STEP))


# --- prompt / shell handshake ----------------------------------------------


def test_prompt_documents_the_status_ledger():
    """The prompt must describe the ledger it tells the agent to branch on.

    Asserted against the prompt heredoc alone, not the whole workflow: the
    gather step's own comments mention the same terms, so a whole-file search
    would keep passing after the prompt lost every mention.
    """
    prompt = _prompt_text()
    check("prompt tells the agent about fetch-status.json",
          "fetch-status.json" in prompt)
    for token in ("`ok`", "`failed`", "`unavailable`", "`partial`"):
        check(f"prompt documents status value {token}", token in prompt)
    for token in ("usable", "complete"):
        check(f"prompt documents the derived flag `{token}`", token in prompt)
    check("prompt no longer promises a complete context",
          "Everything about the failure has been pre-fetched" not in _workflow_text())


def test_prompt_uses_real_ledger_keys():
    """Ledger lookups in the prompt must use the keys the shell actually writes.

    The keys are file names without extension (`logs`, not `logs.txt`). A
    prompt that tells the agent to test `.data["logs.txt"]` gets `null` for a
    perfectly healthy datum, and the prompt's own not-usable rule then
    discards it -- the original bug in mirror image, and exactly the drift this
    check exists to catch.

    Naming a data FILE is fine and necessary ("read `run.json`"); what is
    forbidden is making a file name the subject of a ledger predicate. The
    patterns below are those predicates, so this check does not fire on
    ordinary prose about the files.
    """
    prompt = _prompt_text()

    # A proximity rule, not a phrasing rule. Enumerating English phrasings
    # pins one word order and misses the rest: "`logs.txt` has `usable:
    # false`", "- **`logs.txt`** -- not usable", and `.data | .["logs.txt"]`
    # are the same drift in different clothes. So: no bare data-file name on a
    # line that also talks about the ledger.
    ledger_term = re.compile(
        r"usable|complete|\.data\b|\.degraded\b|missing_from_ledger|status")
    # The two sentences that must legitimately name both: the one stating the
    # key convention, and the one about placeholders being indistinguishable.
    exempt = re.compile(r"without its extension|byte-identical")
    files = [k + (".txt" if k == "logs" else ".json") for k in _DATA_KEYS]
    bad = []
    for lineno, line in enumerate(prompt.split("\n"), 1):
        if exempt.search(line) or not ledger_term.search(line):
            continue
        bad += [f"L{lineno}: {f}" for f in files
                if re.search(rf"(?<![\w/-]){re.escape(f)}\b", line)]
    check(f"no ledger sentence is keyed on a file name (found: {bad})", not bad)

    # The prompt must state the key convention, or a reader has no way to map
    # the file list onto the ledger.
    check("prompt states that ledger keys drop the file extension",
          "without its extension" in prompt)
    for key in _DATA_KEYS:
        check(f"prompt names ledger key `{key}`", f"`{key}`" in prompt)

    with gather("allok") as r:
        check("ledger keys are exactly the seven documented data",
              sorted(r.ledger.get("data", {})) == sorted(_DATA_KEYS))


# --- behavior: the happy path ----------------------------------------------


def test_all_fetches_ok():
    """Every datum reachable: all ok and complete, no warning, real payloads."""
    with gather("allok") as r:
        check("allok: step succeeds", r.returncode == 0)
        check("allok: every datum is ok",
              all(r.status(k) == "ok" for k in _DATA_KEYS))
        check("allok: every datum is usable and complete",
              all(r.usable(k) and r.complete(k) for k in _DATA_KEYS))
        check("allok: no reason is recorded",
              all(r.reason(k) is None for k in _DATA_KEYS))
        check("allok: ledger reports the context complete",
              r.ledger.get("complete") is True and r.ledger.get("degraded") == [])
        check("allok: no key missing from the ledger",
              r.ledger.get("missing_from_ledger") == [])
        check("allok: no degraded warning", r.degraded_warning() is None)
        check("allok: no per-fetch warnings", r.warnings() == [])


# --- behavior: the regression this suite exists for ------------------------


def test_gh_absent_degrades_every_datum_loudly():
    """With no `gh` on PATH, nothing may be reported as a successful empty result.

    This is the exact production failure of run 33870767133: the agent received
    `{}`, `[]` and "No failed-job logs available." for every datum and had no
    way to tell them from real answers.
    """
    with gather("absent") as r:
        check("absent: step still exits 0 so investigation can continue",
              r.returncode == 0)
        check("absent: no datum is usable",
              all(r.usable(k) is False for k in _DATA_KEYS))
        check("absent: no datum is complete",
              all(r.complete(k) is False for k in _DATA_KEYS))
        check("absent: ledger reports the context incomplete",
              r.ledger.get("complete") is False)
        check("absent: every datum is listed as degraded",
              sorted(r.ledger.get("degraded", [])) == sorted(_DATA_KEYS))
        check("absent: directly fetched data are marked failed",
              all(r.status(k) == "failed"
                  for k in ("run", "jobs", "logs", "existing-fix-prs")))
        check("absent: derived data are marked unavailable",
              all(r.status(k) == "unavailable"
                  for k in ("annotations", "associated-prs", "pr-comments")))
        check("absent: the recorded reason names the missing binary",
              "command not found" in (r.reason("run") or ""))
        check("absent: the degraded warning is emitted",
              r.degraded_warning() is not None)
        check("absent: placeholders are still written so readers do not crash",
              r.files.get("run.json", "").strip() == "{}"
              and r.files.get("associated-prs.json", "").strip() == "[]")


# --- behavior: dependent data ---------------------------------------------


def test_datum_derived_from_a_failed_parent_is_unavailable():
    """A derived datum must not inherit `ok` from an empty placeholder parent.

    annotations comes from jobs.json and associated-prs comes from run.json's
    headSha. If the parent fetch failed there is nothing to enumerate, so `ok`
    on an empty file would assert something the step never checked.
    """
    with gather("no_jobs") as r:
        check("no_jobs: jobs marked failed", r.status("jobs") == "failed")
        check("no_jobs: annotations marked unavailable, not ok",
              r.status("annotations") == "unavailable")
        check("no_jobs: annotations reason explains the dependency",
              "jobs" in (r.reason("annotations") or ""))
        check("no_jobs: data not derived from jobs stay ok",
              r.status("run") == "ok" and r.status("associated-prs") == "ok")
        check("no_jobs: degraded warning names both affected data",
              "jobs" in (r.degraded_warning() or "")
              and "annotations" in (r.degraded_warning() or ""))

    with gather("no_run") as r:
        check("no_run: run marked failed", r.status("run") == "failed")
        check("no_run: associated-prs unavailable without a headSha",
              r.status("associated-prs") == "unavailable")
        check("no_run: pr-comments unavailable in turn",
              r.status("pr-comments") == "unavailable")
        check("no_run: jobs and logs are unaffected",
              r.status("jobs") == "ok" and r.status("logs") == "ok")


def test_run_payload_without_head_sha_is_unavailable_not_empty():
    """A run fetch that exits 0 but carries no headSha still blocks the PR lookup.

    `gh` succeeding is not the same as the payload being usable. Without a
    commit there is nothing to ask GitHub about, so reporting `associated-prs`
    as an `ok` empty list would tell the agent "this commit has no PR" on no
    evidence at all.
    """
    with gather("run_without_head_sha") as r:
        check("no headSha: run itself is ok", r.status("run") == "ok")
        check("no headSha: associated-prs unavailable",
              r.status("associated-prs") == "unavailable")
        check("no headSha: the reason names the missing field",
              "headSha" in (r.reason("associated-prs") or ""))
        check("no headSha: pr-comments unavailable in turn",
              r.status("pr-comments") == "unavailable")


def test_partial_aggregate_keeps_what_arrived():
    """One failed item in an aggregate marks it usable but incomplete.

    Two failed jobs, annotations for the second 404. Reporting `failed` would
    throw away the first job's annotations -- usually the most specific
    evidence available -- and reporting `ok` would hide the gap.
    """
    with gather("partial_annotations") as r:
        check("partial: annotations marked partial",
              r.status("annotations") == "partial")
        check("partial: partial is usable but not complete",
              r.usable("annotations") is True and r.complete("annotations") is False)
        check("partial: the successful job's annotations are retained",
              "boom" in r.files.get("annotations.json", ""))
        check("partial: the reason counts the failures",
              "1 of 2" in (r.reason("annotations") or ""))
        check("partial: the failing job is named in a warning",
              any("333" in w for w in r.warnings()))
        check("partial: degraded warning is emitted",
              r.degraded_warning() is not None)


def test_aggregate_status_distinguishes_all_failed_from_some_failed():
    """`partial` must mean "some of it arrived", so all-failed reports `failed`.

    An agent that reads `usable: true` will use the file and note the gap. An
    agent that reads `usable: false` will not use the file at all. Marking an
    aggregate where every call failed as usable therefore points the agent at
    an empty file it has been told to trust -- the same absent-versus-empty
    confusion this whole change exists to remove.
    """
    with gather("all_annotations_fail") as r:
        check("all-failed: annotations marked failed, not partial",
              r.status("annotations") == "failed")
        check("all-failed: annotations not usable", r.usable("annotations") is False)
        check("all-failed: the file is the empty placeholder",
              r.files.get("annotations.json", "").strip() == "[]")
        check("all-failed: both jobs are named in warnings",
              any("111" in w for w in r.warnings())
              and any("333" in w for w in r.warnings()))

    with gather("partial_pr_comments") as r:
        check("partial pr-comments: marked partial",
              r.status("pr-comments") == "partial")
        check("partial pr-comments: the successful PR's comments are retained",
              "coverage 91%" in r.files.get("pr-comments.json", ""))
        check("partial pr-comments: the failing PR is named in a warning",
              any(" 22 " in w for w in r.warnings()))


def test_zero_failed_jobs_is_a_real_empty_answer():
    """A run with no failed jobs reports annotations `ok` over an empty list.

    The annotations loop body never executes, so zero calls failed out of zero
    attempted. That is genuinely "GitHub has no annotations for this run", not
    a fetch that did not happen, and marking it `failed` or `unavailable` would
    send the agent hunting for data that does not exist.
    """
    with gather("no_failed_jobs") as r:
        check("no failed jobs: annotations marked ok", r.status("annotations") == "ok")
        check("no failed jobs: annotations file is an empty list",
              r.files.get("annotations.json", "").strip() == "[]")
        check("no failed jobs: no degraded warning", r.degraded_warning() is None)


def test_unparseable_payload_is_reported_as_failed():
    """A fetch that succeeds but returns non-JSON must not be reported `ok`.

    `gh` exiting 0 is not proof the body is usable: a proxy error page or a
    truncated response still exits 0. The jq merge is what discovers this, and
    its failure has to reach the ledger, otherwise the agent is handed `[]`
    under an `ok` label.
    """
    with gather("malformed_json") as r:
        check("malformed: step exits 0", r.returncode == 0)
        check("malformed: annotations marked failed", r.status("annotations") == "failed")
        check("malformed: annotations not usable", r.usable("annotations") is False)
        check("malformed: annotations file left as a valid empty list",
              r.files.get("annotations.json", "").strip() == "[]")
        check("malformed: the bad body is reported",
              any("not a JSON array" in w for w in r.warnings()))
        check("malformed: the reason says the body was not a JSON array",
              "not a JSON array" in (r.reason("annotations") or ""))
        check("malformed: the ledger is still valid JSON with all keys",
              sorted(r.ledger.get("data", {})) == sorted(_DATA_KEYS))


def test_unenumerable_id_source_is_not_graded_ok():
    """A jq parse failure while enumerating ids must not yield an `ok` empty file.

    `gh run view --json jobs` exits 0 with a truncated body. The enumeration
    then fails, the per-item loop runs zero times, and the naive reading is
    "zero failures, therefore ok" -- publishing an empty annotations.json as a
    checked result. It is the same false-`ok` shape as the original bug, one
    level down.
    """
    with gather("unenumerable_jobs") as r:
        check("unenumerable: annotations marked failed",
              r.status("annotations") == "failed")
        check("unenumerable: annotations not usable",
              r.usable("annotations") is False)
        check("unenumerable: the reason names the enumeration",
              "enumerate" in (r.reason("annotations") or ""))
        check("unenumerable: a warning is emitted",
              any("Enumerating" in w for w in r.warnings()))


# --- behavior: log-injection and secret hygiene ---------------------------


def test_fetch_error_cannot_forge_a_workflow_command():
    """A fetch error must reach the log as one line that cannot be re-parsed.

    Workflow commands are parsed per line, and both LF and a bare CR end a
    line for the runner. Remote error text that contains a newline followed by
    `::error::` would otherwise be honored as a fresh workflow command --
    `::stop-commands::` would silence the INCOMPLETE warning this step exists
    to emit. The stub's 404 carries LF, CR, and a `::error::` payload.
    """
    with gather("partial_annotations") as r:
        offending = [w for w in r.warnings() if "404" in w]
        check("injection: the 404 produced exactly one warning line",
              len(offending) == 1)
        check("injection: both source lines survive on that single line",
              bool(offending) and "Not Found" in offending[0]
              and "second line of the error" in offending[0])
        check("injection: no forged workflow command reaches stdout",
              not any(ln.startswith("::error::") for ln in r.stdout.splitlines()))
        check("injection: the CR is not passed through",
              "\r" not in "".join(r.warnings()))


def test_signed_url_query_string_is_stripped():
    """A query string in an error must not be echoed into the log.

    `gh run view --log-failed` follows a redirect to blob storage, and a
    failure at that hop prints the signed URL. It is not a registered secret,
    so nothing masks it. The stub's 404 carries `?sig=SECRETSIG`.
    """
    with gather("partial_annotations") as r:
        check("scrub: the signature query string is gone",
              "SECRETSIG" not in r.stdout)
        check("scrub: the signature is absent from the ledger too",
              "SECRETSIG" not in json.dumps(r.ledger))
        check("scrub: the URL path is still shown for context",
              any("api.github.com" in w for w in r.warnings()))


# --- behavior: input validation and hygiene -------------------------------


def test_non_numeric_run_id_is_rejected_up_front():
    """A malformed run id must fail the step, not become a 404 six lines later.

    Every production dispatcher builds the URL from `github.run_id`, so a
    non-numeric tail means a hand-made dispatch. Rejecting it here also keeps
    control characters out of the run id that `gh` echoes back inside its
    error text, which is the one path by which caller-supplied text could
    reach a `::warning::`.
    """
    with gather("allok", run_url="https://github.com/x/y/actions/runs/1\r::stop-commands::x") as r:
        check("bad run id: step fails", r.returncode != 0)
        check("bad run id: an ::error:: explains why",
              any(ln.startswith("::error::") and "numeric run id" in ln
                  for ln in r.stdout.splitlines()))
    with gather("allok", run_url="https://github.com/x/y/actions/runs/") as r:
        check("empty run id: step fails", r.returncode != 0)


def test_scratch_files_do_not_leak_into_runner_temp():
    """The step's intermediate files must not be left beside the context dir.

    $RUNNER_TEMP is shared with the result directory the agent writes, and a
    stray `.ndjson` or `.tsv` there is one more thing for a reader to mistake
    for input.
    """
    for case in ("allok", "absent", "partial_annotations", "unenumerable_jobs"):
        with gather(case) as r:
            leftovers = sorted(
                p.name for p in (pathlib.Path(r.workdir) / "runner-temp").glob("*")
                if p.is_file())
            check(f"cleanup ({case}): no scratch files remain (found: {leftovers})",
                  leftovers == [])
            check(f"cleanup ({case}): fetch-status.json is published",
                  "fetch-status.json" in r.files)


# --- what is fetched, not just how it is graded ---------------------------


def test_every_fetch_targets_the_dispatched_run_and_repo():
    """The step must ask GitHub about the run it was dispatched for.

    A step that fetches a different run, or a different repository, produces a
    complete and entirely healthy-looking context about the wrong failure --
    the same end state as the bug this suite exists for, and one the ledger
    cannot detect because every fetch succeeded. Grading the result is not
    enough; the request itself has to be pinned.
    """
    with gather("allok") as r:
        check("every gh call names the configured repo",
              r.gh_calls and all(
                  "--repo JetBrains/youtrackdb" in c
                  or "repos/JetBrains/youtrackdb/" in c
                  for c in r.gh_calls))
        run_views = [c for c in r.gh_calls if c.startswith("run view")]
        check("all three run-scoped fetches were issued", len(run_views) == 3)
        check("every run view targets the dispatched run id",
              run_views and all(c.startswith(f"run view {_RUN_ID} ") for c in run_views))
        check("the run metadata fetch requests headSha",
              any("--json" in c and "headSha" in c for c in run_views))
        check("the jobs fetch requests the jobs field",
              any("--json jobs" in c for c in run_views))
        check("the logs fetch uses --log-failed",
              any("--log-failed" in c for c in run_views))
        check("annotations are read from the check-runs endpoint",
              any("repos/JetBrains/youtrackdb/check-runs/111/annotations" in c
                  for c in r.gh_calls))
        check("PR comments are read from the issues endpoint",
              any("repos/JetBrains/youtrackdb/issues/1295/comments" in c
                  for c in r.gh_calls))
        check("the successful job's annotations were not fetched",
              not any("check-runs/222" in c for c in r.gh_calls))
        check("aggregates are paged",
              all("--paginate" in c for c in r.gh_calls if "annotations" in c))


def test_fetched_payloads_land_verbatim_in_their_files():
    """Each file must hold exactly what GitHub returned, byte for byte.

    The stub payloads are deterministic, so exact equality costs nothing and
    catches what a substring check cannot: a dropped field, an extra element
    spliced into the merge, or the wrong payload written to the right name.
    """
    with gather("allok") as r:
        check("run.json is exactly the fetched payload",
              json.loads(r.files.get("run.json", "null"))
              == {"headSha": "08eaf8c966", "conclusion": "failure", "event": "push"})
        check("annotations.json is exactly the merged payload",
              json.loads(r.files.get("annotations.json", "null"))
              == [{"path": "Foo.java", "message": "boom"}])
        check("pr-comments.json is exactly the merged payload",
              json.loads(r.files.get("pr-comments.json", "null"))
              == [{"body": "coverage 91%"}])
        check("associated-prs.json is exactly the fetched payload",
              json.loads(r.files.get("associated-prs.json", "null"))
              == [{"number": 1295}])
        check("existing-fix-prs.json is exactly the fetched payload",
              json.loads(r.files.get("existing-fix-prs.json", "null"))
              == [{"number": 42, "headRefName": "ci-fix/x"}])
        check("jobs.json is exactly the fetched payload",
              json.loads(r.files.get("jobs.json", "null")) == {"jobs": [
                  {"databaseId": 111, "conclusion": "failure"},
                  {"databaseId": 222, "conclusion": "success"}]})
        check("logs.txt holds the failed-job log",
              "FooTest.bar FAILED" in r.files.get("logs.txt", ""))


def test_placeholders_are_the_documented_ones():
    """The placeholder written on failure must be what the prompt says it is.

    The prompt names `{}`, `[]` and the sentence "No failed-job logs
    available." so the agent can recognize them if it ever looks. A drift here
    would leave the prompt describing files that no longer exist in that shape.
    """
    with gather("absent") as r:
        check("run placeholder is {}", r.files.get("run.json", "").strip() == "{}")
        check("jobs placeholder is an empty job list",
              json.loads(r.files.get("jobs.json", "null")) == {"jobs": []})
        check("logs placeholder is the documented sentence",
              r.files.get("logs.txt", "").strip() == "No failed-job logs available.")
        for name in ("annotations.json", "associated-prs.json",
                     "pr-comments.json", "existing-fix-prs.json"):
            check(f"{name} placeholder is an empty list",
                  r.files.get(name, "").strip() == "[]")
        prompt = _prompt_text()
        check("the prompt names all three placeholder shapes",
              "`{}`" in prompt and "`[]`" in prompt
              and "No failed-job logs available." in prompt)


def test_pr_comments_are_unavailable_when_the_pr_list_could_not_be_fetched():
    """A failed PR-list fetch must not let pr-comments report an `ok` empty list.

    With no PR list there is nothing to enumerate. Counting zero failures over
    zero attempts would grade the aggregate `ok`, so an empty
    pr-comments.json would carry an `ok` label and the agent would read "this
    PR has no gate comments" from a fetch that never ran.
    """
    with gather("prs_fetch_fails") as r:
        check("prs failed: step exits 0", r.returncode == 0)
        check("prs failed: associated-prs marked failed",
              r.status("associated-prs") == "failed")
        check("prs failed: pr-comments unavailable, not ok",
              r.status("pr-comments") == "unavailable")
        check("prs failed: pr-comments not usable", r.usable("pr-comments") is False)
        check("prs failed: no comments call was attempted",
              not any("/comments" in c for c in r.gh_calls))
        check("prs failed: the reason explains the dependency",
              "associated PRs" in (r.reason("pr-comments") or ""))


def test_the_step_exports_the_paths_later_steps_depend_on():
    """The four CI_FIX_* paths are the interface to four later steps.

    "Run Fix Agent", "Read fix result", "Publish fix" and "Prepare Zulip
    summary" all resolve their inputs from these variables, and the agent
    prompt names them literally. A rename here breaks all of them at once, and
    the harness has the file open anyway.
    """
    with gather("allok") as r:
        rt = os.path.join(r.workdir, "runner-temp")
        check("exports CI_FIX_CONTEXT_DIR",
              r.exported.get("CI_FIX_CONTEXT_DIR") == f"{rt}/ci-fix-context")
        check("exports CI_FIX_RESULT_DIR",
              r.exported.get("CI_FIX_RESULT_DIR") == f"{rt}/ci-fix")
        check("exports CI_FIX_RESULT_FILE",
              r.exported.get("CI_FIX_RESULT_FILE") == f"{rt}/ci-fix/result.json")
        check("exports CI_FIX_SUMMARY_FILE",
              r.exported.get("CI_FIX_SUMMARY_FILE") == f"{rt}/ci-fix/summary.md")
        check("creates the result dir the agent writes into",
              os.path.isdir(f"{rt}/ci-fix"))
        check("creates the context dir the agent reads from",
              os.path.isdir(f"{rt}/ci-fix-context"))


def test_gather_step_is_wired_to_a_token():
    """The pre-fetch is the only step that reads GitHub, so it needs the token.

    Static, because the stub does not authenticate: dropping the `env:` block
    would leave every behavioral check green here while turning every fetch
    into an auth failure in production.
    """
    text = _workflow_text()
    step = text.split(f"- name: {_GATHER_STEP}", 1)[1].split("run: |", 1)[0]
    check("gather step declares GH_TOKEN", "GH_TOKEN:" in step)
    check("gather step takes it from secrets.GITHUB_TOKEN",
          "secrets.GITHUB_TOKEN" in step)


def test_no_shell_override_changes_the_flags_under_test():
    """The harness runs `bash -e`, which must stay what the runner runs.

    GitHub's implicit default for `run:` is `bash -e`. Adding `shell: bash`
    switches it to `--noprofile --norc -eo pipefail`, so production would gain
    `pipefail` while this suite kept testing the weaker semantics -- and
    `pipefail` changes the outcome of the `scrub` pipeline and of every
    `cmd | jq` in the block.
    """
    text = _workflow_text()
    check("workflow sets no run-shell default", "defaults:" not in text)
    step = text.split(f"- name: {_GATHER_STEP}", 1)[1].split("run: |", 1)[0]
    check("gather step sets no shell override", "shell:" not in step)
    pre = text.split(f"- name: {_PREREQ_STEP}", 1)[1].split("run: |", 1)[0]
    check("prerequisite step sets no shell override", "shell:" not in pre)


def test_a_broken_ledger_build_is_reported_as_an_error():
    """If the ledger itself cannot be built, that cannot be the quiet path.

    The ledger is the single point of trust for every datum above it. A silent
    `{}` fallback would leave the degraded check with nothing to report, so a
    human would see a clean run while the agent saw an absent context -- the
    original defect, reproduced one level up.
    """
    with gather("allok", break_ledger_build=True) as r:
        check("broken ledger: step still exits 0", r.returncode == 0)
        check("broken ledger: an ::error:: is emitted",
              any("fetch-status.json" in e for e in r.errors()))
        check("broken ledger: the fallback is still valid JSON",
              "__unparseable__" not in r.ledger and r.ledger != {})
        check("broken ledger: the fallback marks the context incomplete",
              r.ledger.get("complete") is False)
        check("broken ledger: every datum is listed as degraded",
              sorted(r.ledger.get("degraded", [])) == sorted(_DATA_KEYS))
        check("broken ledger: the degraded warning still fires",
              r.degraded_warning() is not None)


def test_the_generic_fetch_path_also_flattens_its_errors():
    """Error scrubbing must cover `fetch()`, not only the aggregate loop.

    Four of the seven data go through `fetch()` and three through the
    aggregate helper. They scrub through the same function, but only the
    aggregate path had a multi-line stub, so a regression that dropped
    flattening from `fetch()` alone would have gone unnoticed.
    """
    with gather("multiline_run_error") as r:
        check("generic path: step exits 0", r.returncode == 0)
        check("generic path: run marked failed", r.status("run") == "failed")
        offending = [w for w in r.warnings() if "410" in w]
        check("generic path: exactly one warning line", len(offending) == 1)
        check("generic path: the second source line survives on it",
              bool(offending) and "second line" in offending[0])
        check("generic path: no forged workflow command reaches stdout",
              not any(ln.startswith("::stop-commands::")
                      for ln in r.stdout.splitlines()))
        check("generic path: the signed query string is stripped",
              "RUNSECRET" not in r.stdout and "RUNSECRET" not in json.dumps(r.ledger))


def test_error_text_cannot_inject_ledger_fields_or_rows():
    """Control characters in an error must not restructure the status ledger.

    The ledger is accumulated as tab-separated `key<TAB>status<TAB>reason`
    rows, so a TAB in remote error text would add a field and a NEWLINE would
    add a row -- letting a GitHub error message forge a `run<TAB>ok` entry and
    relabel a failed fetch as a healthy one. `scrub`'s
    `tr -c '[:print:]' ' '` maps both (TAB is not in `[:print:]`), which is why
    it runs before the reason reaches `record`.
    """
    with gather("tab_in_error") as r:
        check("tab injection: step exits 0", r.returncode == 0)
        check("tab injection: run is still marked failed",
              r.status("run") == "failed")
        check("tab injection: the ledger has exactly the seven keys",
              sorted(r.ledger.get("data", {})) == sorted(_DATA_KEYS))
        check("tab injection: the reason holds the flattened text",
              "injected" in (r.reason("run") or ""))
        check("tab injection: no tab survives in the reason",
              "\t" not in (r.reason("run") or ""))
        check("tab injection: run did not get relabelled ok",
              r.usable("run") is False)


def test_a_wrong_type_page_is_never_graded_complete():
    """Every page of a paged response must be an array, not just the last one.

    `jq -e` takes its exit status from its LAST output and emits one output per
    input, so an unslurped check over a `--paginate` body graded only the final
    page. A well-formed page of the wrong type then passed the check and was
    silently discarded by the merge's `map(select(type == "array"))`, publishing
    `complete: true` over data that was dropped -- the false-completeness claim
    the ledger exists to close, one level down from the original bug.

    All three shapes need `gh` to exit 0, which is why they are not covered by
    the error-body scenarios: a proxy error page, a truncated upstream
    response, and a bare error object.
    """
    for case, label in (("mixed_pages_array_then_object", "good page first"),
                        ("mixed_pages_object_then_array", "bad page first"),
                        ("lone_error_object", "single error object")):
        with gather(case) as r:
            check(f"{label}: annotations marked failed", r.status("annotations") == "failed")
            check(f"{label}: annotations not usable", r.usable("annotations") is False)
            check(f"{label}: annotations not complete",
                  r.complete("annotations") is False)
            check(f"{label}: the file is the empty placeholder",
                  r.files.get("annotations.json", "").strip() == "[]")
            check(f"{label}: a warning names the bad body",
                  any("not a JSON array" in w for w in r.warnings()))


def test_a_truncated_open_pr_list_is_not_complete():
    """A PR list returned at exactly the --limit must not be graded complete.

    `gh pr list --limit 50` gives no signal that more exist. Graded
    `complete: true`, the agent would read "no matching fix PR" from a list it
    simply never saw the end of, and open a second fix PR over an existing one.
    """
    block = _extract_run_block(_GATHER_STEP)
    limit = int(re.search(r"PR_LIST_LIMIT=(\d+)", block).group(1))
    # The --limit and the "did we hit it" threshold are two numbers meaning one
    # thing. Pinned to each other: a bump to one alone would grade a healthy
    # full list `partial`, and the agent's own not-complete rule would then
    # discard a datum that was fine.
    check(f"the fetch uses the declared limit ({limit})",
          '--limit "$PR_LIST_LIMIT"' in block)
    check("the truncation guard compares against the same variable",
          '-ge "$PR_LIST_LIMIT" ]' in block)
    with gather("truncated_pr_list", pr_list_size=limit) as r:
        check("truncated PR list: marked partial",
              r.status("existing-fix-prs") == "partial")
        check("truncated PR list: still usable", r.usable("existing-fix-prs") is True)
        check("truncated PR list: not complete",
              r.complete("existing-fix-prs") is False)
        check("truncated PR list: the reason names the limit",
              "truncated" in (r.reason("existing-fix-prs") or ""))
        check(f"truncated PR list: all {limit} PRs are still available",
              len(json.loads(r.files.get("existing-fix-prs.json", "[]"))) == limit)
        check("truncated PR list: a warning names the limit",
              any(f"{limit}-item limit" in w for w in r.warnings()))
    # The boundary complement: one under the limit is a complete answer.
    with gather("truncated_pr_list", pr_list_size=limit - 1) as r:
        check("one under the limit: marked ok",
              r.status("existing-fix-prs") == "ok")
        check("one under the limit: complete",
              r.complete("existing-fix-prs") is True)
        check("one under the limit: no warning", r.warnings() == [])


def test_a_failed_api_call_cannot_leak_its_body_into_the_context():
    """A per-item failure keeps what arrived and publishes none of the error.

    Real `gh api` writes the HTTP error body to stdout and its message to
    stderr, then exits 1. Appending responses straight to the merge input
    pushed `{"message":"Not Found",...}` in beside the array that arrived, and
    `jq -s add` cannot add an object to an array -- so ONE failed item failed
    the merge and discarded every item that DID arrive, turning a `partial`
    into a `failed` over an empty file. This is the only scenario that models
    that stdout-body behaviour; the other aggregate failures write to stderr
    only, where an in-place append would be a harmless no-op.
    """
    with gather("realistic_api_404") as r:
        check("api 404: annotations marked partial",
              r.status("annotations") == "partial")
        check("api 404: partial is usable but not complete",
              r.usable("annotations") is True
              and r.complete("annotations") is False)
        check("api 404: the successful job's annotations survive intact",
              json.loads(r.files.get("annotations.json", "null"))
              == [{"message": "boom"}])
        # The error BODY must not reach the data files. The ledger's `reason`
        # may quote gh's stderr message -- that is its job -- so the
        # distinguishing marker is `documentation_url`, which appears only in
        # the body gh wrote to stdout.
        data_files = {n: b for n, b in r.files.items()
                      if n != "fetch-status.json"}
        check("api 404: the error body reached no data file",
              not any("documentation_url" in b or "Not Found" in b
                      for b in data_files.values()))
        check("api 404: the ledger reason does quote gh's message",
              "Not Found" in (r.reason("annotations") or ""))
        check("api 404: the failing job is named on a warning line",
              any(w.startswith("::warning::") and "333" in w
                  for w in r.warnings()))


def test_an_id_the_step_refused_to_use_is_not_an_empty_answer():
    """A skipped id must count as a failure, not vanish from the arithmetic.

    The non-numeric guard used to `continue` before `tried` was incremented,
    so a skipped id left no trace in the failed/tried counts and the aggregate
    took the zero-failures branch that exists for a run with genuinely no
    failed jobs. A run whose ids were all non-numeric therefore published an
    empty file as `ok` and `complete: true` -- the original bug's shape, one
    level down.
    """
    with gather("nonnumeric_job_ids") as r:
        check("skipped id: annotations marked partial, not ok",
              r.status("annotations") == "partial")
        check("skipped id: usable but not complete",
              r.usable("annotations") is True
              and r.complete("annotations") is False)
        check("skipped id: the good job's annotations still arrived",
              json.loads(r.files.get("annotations.json", "null"))
              == [{"message": "boom"}])
        check("skipped id: the malformed id was never sent to GitHub",
              not any("rm -rf" in c for c in r.gh_calls))
        check("skipped id: the context is reported degraded",
              r.ledger.get("complete") is False
              and "annotations" in r.ledger.get("degraded", []))


def test_a_stalled_fetch_is_killed_and_explained():
    """A hung GitHub read must not consume the dispatch, and must say why.

    Every failure this step handles is a fast error, so a call still running
    after the budget is stalled. Unbounded, one of those eats the whole
    12-hour job and publishes no ledger at all. Bounded, the grading records
    it as failed and the agent still gets a usable partial context.

    `timeout` writes nothing to stderr when it fires, so without a special
    case the reason would be empty and the log line would read
    "Fetch 'logs' failed:" with nothing after the colon -- a failure recorded
    with no explanation, which is the defect the aggregate path already names.
    """
    with gather("hanging_logs", gh_timeout=1) as r:
        check("stalled fetch: step still exits 0", r.returncode == 0)
        check("stalled fetch: logs marked failed", r.status("logs") == "failed")
        check("stalled fetch: logs not usable", r.usable("logs") is False)
        check("stalled fetch: the reason says it timed out",
              "timed out" in (r.reason("logs") or ""))
        check("stalled fetch: the warning is not left dangling",
              any("logs" in w and "timed out" in w for w in r.warnings()))
        check("stalled fetch: the other data are unaffected",
              r.status("jobs") == "ok" and r.status("run") == "ok")


def test_a_huge_error_body_is_truncated():
    """An error body must be capped before it reaches the ledger or the log.

    `gh` can print a whole HTML error page. Uncapped, it would land verbatim
    in `fetch-status.json` -- the file the agent reads first -- and in a
    `::warning::` line, burying every other datum's status.
    """
    with gather("long_error") as r:
        reason = r.reason("run") or ""
        check(f"long error: the reason is capped (got {len(reason)} chars)",
              0 < len(reason) <= 300)
        check("long error: run is still marked failed", r.status("run") == "failed")
        check("long error: the ledger is still parseable",
              sorted(r.ledger.get("data", {})) == sorted(_DATA_KEYS))


def test_an_apt_failure_still_names_the_missing_tool():
    """A failing apt must not abort the step with only apt's own output.

    Under `bash -e` an unguarded `sudo apt-get install` exits the step at apt's
    own status with a dpkg-lock message and no statement of what is missing or
    why it matters. Both apt calls carry `|| true` so control reaches the
    verification loop, which is what fails the job with the tool named. The
    comment on those two `|| true`s calls them load-bearing; this is the test
    that makes that true.
    """
    with prereq(["jq"], apt_rc=100) as p:
        check("apt failure: the step fails with apt's own status suppressed",
              p.returncode == 1)
        check("apt failure: the ::error:: names gh, not apt",
              any(ln.startswith("::error::") and "gh" in ln
                  for ln in p.stdout.splitlines()))
    with prereq(["jq", "gh"], apt_rc=100) as p:
        check("apt failure with both tools already present: still exits 0",
              p.returncode == 0)


def test_a_previous_dispatch_leaves_nothing_readable_behind():
    """$RUNNER_TEMP persists across jobs on the self-hosted pool.

    The step purges the context directory rather than only creating it,
    because a file an earlier version of this step left there -- the scratch
    .ndjson files used to live in the context dir -- is indistinguishable to
    the agent from this run's input. The canonical names are always rewritten,
    so a leftover under any other name is exactly what the purge is for.
    """
    with gather("allok", stale_files={
            "ci-fix-annotations.ndjson": '[{"message":"LAST RUN"}]',
            "annotations.json": '[{"message":"LAST RUN"}]'}) as r:
        check("stale scratch file is gone",
              "ci-fix-annotations.ndjson" not in r.files)
        check("no context file holds the previous run's data",
              not any("LAST RUN" in body for body in r.files.values()))
        check("this run's own data is published",
              json.loads(r.files.get("annotations.json", "null"))
              == [{"path": "Foo.java", "message": "boom"}])


def test_an_unrecorded_datum_is_named_as_a_defect_in_this_step():
    """A key the step forgot to record must be reported, not merely absent.

    A missing entry reads to the agent as a degraded datum and quietly
    discards a healthy one, so the ledger validates itself against the
    expected key list and names the gap. The error says this is a defect in
    the step rather than a GitHub failure, because no GitHub failure can
    produce it -- every fetch path records something.
    """
    with gather("allok", drop_ledger_row="logs") as r:
        check("unrecorded: the key is listed in missing_from_ledger",
              r.ledger.get("missing_from_ledger") == ["logs"])
        check("unrecorded: an ::error:: names it",
              any("logs" in e and "never recorded" in e for e in r.errors()))
        check("unrecorded: the datum counts as degraded",
              "logs" in r.ledger.get("degraded", [])
              and r.ledger.get("complete") is False)
        check("unrecorded: the other six are unaffected",
              all(r.status(k) == "ok" for k in _DATA_KEYS if k != "logs"))


def test_paginated_pages_are_all_merged():
    """Every page of a paged response must reach the merged file.

    `gh api --paginate` writes one JSON array per page, concatenated on stdout,
    so an item's buffered body can hold several top-level arrays. The per-item
    parse check has to accept that shape and the merge has to concatenate all
    of them -- otherwise paging, added to stop truncation being graded
    `complete`, would itself silently drop pages.
    """
    with gather("paginated_annotations") as r:
        check("paginated: annotations marked ok", r.status("annotations") == "ok")
        check("paginated: both pages are present",
              json.loads(r.files.get("annotations.json", "null"))
              == [{"message": "page one"}, {"message": "page two"}])
        check("paginated: no warning is emitted", r.warnings() == [])


def test_every_non_fatal_scenario_exits_zero():
    """A degraded fetch must never abort the step.

    Under `bash -e` a missing `|| true` anywhere in the block kills the step
    part-way through, leaving a half-written context and no summary. Asserting
    the exit status in every scenario catches that whole class in one line --
    it is the bug the `DEGRADED` assignment already had once.
    """
    for case in ("allok", "absent", "no_jobs", "no_run", "run_without_head_sha",
                 "partial_annotations", "all_annotations_fail", "no_failed_jobs",
                 "partial_pr_comments", "malformed_json", "unenumerable_jobs",
                 "realistic_api_404", "prs_fetch_fails", "multiline_run_error",
                 "paginated_annotations", "tab_in_error",
                 "mixed_pages_array_then_object", "mixed_pages_object_then_array",
                 "lone_error_object", "nonnumeric_job_ids", "long_error"):
        with gather(case) as r:
            check(f"{case}: step exits 0", r.returncode == 0)
            check(f"{case}: publishes a ledger with all seven keys",
                  sorted(r.ledger.get("data", {})) == sorted(_DATA_KEYS))


def test_selectable_review_agents_exist():
    """Every agent the mandatory review gate names must exist on disk.

    Step 6 is a blocking gate that tells the agent to launch a specific set of
    sub-agents by name. A name with no file behind it cannot be launched, so
    the dimension is silently skipped and the gate passes having reviewed less
    than it claims. Three names in this table were wrong before this test
    existed: `review-bugs-concurrency`, `review-test-behavior` and
    `review-test-completeness`.
    """
    agents_dir = _WORKFLOW.parents[2] / ".claude" / "agents"
    if not agents_dir.is_dir():
        check("agents directory exists (skipping name check)", True)
        return
    available = {p.stem for p in agents_dir.glob("review-*.md")}
    named = set(re.findall(r"review-[a-z-]+", _prompt_text()))
    missing = sorted(named - available)
    check(f"every named review agent exists (missing: {missing})", not missing)
    check("the check found agents to compare against",
          bool(named) and bool(available))


def test_extracted_shell_passes_shellcheck():
    """The step's shell must be shellcheck-clean.

    Roughly 350 lines of shell live inside YAML, where shellcheck cannot reach
    them, so that whole finding class is otherwise invisible to CI. Run at
    `info` severity, which is strict enough to catch unquoted expansions; the
    one deliberate word split carries an inline `disable=SC2086` with its
    reason. Skips when shellcheck is absent so a local run stays green;
    `ubuntu-latest` ships it, which is where this matters.
    """
    if shutil.which("shellcheck") is None:
        check("shellcheck not installed (skipped)", True)
        return
    for step in (_GATHER_STEP, _PREREQ_STEP, _RESULT_STEP, _PUBLISH_STEP):
        script = _resolve_expressions(_extract_run_block(step), step)
        with tempfile.NamedTemporaryFile("w", suffix=".sh", delete=False,
                                         encoding="utf-8") as fh:
            fh.write("#!/usr/bin/env bash\n" + script)
            path = fh.name
        try:
            proc = subprocess.run(
                ["shellcheck", "-s", "bash", "-S", "info", path],
                capture_output=True, text=True, timeout=60)
            check(f"shellcheck clean: {step}"
                  + ("" if proc.returncode == 0 else "\n" + proc.stdout[:2000]),
                  proc.returncode == 0)
        finally:
            os.unlink(path)


def test_the_tmp_cleanup_step_runs_last_and_covers_every_writer():
    """Nothing may be left in /tmp: this job runs on a persistent runner.

    $RUNNER_TEMP is cleaned per job, but /tmp is host-scoped and shared with
    every job the runner ever executes, so an unremoved file accumulates one
    copy per dispatch forever. Two properties have to hold: the cleanup step is
    last, because a step that writes to /tmp after it would not be covered, and
    it names every producer. The step was originally placed before the two
    Zulip steps, which write their own files.
    """
    names = _step_names_in_order()
    check("a /tmp cleanup step exists", "Clean up /tmp scratch files" in names)
    check("the cleanup step is the last step in the job",
          names and names[-1] == "Clean up /tmp scratch files")

    text = _workflow_text()
    cleanup = text.split("- name: Clean up /tmp scratch files", 1)
    if len(cleanup) != 2:
        return
    block = cleanup[1]
    # Every step output that names a /tmp path must be removed there.
    for producer in ("prompt_file", "json_log", "zulip_message_file"):
        check(f"the cleanup step covers {producer}", producer in block)
    check("the cleanup step covers the Zulip fallback file",
          "zulip-fallback" in block)
    check("the cleanup step cannot fail the job", "|| true" in block)
    check("the cleanup step runs even after a failure",
          "if: always()" in block.split("run:", 1)[0])


def main():
    # Discover and run every module-level test_* function, so adding a test
    # needs no manual registration here. Each is wrapped so an extraction
    # failure in one test still lets the rest report -- otherwise renaming a
    # workflow step aborts the run at the first test and prints no summary.
    tests = sorted(
        ((name, fn) for name, fn in globals().items()
         if name.startswith("test_") and callable(fn)),
        key=lambda kv: kv[0],
    )
    for name, fn in tests:
        print(f"\n{name}")
        try:
            fn()
        except Exception as exc:  # noqa: BLE001 - report, do not abort the run
            check(f"{name} raised {type(exc).__name__}: {exc}", False)
    if _failures:
        print(f"\n{len(_failures)} check(s) failed.")
        return 1
    print(f"\nAll checks passed across {len(tests)} test function(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
