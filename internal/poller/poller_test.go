package poller_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Mic92/gitea-mq/internal/gitea"
	"github.com/Mic92/gitea-mq/internal/merge"
	"github.com/Mic92/gitea-mq/internal/poller"
	"github.com/Mic92/gitea-mq/internal/queue"
	"github.com/Mic92/gitea-mq/internal/store/pg"
	"github.com/Mic92/gitea-mq/internal/testutil"
)

func setupPollerTest(t *testing.T) (*poller.Deps, *gitea.MockClient, *queue.Service, context.Context, int64) {
	t.Helper()

	svc, ctx, repoID := testutil.TestQueueService(t)

	mock := &gitea.MockClient{}
	mock.GetPRFn = func(ctx context.Context, owner, repo string, index int64) (*gitea.PR, error) {
		if mock.ListOpenPRsFn != nil {
			prs, err := mock.ListOpenPRsFn(ctx, owner, repo)
			if err != nil {
				return nil, err
			}
			for i := range prs {
				if prs[i].Index == index {
					return &prs[i], nil
				}
			}
		}
		return nil, fmt.Errorf("PR #%d not found", index)
	}
	deps := &poller.Deps{
		Forge:          gitea.NewForge(mock, "https://gitea.example.com"),
		Queue:          svc,
		RepoID:         repoID,
		Owner:          "org",
		Repo:           "app",
		SuccessTimeout: 5 * time.Minute,
	}

	return deps, mock, svc, ctx, repoID
}

func makePR(index int64, headSHA, baseBranch string) gitea.PR {
	return gitea.PR{
		Index: index,
		State: "open",
		Head:  &gitea.PRRef{Sha: headSHA},
		Base:  &gitea.PRRef{Ref: baseBranch},
	}
}

// mockAutomergePRs makes the forge report the given PRs as open with
// auto-merge scheduled — the standard starting point for most poller tests.
func mockAutomergePRs(mock *gitea.MockClient, prs ...gitea.PR) {
	mock.ListOpenPRsFn = func(_ context.Context, _, _ string) ([]gitea.PR, error) {
		return prs, nil
	}
	mock.GetPRTimelineFn = func(_ context.Context, _, _ string, _ int64) ([]gitea.TimelineComment, error) {
		return automergeTimeline(), nil
	}
}

// Gitea can keep the PR's creation SHA in its list response after a branch
// deletion/rewrite/reopen, while the single-PR endpoint has the current head.
// The queue must reject that candidate before testing or reporting success.
func TestPollOnce_StaleListedHeadRejectsAdmission(t *testing.T) {
	deps, mock, svc, ctx, repoID := setupPollerTest(t)
	listed := makePR(733, "f72a2329", "main")
	current := makePR(733, "c29ba145", "main")
	mockAutomergePRs(mock, listed)
	mock.GetPRFn = func(_ context.Context, _, _ string, _ int64) (*gitea.PR, error) {
		return &current, nil
	}

	result, err := poller.PollOnce(ctx, deps)
	if err != nil {
		t.Fatal(err)
	}
	if entry, _ := svc.GetEntry(ctx, repoID, 733); entry != nil {
		t.Fatalf("stale list head must not be queued: %+v", entry)
	}
	if len(result.Errors) == 0 {
		t.Fatal("expected an explicit head-mismatch error")
	}
	if len(mock.CallsTo("MergeBranches")) != 0 {
		t.Fatal("stale head must not be tested")
	}
	for _, call := range mock.CallsTo("CreateCommitStatus") {
		status := call.Args[3].(gitea.CommitStatus)
		if status.State == "success" || call.Args[2] == listed.Head.Sha {
			t.Fatalf("must not publish a queue status to the stale head: %+v", call)
		}
	}
	if len(mock.CallsTo("CancelAutoMerge")) != 1 || len(mock.CallsTo("CreateComment")) != 1 {
		t.Fatal("head mismatch must cancel merge intent and explain the rejection")
	}
}

func TestPollOnce_RetargetedBeforeAdmissionIsNotAHeadMismatch(t *testing.T) {
	deps, mock, svc, ctx, repoID := setupPollerTest(t)
	listed := makePR(733, "same-head", "main")
	current := makePR(733, "same-head", "release")
	mockAutomergePRs(mock, listed)
	mock.GetPRFn = func(_ context.Context, _, _ string, _ int64) (*gitea.PR, error) {
		return &current, nil
	}

	result, err := poller.PollOnce(ctx, deps)
	if err != nil {
		t.Fatal(err)
	}
	if entry, _ := svc.GetEntry(ctx, repoID, 733); entry != nil {
		t.Fatalf("retargeted PR must not be queued: %+v", entry)
	}
	if len(result.Errors) == 0 || len(mock.CallsTo("CreateComment")) != 0 {
		t.Fatalf("expected retarget error without head-mismatch comment: %v", result.Errors)
	}
}

func TestPollOnce_HeadChangesImmediatelyBeforeTesting(t *testing.T) {
	deps, mock, svc, ctx, repoID := setupPollerTest(t)
	listed := makePR(733, "old-head", "main")
	current := makePR(733, "new-head", "main")
	mockAutomergePRs(mock, listed)
	reads := 0
	mock.GetPRFn = func(_ context.Context, _, _ string, _ int64) (*gitea.PR, error) {
		reads++
		if reads <= 2 {
			return &listed, nil
		}
		return &current, nil
	}

	result, err := poller.PollOnce(ctx, deps)
	if err != nil {
		t.Fatal(err)
	}
	if len(mock.CallsTo("MergeBranches")) != 0 {
		t.Fatal("changed head must not be tested")
	}
	if entry, _ := svc.GetEntry(ctx, repoID, 733); entry == nil || entry.State != pg.EntryStateQueued {
		t.Fatalf("candidate must remain queued for next reconciliation: %+v", entry)
	}
	if len(result.Errors) == 0 {
		t.Fatal("expected head-change error")
	}
}

func TestPollOnce_HeadChangesDuringFastForwardCheck(t *testing.T) {
	deps, mock, svc, ctx, repoID := setupPollerTest(t)
	deps.SkipQueueIfUpToDate = true
	listed := makePR(733, "old-head", "main")
	current := makePR(733, "new-head", "main")
	mockAutomergePRs(mock, listed)
	reads := 0
	mock.GetPRFn = func(_ context.Context, _, _ string, _ int64) (*gitea.PR, error) {
		reads++
		if reads <= 3 {
			return &listed, nil
		}
		return &current, nil
	}
	mock.CompareCommitsFn = func(_ context.Context, _, _, _, _ string) (*gitea.Compare, error) {
		return &gitea.Compare{TotalCommits: 0}, nil
	}

	result, err := poller.PollOnce(ctx, deps)
	if err != nil {
		t.Fatal(err)
	}
	if entry, _ := svc.GetEntry(ctx, repoID, 733); entry == nil || entry.State != pg.EntryStateQueued {
		t.Fatalf("stale head must not enter success state: %+v", entry)
	}
	for _, call := range mock.CallsTo("CreateCommitStatus") {
		if call.Args[3].(gitea.CommitStatus).State == "success" {
			t.Fatal("stale head must not receive success")
		}
	}
	if len(result.Errors) == 0 {
		t.Fatal("expected head-change error")
	}
}

// An existing candidate must be invalidated using a fresh PR read even when
// Gitea's list projection remains stuck at the original SHA.
func TestPollOnce_StaleListedHeadRemovesActiveEntry(t *testing.T) {
	deps, mock, svc, ctx, repoID := setupPollerTest(t)
	if _, err := svc.Enqueue(ctx, repoID, 733, "f72a2329", "main"); err != nil {
		t.Fatal(err)
	}
	if err := svc.UpdateState(ctx, repoID, 733, pg.EntryStateTesting); err != nil {
		t.Fatal(err)
	}
	if err := svc.SetMergeBranch(ctx, repoID, 733, merge.BranchName(733), "candidate"); err != nil {
		t.Fatal(err)
	}
	listed := makePR(733, "f72a2329", "main")
	current := makePR(733, "c29ba145", "main")
	mockAutomergePRs(mock, listed)
	mock.GetPRFn = func(_ context.Context, _, _ string, _ int64) (*gitea.PR, error) {
		return &current, nil
	}
	mock.GetCombinedCommitStatusFn = func(_ context.Context, _, _, _ string) (*gitea.CombinedStatus, error) {
		return &gitea.CombinedStatus{Statuses: []gitea.CommitStatusResult{{Context: "ci/build", Status: "success"}}}, nil
	}

	result, err := poller.PollOnce(ctx, deps)
	if err != nil {
		t.Fatal(err)
	}
	if entry, _ := svc.GetEntry(ctx, repoID, 733); entry != nil {
		t.Fatalf("stale candidate must be dequeued: %+v", entry)
	}
	if len(result.Dequeued) != 1 || result.Dequeued[0] != 733 {
		t.Fatalf("expected PR #733 dequeued, got %v", result.Dequeued)
	}
	if len(mock.CallsTo("CancelAutoMerge")) != 1 || len(mock.CallsTo("CreateComment")) != 1 {
		t.Fatal("head change must cancel merge intent and explain the rejection")
	}
	for _, call := range mock.CallsTo("CreateCommitStatus") {
		if call.Args[3].(gitea.CommitStatus).State == "success" {
			t.Fatal("stale candidate must not publish success")
		}
	}
}

// runPollerTicks runs poller.Run driven by an injected tick channel and
// delivers n ticks, waiting for each to be fully handled via TickDone, so no
// wall-clock sleeps and no ticks racing cancellation.
func runPollerTicks(ctx context.Context, deps *poller.Deps, n int) {
	ticks := make(chan time.Time)
	tickDone := make(chan struct{})
	deps.Ticks = ticks
	deps.TickDone = tickDone

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		poller.Run(runCtx, deps, time.Hour, time.Hour)
		close(done)
	}()
	for range n {
		ticks <- time.Now()
		<-tickDone
	}
	cancel()
	<-done
}

func automergeTimeline() []gitea.TimelineComment {
	return []gitea.TimelineComment{{ID: 1, Type: "pull_scheduled_merge", CreatedAt: time.Now()}}
}

func cancelledTimeline() []gitea.TimelineComment {
	return []gitea.TimelineComment{
		{ID: 1, Type: "pull_scheduled_merge", CreatedAt: time.Now().Add(-time.Minute)},
		{ID: 2, Type: "pull_cancel_scheduled_merge", CreatedAt: time.Now()},
	}
}

// With SkipQueueIfUpToDate, a PR already rebased onto base goes straight to
// success: its own CI is green and the merged tree would be identical.
func TestPollOnce_SkipQueueIfUpToDate(t *testing.T) {
	deps, mock, svc, ctx, repoID := setupPollerTest(t)
	deps.SkipQueueIfUpToDate = true

	mockAutomergePRs(mock, makePR(42, "sha42", "main"))
	mock.CompareCommitsFn = func(_ context.Context, _, _, base, head string) (*gitea.Compare, error) {
		if base != "sha42" || head != "main" {
			t.Errorf("compare called with base=%q head=%q, want sha42...main", base, head)
		}
		return &gitea.Compare{TotalCommits: 0}, nil
	}
	mock.MergeBranchesFn = func(_ context.Context, _, _, _, _, _ string) (*gitea.MergeResult, error) {
		t.Fatal("MergeBranches must not be called when PR is up-to-date")
		return nil, nil
	}

	result, err := poller.PollOnce(ctx, deps)
	if err != nil {
		t.Fatalf("PollOnce: %v", err)
	}
	if len(result.Errors) > 0 {
		t.Fatalf("errors: %v", result.Errors)
	}

	entry, _ := svc.GetEntry(ctx, repoID, 42)
	if entry == nil || entry.State != pg.EntryStateSuccess {
		t.Fatalf("expected entry in success state, got %+v", entry)
	}

	var sawSuccess bool
	for _, c := range mock.CallsTo("CreateCommitStatus") {
		if s := c.Args[3].(gitea.CommitStatus); s.Context == "gitea-mq" && s.State == "success" {
			sawSuccess = true
		}
	}
	if !sawSuccess {
		t.Error("expected gitea-mq success status on PR head")
	}
}

// SkipQueueIfUpToDate must not short-circuit when the PR is behind base:
// the merge branch is still required so CI sees the combined tree.
func TestPollOnce_SkipQueueIfUpToDate_BehindBase(t *testing.T) {
	deps, mock, svc, ctx, repoID := setupPollerTest(t)
	deps.SkipQueueIfUpToDate = true

	mockAutomergePRs(mock, makePR(42, "sha42", "main"))
	mock.CompareCommitsFn = func(_ context.Context, _, _, _, _ string) (*gitea.Compare, error) {
		return &gitea.Compare{TotalCommits: 3}, nil
	}

	if _, err := poller.PollOnce(ctx, deps); err != nil {
		t.Fatalf("PollOnce: %v", err)
	}

	entry, _ := svc.GetEntry(ctx, repoID, 42)
	if entry == nil || entry.State != pg.EntryStateTesting {
		t.Fatalf("expected entry in testing state, got %+v", entry)
	}
	if len(mock.CallsTo("MergeBranches")) != 1 {
		t.Error("expected MergeBranches call for behind-base PR")
	}
}

// Happy path: a fresh auto-merge PR is discovered, queued, and immediately
// promoted to testing with the right commit statuses.
func TestPollOnce_NewAutomergePR_Enqueues(t *testing.T) {
	deps, mock, svc, ctx, repoID := setupPollerTest(t)

	mockAutomergePRs(mock, makePR(42, "sha42", "main"))
	mock.MergeBranchesFn = func(_ context.Context, _, _, _, _, _ string) (*gitea.MergeResult, error) {
		return &gitea.MergeResult{SHA: "mock-merge-sha"}, nil
	}

	result, err := poller.PollOnce(ctx, deps)
	if err != nil {
		t.Fatalf("PollOnce: %v", err)
	}
	if len(result.Enqueued) != 1 || result.Enqueued[0] != 42 {
		t.Fatalf("expected PR #42 enqueued, got %v", result.Enqueued)
	}

	entry, _ := svc.GetEntry(ctx, repoID, 42)
	if entry == nil || entry.PrHeadSha != "sha42" || entry.State != pg.EntryStateTesting {
		t.Fatalf("expected queued+testing entry for #42, got %+v", entry)
	}

	statusCalls := mock.CallsTo("CreateCommitStatus")
	if len(statusCalls) != 2 {
		t.Fatalf("expected 2 CreateCommitStatus calls, got %d", len(statusCalls))
	}
	if s := statusCalls[0].Args[3].(gitea.CommitStatus); s.State != "pending" || s.Context != "gitea-mq" {
		t.Fatalf("expected pending gitea-mq status on enqueue, got %+v", s)
	}
	if s := statusCalls[1].Args[3].(gitea.CommitStatus); s.State != "pending" || s.Description != "Testing merge result" {
		t.Fatalf("expected pending 'Testing merge result' status, got %+v", s)
	}
}

type reconcileCase struct {
	name        string
	openPRs     []gitea.PR
	getPR       *gitea.PR // returned for #42 when not in openPRs
	timeline    []gitea.TimelineComment
	wantAdvance bool
	wantCancel  bool
	wantComment bool
}

func (tc reconcileCase) run(t *testing.T) {
	deps, mock, svc, ctx, repoID := setupPollerTest(t)

	if _, err := svc.Enqueue(ctx, repoID, 42, "sha42", "main"); err != nil {
		t.Fatal(err)
	}

	mock.ListOpenPRsFn = func(_ context.Context, _, _ string) ([]gitea.PR, error) { return tc.openPRs, nil }
	mock.GetPRFn = func(_ context.Context, _, _ string, _ int64) (*gitea.PR, error) {
		if tc.getPR != nil {
			return tc.getPR, nil
		}
		for i := range tc.openPRs {
			if tc.openPRs[i].Index == 42 {
				return &tc.openPRs[i], nil
			}
		}
		return nil, fmt.Errorf("PR #42 not found")
	}
	mock.GetPRTimelineFn = func(_ context.Context, _, _ string, _ int64) ([]gitea.TimelineComment, error) {
		return tc.timeline, nil
	}

	result, err := poller.PollOnce(ctx, deps)
	if err != nil {
		t.Fatalf("PollOnce: %v", err)
	}

	if len(result.Dequeued) != 1 || result.Dequeued[0] != 42 {
		t.Fatalf("expected PR #42 dequeued, got %v", result.Dequeued)
	}
	if entry, _ := svc.GetEntry(ctx, repoID, 42); entry != nil {
		t.Fatalf("PR #42 should be removed from queue, got %+v", entry)
	}
	if got := len(result.Advanced) == 1; got != tc.wantAdvance {
		t.Fatalf("advance: got %v want %v (%v)", got, tc.wantAdvance, result.Advanced)
	}
	if got := len(mock.CallsTo("CancelAutoMerge")); (got == 1) != tc.wantCancel {
		t.Fatalf("CancelAutoMerge calls: got %d want cancel=%v", got, tc.wantCancel)
	}
	if got := len(mock.CallsTo("CreateComment")); (got == 1) != tc.wantComment {
		t.Fatalf("CreateComment calls: got %d want comment=%v", got, tc.wantComment)
	}
}

// All reconcile remove-conditions share the same removePR plumbing; verify each
// predicate fires and carries the right side effects (cancel/comment/advance).
func TestPollOnce_Reconcile(t *testing.T) {
	cases := []reconcileCase{
		{
			name:        "merged",
			openPRs:     nil,
			getPR:       &gitea.PR{Index: 42, HasMerged: true, State: "closed"},
			wantAdvance: true,
		},
		{
			name:    "closed",
			openPRs: nil,
			getPR:   &gitea.PR{Index: 42, HasMerged: false, State: "closed"},
		},
		{
			name:        "retargeted",
			openPRs:     []gitea.PR{makePR(42, "sha42", "release")},
			timeline:    automergeTimeline(),
			wantCancel:  true,
			wantComment: true,
		},
		{
			name:        "pushed",
			openPRs:     []gitea.PR{makePR(42, "newsha", "main")},
			timeline:    automergeTimeline(),
			wantAdvance: true,
			wantCancel:  true,
			wantComment: true,
		},
		{
			name:     "automerge_cancelled",
			openPRs:  []gitea.PR{makePR(42, "sha42", "main")},
			timeline: cancelledTimeline(),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, tc.run)
	}
}

// Removing the head-of-queue entry must also delete its merge branch so the
// next head starts from a clean slate.
func TestPollOnce_RemoveHead_CleansUpMergeBranch(t *testing.T) {
	deps, mock, svc, ctx, repoID := setupPollerTest(t)

	if _, err := svc.Enqueue(ctx, repoID, 42, "sha42", "main"); err != nil {
		t.Fatal(err)
	}
	_ = svc.UpdateState(ctx, repoID, 42, pg.EntryStateTesting)
	_ = svc.SetMergeBranch(ctx, repoID, 42, merge.BranchName(42), "mergesha")
	if _, err := svc.Enqueue(ctx, repoID, 43, "sha43", "main"); err != nil {
		t.Fatal(err)
	}

	mock.ListOpenPRsFn = func(_ context.Context, _, _ string) ([]gitea.PR, error) {
		return []gitea.PR{makePR(42, "sha42", "main"), makePR(43, "sha43", "main")}, nil
	}
	mock.GetPRTimelineFn = func(_ context.Context, _, _ string, index int64) ([]gitea.TimelineComment, error) {
		if index == 42 {
			return cancelledTimeline(), nil
		}
		return automergeTimeline(), nil
	}

	if _, err := poller.PollOnce(ctx, deps); err != nil {
		t.Fatalf("PollOnce: %v", err)
	}

	wantBranch := merge.BranchName(42)
	deleteCalls := mock.CallsTo("DeleteBranch")
	if len(deleteCalls) != 1 || deleteCalls[0].Args[2] != wantBranch {
		t.Fatalf("expected delete of %s, got %v", wantBranch, deleteCalls)
	}
}

// Forges without a commit-status webhook (Forgejo) rely on the poller to
// observe merge-branch CI results. A testing head whose merge branch passes
// must be driven to success without any webhook delivery.
func TestPollOnce_MergeBranchChecksPolled_Success(t *testing.T) {
	deps, mock, svc, ctx, repoID := setupPollerTest(t)
	deps.FallbackChecks = []string{"ci/build"}

	if _, err := svc.Enqueue(ctx, repoID, 42, "sha42", "main"); err != nil {
		t.Fatal(err)
	}
	_ = svc.UpdateState(ctx, repoID, 42, pg.EntryStateTesting)
	_ = svc.SetMergeBranch(ctx, repoID, 42, merge.BranchName(42), "mergesha42")

	mockAutomergePRs(mock, makePR(42, "sha42", "main"))
	mock.GetCombinedCommitStatusFn = func(_ context.Context, _, _, sha string) (*gitea.CombinedStatus, error) {
		if sha == "mergesha42" {
			return &gitea.CombinedStatus{
				State:    "success",
				Statuses: []gitea.CommitStatusResult{{Context: "ci/build", Status: "success"}},
			}, nil
		}
		return &gitea.CombinedStatus{State: "pending"}, nil
	}

	if _, err := poller.PollOnce(ctx, deps); err != nil {
		t.Fatalf("PollOnce: %v", err)
	}

	entry, _ := svc.GetEntry(ctx, repoID, 42)
	if entry == nil || entry.State != pg.EntryStateSuccess {
		t.Fatalf("expected PR #42 in success state, got %+v", entry)
	}

	var sawSuccess bool
	for _, c := range mock.CallsTo("CreateCommitStatus") {
		if s := c.Args[3].(gitea.CommitStatus); s.Context == "gitea-mq" && s.State == "success" {
			sawSuccess = true
		}
	}
	if !sawSuccess {
		t.Fatal("expected gitea-mq=success status on PR head")
	}
}

// A stuck entry (testing that never reports, or success that Gitea never
// merges) must be dequeued with an error status once its timeout elapses.
func TestPollOnce_Timeouts(t *testing.T) {
	cases := []struct {
		name      string
		state     pg.EntryState
		configure func(*poller.Deps)
	}{
		{
			name:      "testing_never_reports",
			state:     pg.EntryStateTesting,
			configure: func(d *poller.Deps) { d.CheckTimeout = 1 * time.Millisecond },
		},
		{
			name:      "success_but_not_merged",
			state:     pg.EntryStateSuccess,
			configure: func(d *poller.Deps) { d.SuccessTimeout = 1 * time.Millisecond },
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			deps, mock, svc, ctx, repoID := setupPollerTest(t)
			tc.configure(deps)

			if _, err := svc.Enqueue(ctx, repoID, 42, "sha42", "main"); err != nil {
				t.Fatal(err)
			}
			_ = svc.UpdateState(ctx, repoID, 42, tc.state)
			// Fake clock past the 1ms timeout instead of sleeping.
			deps.Now = func() time.Time { return time.Now().Add(time.Second) }

			mockAutomergePRs(mock, makePR(42, "sha42", "main"))

			result, err := poller.PollOnce(ctx, deps)
			if err != nil {
				t.Fatalf("PollOnce: %v", err)
			}
			if len(result.Dequeued) != 1 || result.Dequeued[0] != 42 {
				t.Fatalf("expected PR #42 dequeued, got %v", result.Dequeued)
			}
			if len(mock.CallsTo("CancelAutoMerge")) != 1 {
				t.Fatal("expected CancelAutoMerge call")
			}
			statusCalls := mock.CallsTo("CreateCommitStatus")
			if len(statusCalls) != 1 || statusCalls[0].Args[3].(gitea.CommitStatus).State != "error" {
				t.Fatalf("expected single error status, got %v", statusCalls)
			}
		})
	}
}

// A PR in "success" whose own required checks are still running cannot be
// merged by the forge yet, so the success timeout must wait for them — but
// only up to CheckTimeout, so a stuck CI cannot block the queue forever.
func TestPollOnce_SuccessTimeoutWaitsForPendingPRChecks(t *testing.T) {
	cases := []struct {
		name         string
		clockAdvance time.Duration
		wantDequeued bool
	}{
		{name: "pr_ci_still_running", clockAdvance: time.Second, wantDequeued: false},
		{name: "pr_ci_stuck_past_check_timeout", clockAdvance: 2 * time.Hour, wantDequeued: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			deps, mock, svc, ctx, repoID := setupPollerTest(t)
			deps.SuccessTimeout = 1 * time.Millisecond
			deps.CheckTimeout = 1 * time.Hour
			deps.FallbackChecks = []string{"ci/build"}

			if _, err := svc.Enqueue(ctx, repoID, 42, "sha42", "main"); err != nil {
				t.Fatal(err)
			}
			_ = svc.UpdateState(ctx, repoID, 42, pg.EntryStateSuccess)
			deps.Now = func() time.Time { return time.Now().Add(tc.clockAdvance) }

			mockAutomergePRs(mock, makePR(42, "sha42", "main"))
			mock.GetCombinedCommitStatusFn = func(_ context.Context, _, _, _ string) (*gitea.CombinedStatus, error) {
				return &gitea.CombinedStatus{
					State:    "pending",
					Statuses: []gitea.CommitStatusResult{{Context: "ci/build", Status: "pending"}},
				}, nil
			}

			result, err := poller.PollOnce(ctx, deps)
			if err != nil {
				t.Fatalf("PollOnce: %v", err)
			}

			gotDequeued := len(result.Dequeued) == 1 && result.Dequeued[0] == 42
			if gotDequeued != tc.wantDequeued {
				t.Fatalf("dequeued=%v, want %v (result: %v)", gotDequeued, tc.wantDequeued, result.Dequeued)
			}
			if !tc.wantDequeued && len(mock.CallsTo("CancelAutoMerge")) != 0 {
				t.Fatal("automerge must stay enabled while PR CI is pending")
			}
		})
	}
}

// prChecksGreen gating: only enqueue once required checks (or none) are green.
func TestPollOnce_CIGating(t *testing.T) {
	cases := []struct {
		name           string
		fallbackChecks []string
		status         *gitea.CombinedStatus
		wantEnqueued   bool
	}{
		{
			name:           "pending",
			fallbackChecks: []string{"ci/build"},
			status: &gitea.CombinedStatus{
				State: "pending", Statuses: []gitea.CommitStatusResult{{Context: "ci/build", Status: "pending"}},
			},
		},
		{
			name:           "failure",
			fallbackChecks: []string{"ci/build"},
			status: &gitea.CombinedStatus{
				State: "failure", Statuses: []gitea.CommitStatusResult{{Context: "ci/build", Status: "failure"}},
			},
		},
		{
			name:           "success",
			fallbackChecks: []string{"ci/build"},
			status: &gitea.CombinedStatus{
				State: "success", Statuses: []gitea.CommitStatusResult{{Context: "ci/build", Status: "success"}},
			},
			wantEnqueued: true,
		},
		{
			name:         "no_ci_configured",
			status:       &gitea.CombinedStatus{State: "pending", Statuses: nil},
			wantEnqueued: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			deps, mock, svc, ctx, repoID := setupPollerTest(t)
			deps.FallbackChecks = tc.fallbackChecks

			mockAutomergePRs(mock, makePR(42, "sha42", "main"))
			mock.GetCombinedCommitStatusFn = func(_ context.Context, _, _, _ string) (*gitea.CombinedStatus, error) {
				return tc.status, nil
			}
			mock.MergeBranchesFn = func(_ context.Context, _, _, _, _, _ string) (*gitea.MergeResult, error) {
				return &gitea.MergeResult{SHA: "mock-merge-sha"}, nil
			}

			result, err := poller.PollOnce(ctx, deps)
			if err != nil {
				t.Fatalf("PollOnce: %v", err)
			}

			if got := len(result.Enqueued) == 1; got != tc.wantEnqueued {
				t.Fatalf("enqueued: got %v want %v (%v)", got, tc.wantEnqueued, result.Enqueued)
			}
			entry, _ := svc.GetEntry(ctx, repoID, 42)
			if (entry != nil) != tc.wantEnqueued {
				t.Fatalf("queue entry presence: got %v want %v", entry != nil, tc.wantEnqueued)
			}
			if !tc.wantEnqueued && len(mock.CallsTo("CreateCommitStatus")) != 0 {
				t.Fatal("should not post status for unenqueued PR")
			}
		})
	}
}

func TestPollOnce_MergeBranchError_NotifiesUser(t *testing.T) {
	deps, mock, svc, ctx, repoID := setupPollerTest(t)

	if _, err := svc.Enqueue(ctx, repoID, 42, "sha42", "main"); err != nil {
		t.Fatal(err)
	}

	mockAutomergePRs(mock, makePR(42, "sha42", "main"))
	mock.MergeBranchesFn = func(_ context.Context, _, _, _, _, _ string) (*gitea.MergeResult, error) {
		return nil, fmt.Errorf("merge: git merge: exit status 128\nfatal: refusing to merge unrelated histories")
	}

	result, err := poller.PollOnce(ctx, deps)
	if err != nil {
		t.Fatalf("PollOnce: %v", err)
	}

	if entry, _ := svc.GetEntry(ctx, repoID, 42); entry != nil {
		t.Fatalf("PR #42 should be removed after merge error, got state=%s", entry.State)
	}
	if len(result.Dequeued) != 1 || result.Dequeued[0] != 42 {
		t.Fatalf("expected PR #42 in result.Dequeued, got %v", result.Dequeued)
	}
	if len(result.Errors) == 0 {
		t.Fatal("expected error in result.Errors for logging")
	}

	var foundFailure bool
	for _, call := range mock.CallsTo("CreateCommitStatus") {
		if s := call.Args[3].(gitea.CommitStatus); s.State == "error" && s.Context == "gitea-mq" {
			foundFailure = true
		}
	}
	if !foundFailure {
		t.Fatal("expected error status on PR head after merge failure")
	}
	if len(mock.CallsTo("CreateComment")) == 0 {
		t.Fatal("expected a comment explaining the merge failure")
	}
	if len(mock.CallsTo("CancelAutoMerge")) == 0 {
		t.Fatal("expected CancelAutoMerge to be called")
	}
}

func TestPollOnce_GiteaUnavailable_Pauses(t *testing.T) {
	deps, mock, _, ctx, _ := setupPollerTest(t)

	mock.ListOpenPRsFn = func(_ context.Context, _, _ string) ([]gitea.PR, error) {
		return nil, fmt.Errorf("connection refused")
	}

	result, err := poller.PollOnce(ctx, deps)
	if err != nil {
		t.Fatalf("PollOnce should not return error, got: %v", err)
	}
	if !result.Paused {
		t.Fatal("expected Paused=true when Gitea is unreachable")
	}
	if len(result.Errors) != 1 {
		t.Fatalf("expected 1 error, got %d", len(result.Errors))
	}
}

// An idle repo must not hit the forge on every tick: only the startup
// reconcile runs until the idle interval elapses.
func TestRun_IdleRepoSkipsPeriodicPoll(t *testing.T) {
	deps, mock, _, ctx, _ := setupPollerTest(t)
	deps.IdleGating = true

	var listed int
	mock.ListOpenPRsFn = func(_ context.Context, _, _ string) ([]gitea.PR, error) {
		listed++
		return nil, nil
	}

	// Long idle interval: idle repo polls only at startup despite ticks.
	runPollerTicks(ctx, deps, 5)

	if listed != 1 {
		t.Fatalf("idle repo polled forge %d times, want 1 (startup only)", listed)
	}
}

// Without idle-gating (Gitea/Forgejo, which have no status webhook) an idle
// repo must keep polling every tick: the poll is the only way to notice a PR
// went green. Regression guard for the forgejo integration test.
func TestRun_NoIdleGatingPollsEveryTick(t *testing.T) {
	deps, mock, _, ctx, _ := setupPollerTest(t)
	// deps.IdleGating stays false (the gitea default).

	var listed int
	mock.ListOpenPRsFn = func(_ context.Context, _, _ string) ([]gitea.PR, error) {
		listed++
		return nil, nil
	}

	runPollerTicks(ctx, deps, 4)

	// Startup poll plus several periodic polls despite the repo being idle.
	if listed < 3 {
		t.Fatalf("idle repo without gating polled %d times, want many", listed)
	}
}

// A trigger (webhook) must reconcile an idle repo even before the idle
// interval elapses; this is how an auto-merge PR gone green gets enqueued.
func TestRun_TriggerPollsIdleRepo(t *testing.T) {
	deps, mock, _, ctx, _ := setupPollerTest(t)
	deps.IdleGating = true

	trigger := make(chan struct{})
	deps.Trigger = trigger

	// TickDone tells us when the triggered poll finished; the real ticker
	// (1h interval) never fires within the test.
	tickDone := make(chan struct{})
	deps.TickDone = tickDone

	var listed int
	mock.ListOpenPRsFn = func(_ context.Context, _, _ string) ([]gitea.PR, error) {
		listed++
		return nil, nil
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	done := make(chan struct{})
	go func() {
		poller.Run(runCtx, deps, time.Hour, time.Hour)
		close(done)
	}()

	// Fire a webhook-style trigger and wait until it is fully handled so the
	// poll count is deterministic.
	trigger <- struct{}{}
	<-tickDone
	cancel()
	<-done

	// Startup poll + triggered poll.
	if listed != 2 {
		t.Fatalf("triggered idle repo polled forge %d times, want 2", listed)
	}
}
