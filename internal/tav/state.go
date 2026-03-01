package tav

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	git "github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/config"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/object"
)

const (
	metaDir   = ".tav"
	stateFile = "state.json"
	opLogFile = "oplog.jsonl"
)

const (
	ansiReset  = "\033[0m"
	ansiBold   = "\033[1m"
	ansiDim    = "\033[2m"
	ansiRed    = "\033[31m"
	ansiGreen  = "\033[32m"
	ansiYellow = "\033[33m"
	ansiBlue   = "\033[34m"
	ansiCyan   = "\033[36m"
)

type State struct {
	Version       int                `json:"version"`
	RepoRoot      string             `json:"repo_root"`
	GitHeadCommit string             `json:"git_head_commit"`
	CurrentChange string             `json:"current_change"`
	Changes       map[string]*Change `json:"changes"`
	CommitIndex   map[string]string  `json:"commit_index"`
}

type Change struct {
	ID          string   `json:"id"`
	GitCommit   string   `json:"git_commit,omitempty"`
	Parents     []string `json:"parents,omitempty"`
	Description string   `json:"description,omitempty"`
	CreatedAt   string   `json:"created_at"`
	AuthorName  string   `json:"author_name,omitempty"`
	AuthorEmail string   `json:"author_email,omitempty"`
}

type Operation struct {
	ID            string `json:"id"`
	Timestamp     string `json:"timestamp"`
	Op            string `json:"op"`
	Details       string `json:"details,omitempty"`
	CurrentBefore string `json:"current_before,omitempty"`
	CurrentAfter  string `json:"current_after,omitempty"`
	GitHeadBefore string `json:"git_head_before,omitempty"`
	GitHeadAfter  string `json:"git_head_after,omitempty"`
}

type RebaseOptions struct {
	Source       string
	Branch       string
	Revisions    []string
	Onto         string
	InsertAfter  string
	InsertBefore string
}

type NewOptions struct {
	Parents      []string
	Message      string
	NoEdit       bool
	InsertAfter  []string
	InsertBefore []string
}

type PushOptions struct {
	Remote    string
	Revisions []string
	Create    []string
}

type SquashOptions struct {
	Revision              string
	From                  []string
	Into                  string
	Onto                  []string
	InsertAfter           []string
	InsertBefore          []string
	Message               string
	UseDestinationMessage bool
	KeepEmptied           bool
}

type AdvanceOptions struct {
	Remote   string
	Bookmark string
	Revset   string
	Force    bool
}

func Init(path string) error {
	repo, repoRoot, err := openRepo(path)
	if err != nil {
		return err
	}

	headCommit, err := resolveHeadCommit(repo)
	if err != nil {
		return err
	}

	statePath := filepath.Join(repoRoot, metaDir, stateFile)
	if _, err := os.Stat(statePath); err == nil {
		return fmt.Errorf("tav state already exists at %s", statePath)
	}

	if err := os.MkdirAll(filepath.Dir(statePath), 0o755); err != nil {
		return fmt.Errorf("create %s: %w", metaDir, err)
	}

	workingID := randomChangeID()

	name, email := authorFromEnv()
	if headCommit != nil {
		name, email = authorFromCommitOrEnv(headCommit)
	}

	state := &State{
		Version:       1,
		RepoRoot:      repoRoot,
		CurrentChange: workingID,
		Changes:       map[string]*Change{},
		CommitIndex:   map[string]string{},
	}

	working := &Change{
		ID:          workingID,
		Description: "",
		CreatedAt:   time.Now().UTC().Format(time.RFC3339),
		AuthorName:  name,
		AuthorEmail: email,
	}
	if headCommit != nil {
		baseID := randomChangeID()
		for workingID == baseID {
			baseID = randomChangeID()
		}

		state.GitHeadCommit = headCommit.Hash.String()
		state.Changes[baseID] = &Change{
			ID:          baseID,
			GitCommit:   headCommit.Hash.String(),
			Parents:     commitParentIDs(headCommit, state),
			Description: strings.TrimSpace(headCommit.Message),
			CreatedAt:   headCommit.Author.When.UTC().Format(time.RFC3339),
			AuthorName:  name,
			AuthorEmail: email,
		}
		state.CommitIndex[headCommit.Hash.String()] = baseID
		working.Parents = []string{baseID}

		if err := detachHEAD(repo, headCommit.Hash); err != nil {
			return err
		}
		fmt.Printf("%s Wrapped git HEAD %s as change %s\n", paint(ansiGreen, "✓"), paint(ansiYellow, short(headCommit.Hash.String())), paint(ansiCyan, baseID))
		fmt.Printf("%s Created working change %s (detached HEAD)\n", paint(ansiGreen, "✓"), paint(ansiCyan, workingID))
	} else {
		fmt.Printf("%s No git commits found; initialized with root working change %s\n", paint(ansiYellow, "!"), paint(ansiCyan, workingID))
	}
	state.Changes[workingID] = working

	if err := saveState(statePath, state); err != nil {
		return err
	}
	if err := ensureGitignoreHasTav(repoRoot); err != nil {
		return err
	}
	if err := appendOperation(repoRoot, Operation{
		ID:            randomOperationID(),
		Timestamp:     time.Now().UTC().Format(time.RFC3339),
		Op:            "init",
		CurrentBefore: "",
		CurrentAfter:  state.CurrentChange,
		GitHeadBefore: "",
		GitHeadAfter:  state.GitHeadCommit,
	}); err != nil {
		return err
	}

	fmt.Printf("%s Initialized tav in %s\n", paint(ansiGreen, "✓"), paint(ansiBlue, repoRoot))
	return nil
}

func resolveHeadCommit(repo *git.Repository) (*object.Commit, error) {
	headRef, err := repo.Head()
	if err != nil {
		if errors.Is(err, plumbing.ErrReferenceNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("resolve head: %w", err)
	}

	headCommit, err := repo.CommitObject(headRef.Hash())
	if err != nil {
		if errors.Is(err, plumbing.ErrObjectNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("read HEAD commit: %w", err)
	}
	return headCommit, nil
}

func Log(path string) error {
	return LogWithRev(path, "")
}

func Status(path string) error {
	repo, repoRoot, err := openRepo(path)
	if err != nil {
		return err
	}
	state, err := loadState(filepath.Join(repoRoot, metaDir, stateFile))
	if err != nil {
		return err
	}

	current := state.Changes[state.CurrentChange]
	if current == nil {
		return fmt.Errorf("state corruption: missing current change %s", state.CurrentChange)
	}
	wt, err := repo.Worktree()
	if err != nil {
		return fmt.Errorf("get worktree: %w", err)
	}
	status, err := wt.Status()
	if err != nil {
		return fmt.Errorf("read worktree status: %w", err)
	}

	fmt.Printf("%s change: %s\n", paint(ansiBold, "Working copy"), paint(ansiCyan, current.ID))
	desc := strings.TrimSpace(current.Description)
	if desc == "" {
		fmt.Printf("%s %s\n", paint(ansiDim, "description:"), paint(ansiDim, "(no description set)"))
	} else {
		fmt.Printf("%s %s\n", paint(ansiDim, "description:"), firstLine(desc))
	}
	if current.GitCommit != "" {
		fmt.Printf("%s %s\n", paint(ansiDim, "base:"), paint(ansiYellow, short(current.GitCommit)))
	} else {
		fmt.Printf("%s %s\n", paint(ansiDim, "base:"), paint(ansiDim, "(uncommitted)"))
	}

	added, modified, deleted, untracked := summarizeWorktreeStatus(status)
	if added+modified+deleted+untracked == 0 {
		fmt.Printf("%s clean\n", paint(ansiGreen, "✓"))
		return nil
	}
	fmt.Printf("%s changes:\n", paint(ansiBold, "Working tree"))
	if added > 0 {
		fmt.Printf("  %s %d added\n", paint(ansiGreen, "A"), added)
	}
	if modified > 0 {
		fmt.Printf("  %s %d modified\n", paint(ansiYellow, "M"), modified)
	}
	if deleted > 0 {
		fmt.Printf("  %s %d deleted\n", paint(ansiRed, "D"), deleted)
	}
	if untracked > 0 {
		fmt.Printf("  %s %d untracked\n", paint(ansiBlue, "?"), untracked)
	}
	return nil
}

func LogWithRev(path, revspec string) error {
	repo, repoRoot, err := openRepo(path)
	if err != nil {
		return err
	}

	statePath := filepath.Join(repoRoot, metaDir, stateFile)
	state, err := loadState(statePath)
	if err != nil {
		return err
	}
	changed, err := reconcileStateWithGit(repo, state)
	if err != nil {
		return err
	}
	if changed {
		if err := saveState(statePath, state); err != nil {
			return err
		}
	}

	branchByCommit, err := branchesByCommit(repo)
	if err != nil {
		return err
	}

	ids, err := selectLogIDs(state, strings.TrimSpace(revspec))
	if err != nil {
		return err
	}
	for i, id := range ids {
		chg := state.Changes[id]
		marker := "◆"
		if i == 0 {
			marker = "◉"
		}

		timeLabel := relativeTime(chg.CreatedAt)
		rev := syntheticRevision(chg)
		if chg.GitCommit != "" {
			rev = short(chg.GitCommit)
		}

		fmt.Printf("%s  %s <%s> [%s] %s",
			paint(ansiYellow, marker),
			paint(ansiCyan, chg.ID),
			paint(ansiGreen, fallback(chg.AuthorEmail, fallback(chg.AuthorName, "unknown"))),
			paint(ansiDim, timeLabel),
			paint(ansiBlue, rev),
		)

		labels := labelsForChange(chg, state, branchByCommit)
		if len(labels) > 0 {
			fmt.Printf(" %s", paint(ansiBold, strings.Join(labels, " ")))
		}
		fmt.Println()

		desc := strings.TrimSpace(chg.Description)
		if desc == "" {
			fmt.Println("│  (empty) (no description set)")
		} else {
			fmt.Printf("│  %s\n", firstLine(desc))
		}

		if len(chg.Parents) == 0 {
			break
		}
		fmt.Println("│")
	}
	fmt.Println(paint(ansiDim, "~"))
	return nil
}

func reconcileStateWithGit(repo *git.Repository, state *State) (bool, error) {
	headCommit, err := resolveHeadCommit(repo)
	if err != nil {
		return false, err
	}
	if headCommit == nil {
		return false, nil
	}

	changed := false
	headHash := headCommit.Hash.String()
	if state.CommitIndex == nil {
		state.CommitIndex = map[string]string{}
	}
	if state.Changes == nil {
		state.Changes = map[string]*Change{}
	}

	if _, ok := state.CommitIndex[headHash]; !ok {
		if err := importFirstParentChain(state, repo, headCommit); err != nil {
			return false, err
		}
		changed = true
	}

	if state.GitHeadCommit != headHash {
		state.GitHeadCommit = headHash
		changed = true
	}

	headChangeID, ok := state.CommitIndex[headHash]
	if !ok {
		return changed, fmt.Errorf("state sync failed: missing change for git HEAD %s", short(headHash))
	}

	if ensureWorkingChangeOnHead(state, headChangeID) {
		changed = true
	}
	return changed, nil
}

func importFirstParentChain(state *State, repo *git.Repository, head *object.Commit) error {
	toImport := make([]*object.Commit, 0, 16)
	cur := head
	for cur != nil {
		if _, ok := state.CommitIndex[cur.Hash.String()]; ok {
			break
		}
		toImport = append(toImport, cur)
		if len(cur.ParentHashes) == 0 {
			break
		}
		next, err := repo.CommitObject(cur.ParentHashes[0])
		if err != nil {
			return fmt.Errorf("load parent commit %s: %w", short(cur.ParentHashes[0].String()), err)
		}
		cur = next
	}

	for i := len(toImport) - 1; i >= 0; i-- {
		co := toImport[i]
		parents := make([]string, 0, len(co.ParentHashes))
		for _, ph := range co.ParentHashes {
			phs := ph.String()
			pid, ok := state.CommitIndex[phs]
			if !ok {
				pid = randomChangeID()
				for state.Changes[pid] != nil {
					pid = randomChangeID()
				}
				state.Changes[pid] = &Change{
					ID:        pid,
					GitCommit: phs,
					CreatedAt: co.Author.When.UTC().Format(time.RFC3339),
				}
				state.CommitIndex[phs] = pid
			}
			parents = append(parents, pid)
		}

		id := randomChangeID()
		for state.Changes[id] != nil {
			id = randomChangeID()
		}
		state.Changes[id] = &Change{
			ID:          id,
			GitCommit:   co.Hash.String(),
			Parents:     parents,
			Description: strings.TrimSpace(co.Message),
			CreatedAt:   co.Author.When.UTC().Format(time.RFC3339),
			AuthorName:  co.Author.Name,
			AuthorEmail: co.Author.Email,
		}
		state.CommitIndex[co.Hash.String()] = id
	}
	return nil
}

func ensureWorkingChangeOnHead(state *State, headChangeID string) bool {
	current := state.Changes[state.CurrentChange]
	if current != nil && current.GitCommit == "" && len(current.Parents) > 0 && current.Parents[0] == headChangeID {
		return false
	}
	for id, ch := range state.Changes {
		if ch == nil || ch.GitCommit != "" {
			continue
		}
		if len(ch.Parents) > 0 && ch.Parents[0] == headChangeID {
			if state.CurrentChange != id {
				state.CurrentChange = id
				return true
			}
			return false
		}
	}

	template := state.Changes[state.CurrentChange]
	name, email := "", ""
	if template != nil {
		name, email = template.AuthorName, template.AuthorEmail
	}
	nextID := randomChangeID()
	for state.Changes[nextID] != nil {
		nextID = randomChangeID()
	}
	state.Changes[nextID] = &Change{
		ID:          nextID,
		Parents:     []string{headChangeID},
		Description: "",
		CreatedAt:   time.Now().UTC().Format(time.RFC3339),
		AuthorName:  name,
		AuthorEmail: email,
	}
	state.CurrentChange = nextID
	return true
}

func ensureWorkingChangeOnParent(state *State, parentID, authorName, authorEmail string) string {
	for id, ch := range state.Changes {
		if ch == nil || ch.GitCommit != "" {
			continue
		}
		if len(ch.Parents) > 0 && ch.Parents[0] == parentID {
			return id
		}
	}
	nextID := randomChangeID()
	for state.Changes[nextID] != nil {
		nextID = randomChangeID()
	}
	state.Changes[nextID] = &Change{
		ID:          nextID,
		Parents:     []string{parentID},
		Description: "",
		CreatedAt:   time.Now().UTC().Format(time.RFC3339),
		AuthorName:  authorName,
		AuthorEmail: authorEmail,
	}
	return nextID
}

func OpLog(path string) error {
	_, repoRoot, err := openRepo(path)
	if err != nil {
		return err
	}
	ops, err := loadOperations(repoRoot)
	if err != nil {
		return err
	}
	if len(ops) == 0 {
		fmt.Println(paint(ansiDim, "(no operations)"))
		return nil
	}

	for i := len(ops) - 1; i >= 0; i-- {
		op := ops[i]
		when := op.Timestamp
		if t, err := time.Parse(time.RFC3339, op.Timestamp); err == nil {
			when = t.Local().Format("2006-01-02 15:04:05 MST")
		}
		fmt.Printf("%s %s %s %s\n", paint(ansiYellow, "@"), paint(ansiCyan, op.ID), paint(ansiDim, when), paint(ansiBold, op.Op))
		if strings.TrimSpace(op.Details) != "" {
			fmt.Printf("  %s\n", paint(ansiBlue, op.Details))
		}
		if op.CurrentBefore != op.CurrentAfter {
			fmt.Printf("  %s: %s -> %s\n", paint(ansiBold, "current"), paint(ansiCyan, displayID(op.CurrentBefore)), paint(ansiCyan, displayID(op.CurrentAfter)))
		}
		if op.GitHeadBefore != op.GitHeadAfter {
			fmt.Printf("  %s: %s -> %s\n", paint(ansiBold, "git_head"), paint(ansiYellow, displayHash(op.GitHeadBefore)), paint(ansiYellow, displayHash(op.GitHeadAfter)))
		}
	}
	return nil
}

func New(path string) error {
	return NewWithOptions(path, NewOptions{})
}

func NewWithOptions(path string, opts NewOptions) error {
	_, repoRoot, err := openRepo(path)
	if err != nil {
		return err
	}
	statePath := filepath.Join(repoRoot, metaDir, stateFile)
	state, err := loadState(statePath)
	if err != nil {
		return err
	}

	current := state.Changes[state.CurrentChange]
	if current == nil {
		return fmt.Errorf("state corruption: missing current change %s", state.CurrentChange)
	}
	beforeCurrent := state.CurrentChange
	beforeGitHead := state.GitHeadCommit

	mode, err := validateNewModes(opts)
	if err != nil {
		return err
	}
	selected, err := resolveRevList(state, selectedNewRefs(opts, mode))
	if err != nil {
		return err
	}
	if mode == "parents" && len(selected) == 0 {
		selected = []string{state.CurrentChange}
	}
	if (mode == "insert-after" || mode == "insert-before") && len(selected) == 0 {
		return errors.New("insert modes require at least one target revision")
	}

	nextID := randomChangeID()
	for state.Changes[nextID] != nil {
		nextID = randomChangeID()
	}

	parents := []string{current.ID}
	switch mode {
	case "parents":
		parents = selected
	case "insert-after":
		parents = selected
	case "insert-before":
		parents = collectUniqueParentIDs(state, selected)
	}
	parents = uniqueStrings(parents)

	next := &Change{
		ID:          nextID,
		Parents:     parents,
		Description: strings.TrimSpace(opts.Message),
		CreatedAt:   time.Now().UTC().Format(time.RFC3339),
		AuthorName:  current.AuthorName,
		AuthorEmail: current.AuthorEmail,
	}
	state.Changes[nextID] = next

	switch mode {
	case "insert-after":
		replaceParentsWith(state, selected, nextID)
	case "insert-before":
		for _, id := range selected {
			ch := state.Changes[id]
			if ch == nil {
				continue
			}
			ch.Parents = []string{nextID}
		}
	}

	if !opts.NoEdit {
		state.CurrentChange = nextID
	}

	if err := saveState(statePath, state); err != nil {
		return err
	}
	if err := appendOperation(repoRoot, Operation{
		ID:            randomOperationID(),
		Timestamp:     time.Now().UTC().Format(time.RFC3339),
		Op:            "new",
		Details:       fmt.Sprintf("mode=%s created=%s parents=%s no_edit=%t", mode, nextID, strings.Join(parents, ","), opts.NoEdit),
		CurrentBefore: beforeCurrent,
		CurrentAfter:  state.CurrentChange,
		GitHeadBefore: beforeGitHead,
		GitHeadAfter:  state.GitHeadCommit,
	}); err != nil {
		return err
	}
	if opts.NoEdit {
		fmt.Printf("%s Created new change %s with parents %s (not editing)\n", paint(ansiGreen, "✓"), paint(ansiCyan, nextID), paint(ansiCyan, strings.Join(parents, ",")))
	} else {
		fmt.Printf("%s Created and edited new change %s with parents %s\n", paint(ansiGreen, "✓"), paint(ansiCyan, nextID), paint(ansiCyan, strings.Join(parents, ",")))
	}
	return nil
}

func Describe(path, message string) error {
	_, repoRoot, err := openRepo(path)
	if err != nil {
		return err
	}
	statePath := filepath.Join(repoRoot, metaDir, stateFile)
	state, err := loadState(statePath)
	if err != nil {
		return err
	}

	current := state.Changes[state.CurrentChange]
	if current == nil {
		return fmt.Errorf("state corruption: missing current change %s", state.CurrentChange)
	}
	beforeCurrent := state.CurrentChange
	beforeGitHead := state.GitHeadCommit
	current.Description = strings.TrimSpace(message)
	if current.Description == "" {
		current.Description = ""
	}

	if err := saveState(statePath, state); err != nil {
		return err
	}
	if err := appendOperation(repoRoot, Operation{
		ID:            randomOperationID(),
		Timestamp:     time.Now().UTC().Format(time.RFC3339),
		Op:            "describe",
		Details:       fmt.Sprintf("change %s", current.ID),
		CurrentBefore: beforeCurrent,
		CurrentAfter:  state.CurrentChange,
		GitHeadBefore: beforeGitHead,
		GitHeadAfter:  state.GitHeadCommit,
	}); err != nil {
		return err
	}
	fmt.Printf("%s Updated description for change %s\n", paint(ansiGreen, "✓"), paint(ansiCyan, current.ID))
	return nil
}

func Commit(path, messageOverride string) error {
	repo, repoRoot, err := openRepo(path)
	if err != nil {
		return err
	}
	statePath := filepath.Join(repoRoot, metaDir, stateFile)
	state, err := loadState(statePath)
	if err != nil {
		return err
	}

	current := state.Changes[state.CurrentChange]
	if current == nil {
		return fmt.Errorf("state corruption: missing current change %s", state.CurrentChange)
	}
	beforeCurrent := state.CurrentChange
	beforeGitHead := state.GitHeadCommit
	if current.GitCommit != "" {
		return fmt.Errorf("current change %s is already committed", current.ID)
	}

	wt, err := repo.Worktree()
	if err != nil {
		return fmt.Errorf("get worktree: %w", err)
	}
	if err := stageAll(wt); err != nil {
		return err
	}

	message := strings.TrimSpace(messageOverride)
	if message == "" {
		message = strings.TrimSpace(current.Description)
	}
	if message == "" {
		return errors.New("commit message is empty; run `tav describe <message>` or pass `tav commit <message>`")
	}

	name, email := current.AuthorName, current.AuthorEmail
	if strings.TrimSpace(name) == "" || strings.TrimSpace(email) == "" {
		envName, envEmail := authorFromEnv()
		if strings.TrimSpace(name) == "" {
			name = envName
		}
		if strings.TrimSpace(email) == "" {
			email = envEmail
		}
	}
	if strings.TrimSpace(name) == "" {
		name = "tav"
	}
	if strings.TrimSpace(email) == "" {
		email = "tav@local"
	}

	commitHash, err := wt.Commit(message, &git.CommitOptions{
		All: true,
		Author: &object.Signature{
			Name:  name,
			Email: email,
			When:  time.Now(),
		},
	})
	if err != nil {
		return fmt.Errorf("git commit: %w", err)
	}

	current.GitCommit = commitHash.String()
	current.Description = message
	current.AuthorName = name
	current.AuthorEmail = email
	state.CommitIndex[commitHash.String()] = current.ID
	state.GitHeadCommit = commitHash.String()

	if err := detachHEAD(repo, commitHash); err != nil {
		return err
	}

	nextID := randomChangeID()
	for state.Changes[nextID] != nil {
		nextID = randomChangeID()
	}
	next := &Change{
		ID:          nextID,
		Parents:     []string{current.ID},
		Description: "",
		CreatedAt:   time.Now().UTC().Format(time.RFC3339),
		AuthorName:  name,
		AuthorEmail: email,
	}
	state.Changes[nextID] = next
	state.CurrentChange = nextID

	if err := saveState(statePath, state); err != nil {
		return err
	}
	if err := appendOperation(repoRoot, Operation{
		ID:            randomOperationID(),
		Timestamp:     time.Now().UTC().Format(time.RFC3339),
		Op:            "commit",
		Details:       fmt.Sprintf("change %s committed as %s", current.ID, short(commitHash.String())),
		CurrentBefore: beforeCurrent,
		CurrentAfter:  state.CurrentChange,
		GitHeadBefore: beforeGitHead,
		GitHeadAfter:  state.GitHeadCommit,
	}); err != nil {
		return err
	}

	fmt.Printf("%s Committed change %s as %s\n", paint(ansiGreen, "✓"), paint(ansiCyan, current.ID), paint(ansiYellow, short(commitHash.String())))
	fmt.Printf("%s Created working change %s\n", paint(ansiGreen, "✓"), paint(ansiCyan, nextID))
	return nil
}

func Squash(path, targetID string) error {
	if strings.TrimSpace(targetID) == "" {
		return SquashWithOptions(path, SquashOptions{})
	}
	return SquashWithOptions(path, SquashOptions{Into: targetID})
}

func SquashWithOptions(path string, opts SquashOptions) error {
	repo, repoRoot, err := openRepo(path)
	if err != nil {
		return err
	}
	statePath := filepath.Join(repoRoot, metaDir, stateFile)
	state, err := loadState(statePath)
	if err != nil {
		return err
	}

	current := state.Changes[state.CurrentChange]
	if current == nil {
		return fmt.Errorf("state corruption: missing current change %s", state.CurrentChange)
	}
	beforeCurrent := state.CurrentChange
	beforeGitHead := state.GitHeadCommit

	modeDestCount := 0
	destMode := ""
	if len(opts.Onto) > 0 {
		modeDestCount++
		destMode = "onto"
	}
	if len(opts.InsertAfter) > 0 {
		modeDestCount++
		destMode = "insert-after"
	}
	if len(opts.InsertBefore) > 0 {
		modeDestCount++
		destMode = "insert-before"
	}
	if modeDestCount > 1 {
		return errors.New("use at most one of -o/--onto, -A/--insert-after, -B/--insert-before")
	}
	if strings.TrimSpace(opts.Revision) != "" && modeDestCount > 0 {
		return errors.New("-r/--revision is incompatible with -o/-A/-B")
	}

	sourceID, targetID, err := selectSquashSourceAndTarget(repo, state, opts)
	if err != nil {
		return err
	}
	if sourceID == targetID {
		return errors.New("cannot squash a change into itself")
	}

	source := state.Changes[sourceID]
	target := state.Changes[targetID]
	if source == nil {
		return fmt.Errorf("unknown squash source change %s", sourceID)
	}
	if target == nil {
		return fmt.Errorf("unknown squash target change %s", targetID)
	}
	if reachesChange(state, target.ID, source.ID) {
		return fmt.Errorf("cannot squash %s into descendant %s", source.ID, target.ID)
	}
	if source.GitCommit != "" && target.GitCommit != "" && !reachesChange(state, source.ID, target.ID) {
		return errors.New("committed-change squash requires target to be an ancestor of current")
	}
	if opts.KeepEmptied && strings.TrimSpace(source.GitCommit) != "" {
		return errors.New("--keep-emptied is not supported for committed source changes")
	}

	currentGit := source.GitCommit
	targetGit := target.GitCommit
	rewriteMap := map[string]string{}
	if currentGit != "" && targetGit != "" {
		rewriteMap, err = rewriteCommittedSquash(repo, state, source, target)
		if err != nil {
			return err
		}
		applyCommitRewrite(state, rewriteMap)
	}
	if currentGit == "" && targetGit != "" && !opts.KeepEmptied {
		newTargetHash, committed, err := materializeWorkingTreeIntoTarget(repo, target, opts)
		if err != nil {
			return err
		}
		if committed {
			target.GitCommit = newTargetHash
			state.GitHeadCommit = newTargetHash
			rewriteMap[targetGit] = newTargetHash
			targetGit = newTargetHash
		}
	}

	if strings.TrimSpace(opts.Message) != "" {
		target.Description = strings.TrimSpace(opts.Message)
	} else if !opts.UseDestinationMessage {
		target.Description = squashDescription(target.Description, source.Description)
	}
	if strings.TrimSpace(target.AuthorName) == "" {
		target.AuthorName = source.AuthorName
	}
	if strings.TrimSpace(target.AuthorEmail) == "" {
		target.AuthorEmail = source.AuthorEmail
	}
	if source.GitCommit != "" {
		// Carry forward materialized commit identity to the squash target so
		// committed changes can be merged in Tav's virtual graph.
		if len(rewriteMap) > 0 {
			target.GitCommit = rewriteMap[currentGit]
		} else {
			target.GitCommit = source.GitCommit
		}
	}

	if opts.KeepEmptied {
		source.Description = ""
		source.GitCommit = ""
		source.Parents = []string{target.ID}
		state.CurrentChange = source.ID
	} else {
		for _, child := range state.Changes {
			for i, p := range child.Parents {
				if p == source.ID {
					child.Parents[i] = target.ID
				}
			}
			child.Parents = uniqueStrings(child.Parents)
		}
		delete(state.Changes, source.ID)
		state.CurrentChange = ensureWorkingChangeOnParent(state, target.ID, target.AuthorName, target.AuthorEmail)
	}
	rebindCommitIndex(state, source.ID, target.ID)
	state.CommitIndex = buildCommitIndex(state)
	if len(rewriteMap) == 0 && target.GitCommit != "" && (beforeGitHead == currentGit || beforeGitHead == targetGit) {
		state.GitHeadCommit = target.GitCommit
	}
	if modeDestCount == 1 {
		var placeIDs []string
		switch destMode {
		case "onto":
			placeIDs, err = resolveRevList(state, opts.Onto)
		case "insert-after":
			placeIDs, err = resolveRevList(state, opts.InsertAfter)
		case "insert-before":
			placeIDs, err = resolveRevList(state, opts.InsertBefore)
		}
		if err != nil {
			return err
		}
		if len(placeIDs) != 1 {
			return errors.New("experimental destination placement currently requires exactly one revset target")
		}
		switch destMode {
		case "onto":
			if err := rebaseOnto(state, []string{target.ID}, placeIDs[0]); err != nil {
				return err
			}
		case "insert-after":
			if err := rebaseInsertAfter(state, []string{target.ID}, placeIDs[0]); err != nil {
				return err
			}
		case "insert-before":
			if err := rebaseInsertBefore(state, []string{target.ID}, placeIDs[0]); err != nil {
				return err
			}
		}
	}

	if err := saveState(statePath, state); err != nil {
		return err
	}
	if err := appendOperation(repoRoot, Operation{
		ID:            randomOperationID(),
		Timestamp:     time.Now().UTC().Format(time.RFC3339),
		Op:            "squash",
		Details:       fmt.Sprintf("squashed %s into %s keep_emptied=%t", source.ID, target.ID, opts.KeepEmptied),
		CurrentBefore: beforeCurrent,
		CurrentAfter:  state.CurrentChange,
		GitHeadBefore: beforeGitHead,
		GitHeadAfter:  state.GitHeadCommit,
	}); err != nil {
		return err
	}
	fmt.Printf("%s Squashed %s into %s\n", paint(ansiGreen, "✓"), paint(ansiCyan, source.ID), paint(ansiCyan, target.ID))
	return nil
}

func Rebase(path, ontoID string) error {
	return RebaseWithOptions(path, RebaseOptions{Onto: ontoID})
}

func Push(path string, opts PushOptions) error {
	repo, repoRoot, err := openRepo(path)
	if err != nil {
		return err
	}
	statePath := filepath.Join(repoRoot, metaDir, stateFile)
	state, err := loadState(statePath)
	if err != nil {
		return err
	}
	changed, err := reconcileStateWithGit(repo, state)
	if err != nil {
		return err
	}
	if changed {
		if err := saveState(statePath, state); err != nil {
			return err
		}
	}

	remote := strings.TrimSpace(opts.Remote)
	if remote == "" {
		remote = "origin"
	}
	mode := ""
	if len(opts.Revisions) > 0 {
		mode = "revisions"
	}
	if len(opts.Create) > 0 {
		if mode != "" {
			return errors.New("use one push mode at a time: either -r or -c")
		}
		mode = "create"
	}
	if mode == "" {
		return errors.New("specify revisions with -r <REVSETS> or -c <REVSETS>")
	}

	ids, err := resolveRevList(state, func() []string {
		if mode == "revisions" {
			return opts.Revisions
		}
		return opts.Create
	}())
	if err != nil {
		return err
	}
	if len(ids) == 0 {
		return errors.New("no revisions selected to push")
	}

	branchByCommit, err := branchesByCommit(repo)
	if err != nil {
		return err
	}

	tempRefs := make([]plumbing.ReferenceName, 0, len(ids))
	refspecSet := map[config.RefSpec]bool{}
	addRefspec := func(src, dst plumbing.ReferenceName) {
		refspecSet[config.RefSpec(fmt.Sprintf("%s:%s", src, dst))] = true
	}

	for _, id := range ids {
		ch := state.Changes[id]
		if ch == nil {
			return fmt.Errorf("state corruption: missing change %s", id)
		}
		if ch.GitCommit == "" {
			return fmt.Errorf("change %s has no git commit to push", id)
		}
		commitHash := plumbing.NewHash(ch.GitCommit)
		if mode == "revisions" {
			branches := branchByCommit[ch.GitCommit]
			if len(branches) == 0 {
				return fmt.Errorf("change %s (%s) has no local branch; use -c to push to autogenerated branch", id, short(ch.GitCommit))
			}
			for _, b := range branches {
				src := plumbing.ReferenceName("refs/heads/" + b)
				dst := plumbing.ReferenceName("refs/heads/" + b)
				addRefspec(src, dst)
			}
			continue
		}

		// -c mode: synthesize temporary local refs, then push to autogenerated remote branches.
		auto := autogeneratedBranchName(id)
		src := plumbing.ReferenceName("refs/tav/push/" + id)
		dst := plumbing.ReferenceName("refs/heads/" + auto)
		if err := repo.Storer.SetReference(plumbing.NewHashReference(src, commitHash)); err != nil {
			return fmt.Errorf("create temporary ref %s: %w", src, err)
		}
		tempRefs = append(tempRefs, src)
		addRefspec(src, dst)
	}
	defer func() {
		for _, rn := range tempRefs {
			_ = repo.Storer.RemoveReference(rn)
		}
	}()

	refspecs := make([]config.RefSpec, 0, len(refspecSet))
	for rs := range refspecSet {
		refspecs = append(refspecs, rs)
	}
	sort.Slice(refspecs, func(i, j int) bool { return string(refspecs[i]) < string(refspecs[j]) })
	if len(refspecs) == 0 {
		return errors.New("no refspecs to push")
	}

	if err := repo.Push(&git.PushOptions{
		RemoteName: remote,
		RefSpecs:   refspecs,
	}); err != nil && !errors.Is(err, git.NoErrAlreadyUpToDate) {
		return fmt.Errorf("push to %s failed: %w", remote, err)
	}

	if err := appendOperation(repoRoot, Operation{
		ID:            randomOperationID(),
		Timestamp:     time.Now().UTC().Format(time.RFC3339),
		Op:            "push",
		Details:       fmt.Sprintf("mode=%s remote=%s revisions=%s refspecs=%d", mode, remote, strings.Join(ids, ","), len(refspecs)),
		CurrentBefore: state.CurrentChange,
		CurrentAfter:  state.CurrentChange,
		GitHeadBefore: state.GitHeadCommit,
		GitHeadAfter:  state.GitHeadCommit,
	}); err != nil {
		return err
	}

	fmt.Printf("%s Pushed %d refspec(s) to %s using %s mode\n", paint(ansiGreen, "✓"), len(refspecs), paint(ansiBlue, remote), paint(ansiBlue, mode))
	return nil
}

func Advance(path string, opts AdvanceOptions) error {
	repo, repoRoot, err := openRepo(path)
	if err != nil {
		return err
	}
	statePath := filepath.Join(repoRoot, metaDir, stateFile)
	state, err := loadState(statePath)
	if err != nil {
		return err
	}
	changed, err := reconcileStateWithGit(repo, state)
	if err != nil {
		return err
	}
	if changed {
		if err := saveState(statePath, state); err != nil {
			return err
		}
	}

	remote := strings.TrimSpace(opts.Remote)
	if remote == "" {
		remote = "origin"
	}
	rev := strings.TrimSpace(opts.Revset)
	if rev == "" {
		rev = "@-"
	}
	id, err := resolveChangeRef(state, rev)
	if err != nil {
		return err
	}
	ch := state.Changes[id]
	if ch == nil {
		return fmt.Errorf("unknown change %s", id)
	}
	if strings.TrimSpace(ch.GitCommit) == "" {
		return fmt.Errorf("change %s has no git commit; commit it first", id)
	}

	bookmark, err := resolveAdvanceBookmark(repo, state, opts.Bookmark, ch.GitCommit)
	if err != nil {
		return err
	}
	commitHash := plumbing.NewHash(ch.GitCommit)
	localRef := plumbing.ReferenceName("refs/heads/" + bookmark)
	if err := repo.Storer.SetReference(plumbing.NewHashReference(localRef, commitHash)); err != nil {
		return fmt.Errorf("move local bookmark %s: %w", bookmark, err)
	}
	refspec := config.RefSpec(fmt.Sprintf("%s:%s", localRef, localRef))

	pushErr := repo.Push(&git.PushOptions{
		RemoteName: remote,
		RefSpecs:   []config.RefSpec{refspec},
		Force:      opts.Force,
	})
	if pushErr != nil && !errors.Is(pushErr, git.NoErrAlreadyUpToDate) {
		return fmt.Errorf("advance push failed (use --force if rewritten): %w", pushErr)
	}

	if err := appendOperation(repoRoot, Operation{
		ID:            randomOperationID(),
		Timestamp:     time.Now().UTC().Format(time.RFC3339),
		Op:            "advance",
		Details:       fmt.Sprintf("remote=%s bookmark=%s revision=%s force=%t", remote, bookmark, id, opts.Force),
		CurrentBefore: state.CurrentChange,
		CurrentAfter:  state.CurrentChange,
		GitHeadBefore: state.GitHeadCommit,
		GitHeadAfter:  state.GitHeadCommit,
	}); err != nil {
		return err
	}

	fmt.Printf("%s Advanced %s/%s to %s (%s)\n", paint(ansiGreen, "✓"), paint(ansiBlue, remote), paint(ansiBlue, bookmark), paint(ansiYellow, short(ch.GitCommit)), paint(ansiCyan, id))
	fmt.Printf("%s pushed refspec: %s\n", paint(ansiDim, "note:"), paint(ansiDim, string(refspec)))
	return nil
}

func RebaseWithOptions(path string, opts RebaseOptions) error {
	_, repoRoot, err := openRepo(path)
	if err != nil {
		return err
	}
	statePath := filepath.Join(repoRoot, metaDir, stateFile)
	state, err := loadState(statePath)
	if err != nil {
		return err
	}

	current := state.Changes[state.CurrentChange]
	if current == nil {
		return fmt.Errorf("state corruption: missing current change %s", state.CurrentChange)
	}
	beforeCurrent := state.CurrentChange
	beforeGitHead := state.GitHeadCommit
	if current.GitCommit != "" {
		return errors.New("rebase for committed changes is not implemented yet")
	}

	selectionMode, selected, err := selectRebaseRevisions(state, opts)
	if err != nil {
		return err
	}
	destMode, dest, err := selectRebaseDestination(state, opts)
	if err != nil {
		return err
	}
	if len(selected) == 0 {
		return errors.New("no revisions selected for rebase")
	}
	for _, id := range selected {
		ch := state.Changes[id]
		if ch == nil {
			return fmt.Errorf("state corruption: missing change %s", id)
		}
		if ch.GitCommit != "" {
			return fmt.Errorf("rebase for committed change %s is not implemented yet", id)
		}
	}
	for _, id := range selected {
		if id == dest {
			return errors.New("cannot rebase onto one of the selected revisions")
		}
	}

	switch destMode {
	case "onto":
		if err := rebaseOnto(state, selected, dest); err != nil {
			return err
		}
	case "insert-after":
		if err := rebaseInsertAfter(state, selected, dest); err != nil {
			return err
		}
	case "insert-before":
		if err := rebaseInsertBefore(state, selected, dest); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported rebase destination mode: %s", destMode)
	}

	if err := saveState(statePath, state); err != nil {
		return err
	}
	if err := appendOperation(repoRoot, Operation{
		ID:            randomOperationID(),
		Timestamp:     time.Now().UTC().Format(time.RFC3339),
		Op:            "rebase",
		Details:       fmt.Sprintf("mode=%s destination=%s selected=%s", selectionMode, destMode+":"+dest, strings.Join(selected, ",")),
		CurrentBefore: beforeCurrent,
		CurrentAfter:  state.CurrentChange,
		GitHeadBefore: beforeGitHead,
		GitHeadAfter:  state.GitHeadCommit,
	}); err != nil {
		return err
	}
	fmt.Printf("%s Rebased %d change(s) with %s to %s:%s\n", paint(ansiGreen, "✓"), len(selected), paint(ansiBlue, selectionMode), paint(ansiBlue, destMode), paint(ansiCyan, dest))
	return nil
}

func stageAll(wt *git.Worktree) error {
	status, err := wt.Status()
	if err != nil {
		return fmt.Errorf("read worktree status: %w", err)
	}
	for path, fs := range status {
		if isTavMetadataPath(path) {
			continue
		}
		switch fs.Worktree {
		case git.Unmodified:
			continue
		case git.Untracked, git.Modified, git.Added, git.Renamed, git.Copied:
			if _, err := wt.Add(path); err != nil {
				return fmt.Errorf("stage %s: %w", path, err)
			}
		case git.Deleted:
			if _, err := wt.Remove(path); err != nil {
				return fmt.Errorf("stage deletion %s: %w", path, err)
			}
		default:
			if _, err := wt.Add(path); err != nil {
				return fmt.Errorf("stage %s: %w", path, err)
			}
		}
	}
	return nil
}

func isTavMetadataPath(path string) bool {
	clean := filepath.Clean(path)
	return clean == metaDir || strings.HasPrefix(clean, metaDir+string(filepath.Separator))
}

func reachesChange(state *State, startID, targetID string) bool {
	seen := map[string]bool{}
	stack := []string{startID}
	for len(stack) > 0 {
		last := len(stack) - 1
		id := stack[last]
		stack = stack[:last]
		if id == targetID {
			return true
		}
		if seen[id] {
			continue
		}
		seen[id] = true
		ch := state.Changes[id]
		if ch == nil {
			continue
		}
		stack = append(stack, ch.Parents...)
	}
	return false
}

func selectLogIDs(state *State, revspec string) ([]string, error) {
	if state.CurrentChange == "" {
		return nil, errors.New("invalid state: missing current_change")
	}
	startRef := state.CurrentChange
	stopRef := ""
	hasRange := strings.Contains(revspec, "..")

	if strings.TrimSpace(revspec) != "" {
		if hasRange {
			parts := strings.SplitN(revspec, "..", 2)
			left := strings.TrimSpace(parts[0])
			right := strings.TrimSpace(parts[1])
			if right != "" {
				startRef = right
			}
			stopRef = left
		} else {
			startRef = strings.TrimSpace(revspec)
		}
	}

	startID, err := resolveChangeRef(state, startRef)
	if err != nil {
		return nil, err
	}
	stopID := ""
	if strings.TrimSpace(stopRef) != "" {
		stopID, err = resolveChangeRef(state, stopRef)
		if err != nil {
			return nil, err
		}
	}

	ids := make([]string, 0, 32)
	seen := map[string]bool{}
	cur := startID
	foundStop := false
	for cur != "" {
		if cur == stopID && stopID != "" {
			foundStop = true
			break
		}
		if seen[cur] {
			return nil, fmt.Errorf("cycle detected in change graph at %s", cur)
		}
		seen[cur] = true
		chg := state.Changes[cur]
		if chg == nil {
			return nil, fmt.Errorf("state corruption: missing change %s", cur)
		}
		ids = append(ids, cur)
		if len(chg.Parents) == 0 {
			break
		}
		cur = chg.Parents[0]
	}
	if stopID != "" && !foundStop {
		return nil, fmt.Errorf("range stop %s not found in ancestry of %s", stopID, startID)
	}
	return ids, nil
}

func resolveChangeRef(state *State, ref string) (string, error) {
	ref = strings.TrimSpace(ref)
	switch ref {
	case "", "@":
		if state.CurrentChange == "" {
			return "", errors.New("invalid state: missing current_change")
		}
		return state.CurrentChange, nil
	case "@-":
		cur := state.Changes[state.CurrentChange]
		if cur == nil {
			return "", errors.New("`@-` is not available (current change is missing)")
		}
		if len(cur.Parents) == 0 {
			fallback := inferPreviousChangeID(state, state.CurrentChange)
			if fallback == "" {
				return "", errors.New("`@-` is not available (current change has no parent)")
			}
			return fallback, nil
		}
		return cur.Parents[0], nil
	}
	if _, ok := state.Changes[ref]; ok {
		return ref, nil
	}

	matches := make([]string, 0, 4)
	for id := range state.Changes {
		if strings.HasPrefix(id, ref) {
			matches = append(matches, id)
		}
	}
	if len(matches) == 1 {
		return matches[0], nil
	}
	if len(matches) > 1 {
		sort.Strings(matches)
		return "", fmt.Errorf("ambiguous change id prefix %q: %s", ref, strings.Join(matches, ", "))
	}
	return "", fmt.Errorf("unknown change id %q", ref)
}

func selectRebaseRevisions(state *State, opts RebaseOptions) (string, []string, error) {
	source := strings.TrimSpace(opts.Source)
	branch := strings.TrimSpace(opts.Branch)
	revCount := 0
	if source != "" {
		revCount++
	}
	if branch != "" {
		revCount++
	}
	if len(opts.Revisions) > 0 {
		revCount++
	}
	if revCount > 1 {
		return "", nil, errors.New("use only one selector: --source/-s, --branch/-b, or --revisions/-r")
	}

	if revCount == 0 {
		branch = "@"
	}

	switch {
	case source != "":
		id, err := resolveChangeRef(state, source)
		if err != nil {
			return "", nil, err
		}
		return "source", collectDescendants(state, id, nil), nil
	case branch != "":
		id, err := resolveChangeRef(state, branch)
		if err != nil {
			return "", nil, err
		}
		return "branch", collectDescendants(state, id, nil), nil
	default:
		seen := map[string]bool{}
		out := make([]string, 0, len(opts.Revisions))
		for _, r := range opts.Revisions {
			id, err := resolveChangeRef(state, r)
			if err != nil {
				return "", nil, err
			}
			if seen[id] {
				continue
			}
			seen[id] = true
			out = append(out, id)
		}
		return "revisions", out, nil
	}
}

func selectRebaseDestination(state *State, opts RebaseOptions) (string, string, error) {
	onto := strings.TrimSpace(opts.Onto)
	after := strings.TrimSpace(opts.InsertAfter)
	before := strings.TrimSpace(opts.InsertBefore)
	count := 0
	if onto != "" {
		count++
	}
	if after != "" {
		count++
	}
	if before != "" {
		count++
	}
	if count != 1 {
		return "", "", errors.New("use exactly one destination: --onto/-o, --insert-after/-A, or --insert-before/-B")
	}
	switch {
	case onto != "":
		id, err := resolveChangeRef(state, onto)
		return "onto", id, err
	case after != "":
		id, err := resolveChangeRef(state, after)
		return "insert-after", id, err
	default:
		id, err := resolveChangeRef(state, before)
		return "insert-before", id, err
	}
}

func rebaseOnto(state *State, selected []string, ontoID string) error {
	selectedSet := make(map[string]bool, len(selected))
	for _, id := range selected {
		selectedSet[id] = true
	}
	if selectedSet[ontoID] {
		return errors.New("cannot rebase onto a selected revision")
	}
	roots := rootsWithinSelection(state, selectedSet)
	for _, r := range roots {
		if reachesChange(state, ontoID, r) {
			return fmt.Errorf("cannot rebase %s onto descendant %s", r, ontoID)
		}
	}
	for _, r := range roots {
		state.Changes[r].Parents = []string{ontoID}
	}
	return nil
}

func rebaseInsertAfter(state *State, selected []string, destID string) error {
	if err := rebaseOnto(state, selected, destID); err != nil {
		return err
	}
	selectedSet := make(map[string]bool, len(selected))
	for _, id := range selected {
		selectedSet[id] = true
	}
	tip, err := singleSelectionTip(state, selectedSet)
	if err != nil {
		return err
	}
	desc := collectDescendants(state, destID, selectedSet)
	for _, id := range desc {
		ch := state.Changes[id]
		if ch == nil {
			continue
		}
		changed := false
		for i, p := range ch.Parents {
			if p == destID {
				ch.Parents[i] = tip
				changed = true
			}
		}
		if changed {
			ch.Parents = uniqueStrings(ch.Parents)
		}
	}
	return nil
}

func rebaseInsertBefore(state *State, selected []string, destID string) error {
	dest := state.Changes[destID]
	if dest == nil {
		return fmt.Errorf("unknown rebase target change %s", destID)
	}
	base := ""
	if len(dest.Parents) > 0 {
		base = dest.Parents[0]
	}
	if base != "" {
		if err := rebaseOnto(state, selected, base); err != nil {
			return err
		}
	}

	selectedSet := make(map[string]bool, len(selected))
	for _, id := range selected {
		selectedSet[id] = true
	}
	tip, err := singleSelectionTip(state, selectedSet)
	if err != nil {
		return err
	}

	// Place the target branch on top of the rebased selection.
	dest.Parents = []string{tip}
	return nil
}

func rootsWithinSelection(state *State, selectedSet map[string]bool) []string {
	roots := make([]string, 0, len(selectedSet))
	for id := range selectedSet {
		ch := state.Changes[id]
		if ch == nil {
			continue
		}
		hasSelectedParent := false
		for _, p := range ch.Parents {
			if selectedSet[p] {
				hasSelectedParent = true
				break
			}
		}
		if !hasSelectedParent {
			roots = append(roots, id)
		}
	}
	sort.Strings(roots)
	return roots
}

func singleSelectionTip(state *State, selectedSet map[string]bool) (string, error) {
	children := childrenIndex(state)
	tips := make([]string, 0, len(selectedSet))
	for id := range selectedSet {
		hasChildInSelection := false
		for _, c := range children[id] {
			if selectedSet[c] {
				hasChildInSelection = true
				break
			}
		}
		if !hasChildInSelection {
			tips = append(tips, id)
		}
	}
	if len(tips) == 1 {
		return tips[0], nil
	}
	sort.Strings(tips)
	if len(tips) == 0 {
		return "", errors.New("unable to determine selected tip")
	}
	return "", fmt.Errorf("operation requires a single selected tip, got %d: %s", len(tips), strings.Join(tips, ", "))
}

func childrenIndex(state *State) map[string][]string {
	out := map[string][]string{}
	for id := range state.Changes {
		out[id] = nil
	}
	for childID, ch := range state.Changes {
		for _, p := range ch.Parents {
			out[p] = append(out[p], childID)
		}
	}
	for k := range out {
		sort.Strings(out[k])
	}
	return out
}

func collectDescendants(state *State, startID string, exclude map[string]bool) []string {
	children := childrenIndex(state)
	seen := map[string]bool{}
	out := make([]string, 0, 16)
	queue := []string{startID}
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		if seen[id] {
			continue
		}
		seen[id] = true
		if exclude == nil || !exclude[id] {
			out = append(out, id)
		}
		for _, c := range children[id] {
			if exclude != nil && exclude[c] {
				continue
			}
			queue = append(queue, c)
		}
	}
	sort.Strings(out)
	return out
}

func validateNewModes(opts NewOptions) (string, error) {
	modeCount := 0
	mode := "parents"
	if len(opts.InsertAfter) > 0 {
		modeCount++
		mode = "insert-after"
	}
	if len(opts.InsertBefore) > 0 {
		modeCount++
		mode = "insert-before"
	}
	if modeCount > 1 {
		return "", errors.New("use at most one of --insert-after/-A or --insert-before/-B")
	}
	return mode, nil
}

func selectedNewRefs(opts NewOptions, mode string) []string {
	switch mode {
	case "insert-after":
		return opts.InsertAfter
	case "insert-before":
		return opts.InsertBefore
	default:
		return opts.Parents
	}
}

func resolveRevList(state *State, refs []string) ([]string, error) {
	out := make([]string, 0, len(refs))
	seen := map[string]bool{}
	for _, r := range refs {
		id, err := resolveChangeRef(state, r)
		if err != nil {
			return nil, err
		}
		if seen[id] {
			continue
		}
		seen[id] = true
		out = append(out, id)
	}
	return out, nil
}

func selectSquashSourceAndTarget(repo *git.Repository, state *State, opts SquashOptions) (string, string, error) {
	implicitDefault := strings.TrimSpace(opts.Revision) == "" && len(opts.From) == 0 && strings.TrimSpace(opts.Into) == ""
	if len(opts.From) > 1 {
		return "", "", errors.New("multiple --from revisions are not implemented yet")
	}
	if strings.TrimSpace(opts.Revision) != "" && len(opts.From) > 0 {
		return "", "", errors.New("use either --revision/-r or --from/-f")
	}

	sourceRef := "@"
	if strings.TrimSpace(opts.Revision) != "" {
		sourceRef = strings.TrimSpace(opts.Revision)
	} else if len(opts.From) == 1 {
		sourceRef = strings.TrimSpace(opts.From[0])
	}
	sourceID, err := resolveChangeRef(state, sourceRef)
	if err != nil {
		return "", "", err
	}
	// If user ran plain `tav squash` while editing a root/target change,
	// prefer squashing the newest child into it.
	if implicitDefault {
		src := state.Changes[sourceID]
		if src != nil && len(src.Parents) == 0 {
			if child := newestChildOf(state, sourceID); child != "" {
				return child, sourceID, nil
			}
		}
	}

	targetRef := strings.TrimSpace(opts.Into)
	if targetRef == "" {
		source := state.Changes[sourceID]
		if source == nil {
			return "", "", fmt.Errorf("unknown squash source change %s", sourceID)
		}
		if len(source.Parents) > 0 {
			targetRef = source.Parents[0]
		} else {
			fallbackTarget, err := inferSquashTargetFromGitParent(repo, state, source)
			if err != nil {
				return "", "", err
			}
			if fallbackTarget == "" {
				fallbackTarget = inferPreviousChangeID(state, sourceID)
			}
			if fallbackTarget == "" {
				return "", "", errors.New("current change has no parent and no previous change; pass an explicit squash target")
			}
			targetRef = fallbackTarget
		}
	}
	targetID, err := resolveChangeRef(state, targetRef)
	if err != nil {
		return "", "", err
	}
	return sourceID, targetID, nil
}

func newestChildOf(state *State, parentID string) string {
	var bestID string
	var bestTime time.Time
	for id, ch := range state.Changes {
		if ch == nil {
			continue
		}
		isChild := false
		for _, p := range ch.Parents {
			if p == parentID {
				isChild = true
				break
			}
		}
		if !isChild {
			continue
		}
		t, err := time.Parse(time.RFC3339, ch.CreatedAt)
		if err != nil {
			continue
		}
		if bestID == "" || t.After(bestTime) {
			bestID = id
			bestTime = t
		}
	}
	return bestID
}

func collectUniqueParentIDs(state *State, ids []string) []string {
	out := make([]string, 0, len(ids))
	seen := map[string]bool{}
	for _, id := range ids {
		ch := state.Changes[id]
		if ch == nil {
			continue
		}
		for _, p := range ch.Parents {
			if strings.TrimSpace(p) == "" || seen[p] {
				continue
			}
			seen[p] = true
			out = append(out, p)
		}
	}
	return out
}

func replaceParentsWith(state *State, oldParents []string, newParent string) {
	oldSet := map[string]bool{}
	for _, id := range oldParents {
		oldSet[id] = true
	}
	for id, ch := range state.Changes {
		if ch == nil || id == newParent {
			continue
		}
		changed := false
		for i, p := range ch.Parents {
			if oldSet[p] {
				ch.Parents[i] = newParent
				changed = true
			}
		}
		if changed {
			ch.Parents = uniqueStrings(ch.Parents)
		}
	}
}

func squashDescription(targetDesc, sourceDesc string) string {
	targetDesc = strings.TrimSpace(targetDesc)
	sourceDesc = strings.TrimSpace(sourceDesc)
	if targetDesc == "" {
		return sourceDesc
	}
	if sourceDesc == "" {
		return targetDesc
	}
	return targetDesc + "\n\n" + sourceDesc
}

func uniqueStrings(values []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(values))
	for _, v := range values {
		if strings.TrimSpace(v) == "" || seen[v] {
			continue
		}
		seen[v] = true
		out = append(out, v)
	}
	return out
}

func rebindCommitIndex(state *State, oldChangeID, newChangeID string) {
	for h, id := range state.CommitIndex {
		if id == oldChangeID {
			state.CommitIndex[h] = newChangeID
		}
	}
}

func rewriteCommittedSquash(repo *git.Repository, state *State, current, target *Change) (map[string]string, error) {
	targetHash := plumbing.NewHash(target.GitCommit)
	currentHash := plumbing.NewHash(current.GitCommit)

	targetCommit, err := repo.CommitObject(targetHash)
	if err != nil {
		return nil, fmt.Errorf("load target commit %s: %w", short(target.GitCommit), err)
	}
	currentCommit, err := repo.CommitObject(currentHash)
	if err != nil {
		return nil, fmt.Errorf("load current commit %s: %w", short(current.GitCommit), err)
	}
	if len(targetCommit.ParentHashes) > 1 || len(currentCommit.ParentHashes) > 1 {
		return nil, errors.New("committed squash with merge commits is not supported yet")
	}
	ancestor, err := isFirstParentAncestor(repo, targetHash, currentHash)
	if err != nil {
		return nil, err
	}
	if !ancestor {
		return nil, errors.New("committed squash requires target commit to be an ancestor of current commit")
	}

	parentHashes := make([]plumbing.Hash, 0, 1)
	if len(targetCommit.ParentHashes) > 0 {
		parentHashes = append(parentHashes, targetCommit.ParentHashes[0])
	}
	msg := squashDescription(targetCommit.Message, currentCommit.Message)
	squashedHash, err := writeCommitObject(repo, currentCommit.TreeHash, parentHashes, currentCommit.Author, currentCommit.Committer, msg)
	if err != nil {
		return nil, err
	}

	rewrite := map[string]string{
		target.GitCommit:  squashedHash.String(),
		current.GitCommit: squashedHash.String(),
	}

	for _, id := range committedDescendantsBreadthFirst(state, current.ID) {
		ch := state.Changes[id]
		if ch == nil || ch.GitCommit == "" {
			continue
		}
		oldHash := plumbing.NewHash(ch.GitCommit)
		oldCommit, err := repo.CommitObject(oldHash)
		if err != nil {
			return nil, fmt.Errorf("load descendant commit %s: %w", short(ch.GitCommit), err)
		}
		if len(oldCommit.ParentHashes) > 1 {
			return nil, errors.New("rewriting merge commits is not supported yet")
		}

		newParents := make([]plumbing.Hash, 0, 1)
		if len(oldCommit.ParentHashes) > 0 {
			parent := oldCommit.ParentHashes[0]
			if mapped, ok := rewrite[parent.String()]; ok {
				parent = plumbing.NewHash(mapped)
			}
			newParents = append(newParents, parent)
		}
		newHash, err := writeCommitObject(repo, oldCommit.TreeHash, newParents, oldCommit.Author, oldCommit.Committer, oldCommit.Message)
		if err != nil {
			return nil, err
		}
		rewrite[ch.GitCommit] = newHash.String()
	}

	return rewrite, nil
}

func materializeWorkingTreeIntoTarget(repo *git.Repository, target *Change, opts SquashOptions) (string, bool, error) {
	if target == nil || strings.TrimSpace(target.GitCommit) == "" {
		return "", false, nil
	}
	wt, err := repo.Worktree()
	if err != nil {
		return "", false, fmt.Errorf("get worktree for squash materialization: %w", err)
	}
	if err := detachHEAD(repo, plumbing.NewHash(target.GitCommit)); err != nil {
		return "", false, err
	}
	if err := stageAll(wt); err != nil {
		return "", false, err
	}
	status, err := wt.Status()
	if err != nil {
		return "", false, fmt.Errorf("read worktree status: %w", err)
	}
	added, modified, deleted, untracked := summarizeWorktreeStatus(status)
	if added+modified+deleted+untracked == 0 {
		return "", false, nil
	}

	msg := strings.TrimSpace(opts.Message)
	if msg == "" && opts.UseDestinationMessage {
		msg = strings.TrimSpace(target.Description)
	}
	if msg == "" {
		msg = "squash: fold working tree into destination"
	}
	name, email := target.AuthorName, target.AuthorEmail
	if strings.TrimSpace(name) == "" || strings.TrimSpace(email) == "" {
		envName, envEmail := authorFromEnv()
		if strings.TrimSpace(name) == "" {
			name = envName
		}
		if strings.TrimSpace(email) == "" {
			email = envEmail
		}
	}
	if strings.TrimSpace(name) == "" {
		name = "tav"
	}
	if strings.TrimSpace(email) == "" {
		email = "tav@local"
	}

	h, err := wt.Commit(msg, &git.CommitOptions{
		All: true,
		Author: &object.Signature{
			Name:  name,
			Email: email,
			When:  time.Now(),
		},
	})
	if err != nil {
		return "", false, fmt.Errorf("materialize squash commit: %w", err)
	}
	if err := detachHEAD(repo, h); err != nil {
		return "", false, err
	}
	return h.String(), true, nil
}

func inferSquashTargetFromGitParent(repo *git.Repository, state *State, current *Change) (string, error) {
	if current == nil || strings.TrimSpace(current.GitCommit) == "" {
		return "", nil
	}
	co, err := repo.CommitObject(plumbing.NewHash(current.GitCommit))
	if err != nil {
		return "", fmt.Errorf("resolve git parent for squash target: %w", err)
	}
	if len(co.ParentHashes) == 0 {
		return "", nil
	}
	parent := co.ParentHashes[0].String()
	if id, ok := state.CommitIndex[parent]; ok {
		return id, nil
	}
	return "", nil
}

func inferPreviousChangeID(state *State, currentID string) string {
	var bestID string
	var bestTime time.Time
	for id, ch := range state.Changes {
		if id == currentID || ch == nil {
			continue
		}
		t, err := time.Parse(time.RFC3339, ch.CreatedAt)
		if err != nil {
			continue
		}
		if bestID == "" || t.After(bestTime) {
			bestID = id
			bestTime = t
		}
	}
	return bestID
}

func isFirstParentAncestor(repo *git.Repository, ancestor, descendant plumbing.Hash) (bool, error) {
	cur := descendant
	for {
		if cur == ancestor {
			return true, nil
		}
		co, err := repo.CommitObject(cur)
		if err != nil {
			return false, fmt.Errorf("load commit %s: %w", short(cur.String()), err)
		}
		if len(co.ParentHashes) == 0 {
			return false, nil
		}
		if len(co.ParentHashes) > 1 {
			return false, errors.New("merge commits are not supported in first-parent ancestry checks")
		}
		cur = co.ParentHashes[0]
	}
}

func writeCommitObject(repo *git.Repository, tree plumbing.Hash, parents []plumbing.Hash, author, committer object.Signature, message string) (plumbing.Hash, error) {
	enc := repo.Storer.NewEncodedObject()
	c := &object.Commit{
		Author:       author,
		Committer:    committer,
		Message:      message,
		TreeHash:     tree,
		ParentHashes: parents,
	}
	if err := c.Encode(enc); err != nil {
		return plumbing.ZeroHash, fmt.Errorf("encode commit: %w", err)
	}
	h, err := repo.Storer.SetEncodedObject(enc)
	if err != nil {
		return plumbing.ZeroHash, fmt.Errorf("write commit object: %w", err)
	}
	return h, nil
}

func committedDescendantsBreadthFirst(state *State, startID string) []string {
	children := childrenIndex(state)
	seen := map[string]bool{}
	q := []string{startID}
	out := make([]string, 0, 16)
	for len(q) > 0 {
		id := q[0]
		q = q[1:]
		if seen[id] {
			continue
		}
		seen[id] = true
		for _, ch := range children[id] {
			if seen[ch] {
				continue
			}
			out = append(out, ch)
			q = append(q, ch)
		}
	}
	return out
}

func applyCommitRewrite(state *State, rewrite map[string]string) {
	for _, ch := range state.Changes {
		if ch == nil || ch.GitCommit == "" {
			continue
		}
		if mapped, ok := rewrite[ch.GitCommit]; ok {
			ch.GitCommit = mapped
		}
	}
	if mapped, ok := rewrite[state.GitHeadCommit]; ok {
		state.GitHeadCommit = mapped
	}
}

func buildCommitIndex(state *State) map[string]string {
	out := map[string]string{}
	for id, ch := range state.Changes {
		if ch == nil || ch.GitCommit == "" {
			continue
		}
		out[ch.GitCommit] = id
	}
	return out
}

func autogeneratedBranchName(changeID string) string {
	id := strings.TrimSpace(changeID)
	if len(id) > 12 {
		id = id[:12]
	}
	return "tav/" + id
}

func resolveAdvanceBookmark(repo *git.Repository, state *State, requested, targetCommit string) (string, error) {
	if b := strings.TrimSpace(requested); b != "" {
		return b, nil
	}
	branchByCommit, err := branchesByCommit(repo)
	if err != nil {
		return "", err
	}
	if names := branchByCommit[state.GitHeadCommit]; len(names) > 0 {
		return names[0], nil
	}
	if names := branchByCommit[targetCommit]; len(names) > 0 {
		return names[0], nil
	}
	return "main", nil
}

func appendOperation(repoRoot string, op Operation) error {
	path := filepath.Join(repoRoot, metaDir, opLogFile)
	b, err := json.Marshal(op)
	if err != nil {
		return fmt.Errorf("marshal operation: %w", err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open operation log: %w", err)
	}
	defer f.Close()
	if _, err := f.Write(append(b, '\n')); err != nil {
		return fmt.Errorf("write operation log: %w", err)
	}
	return nil
}

func loadOperations(repoRoot string) ([]Operation, error) {
	path := filepath.Join(repoRoot, metaDir, opLogFile)
	b, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("read operation log: %w", err)
	}

	lines := strings.Split(string(b), "\n")
	ops := make([]Operation, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var op Operation
		if err := json.Unmarshal([]byte(line), &op); err != nil {
			return nil, fmt.Errorf("parse operation log entry: %w", err)
		}
		ops = append(ops, op)
	}
	return ops, nil
}

func randomOperationID() string {
	return randomChangeID()
}

func displayID(value string) string {
	if strings.TrimSpace(value) == "" {
		return "(none)"
	}
	return value
}

func displayHash(value string) string {
	if strings.TrimSpace(value) == "" {
		return "(none)"
	}
	return short(value)
}

func summarizeWorktreeStatus(status git.Status) (added, modified, deleted, untracked int) {
	for path, fs := range status {
		if isTavMetadataPath(path) {
			continue
		}
		switch fs.Worktree {
		case git.Added:
			added++
		case git.Modified, git.Renamed, git.Copied:
			modified++
		case git.Deleted:
			deleted++
		case git.Untracked:
			untracked++
		}
	}
	return
}

func ensureGitignoreHasTav(repoRoot string) error {
	gitignorePath := filepath.Join(repoRoot, ".gitignore")
	line := ".tav/"

	b, err := os.ReadFile(gitignorePath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("read .gitignore: %w", err)
	}
	if err == nil {
		lines := strings.Split(strings.ReplaceAll(string(b), "\r\n", "\n"), "\n")
		for _, l := range lines {
			if strings.TrimSpace(l) == line || strings.TrimSpace(l) == ".tav" {
				return nil
			}
		}
	}

	var out string
	if len(b) == 0 {
		out = line + "\n"
	} else {
		content := string(b)
		if !strings.HasSuffix(content, "\n") {
			content += "\n"
		}
		out = content + line + "\n"
	}
	if err := os.WriteFile(gitignorePath, []byte(out), 0o644); err != nil {
		return fmt.Errorf("update .gitignore: %w", err)
	}
	return nil
}

func paint(code, s string) string {
	if !colorsEnabled() {
		return s
	}
	return code + s + ansiReset
}

func colorsEnabled() bool {
	if strings.TrimSpace(os.Getenv("NO_COLOR")) != "" {
		return false
	}
	return strings.TrimSpace(os.Getenv("TERM")) != "dumb"
}

func commitParentIDs(commit *object.Commit, state *State) []string {
	ids := make([]string, 0, len(commit.ParentHashes))
	for _, ph := range commit.ParentHashes {
		h := ph.String()
		id, ok := state.CommitIndex[h]
		if !ok {
			id = randomChangeID()
			for state.Changes[id] != nil {
				id = randomChangeID()
			}
			state.CommitIndex[h] = id
			state.Changes[id] = &Change{
				ID:        id,
				GitCommit: h,
				CreatedAt: commit.Author.When.UTC().Format(time.RFC3339),
			}
		}
		ids = append(ids, id)
	}
	return ids
}

func labelsForChange(chg *Change, state *State, branchByCommit map[string][]string) []string {
	labels := make([]string, 0, 3)
	if chg.GitCommit != "" {
		if names, ok := branchByCommit[chg.GitCommit]; ok {
			labels = append(labels, names...)
		}
		if chg.GitCommit == state.GitHeadCommit {
			labels = append(labels, "git_head()")
		}
	}
	return labels
}

func branchesByCommit(repo *git.Repository) (map[string][]string, error) {
	it, err := repo.Branches()
	if err != nil {
		return nil, fmt.Errorf("list branches: %w", err)
	}
	defer it.Close()

	m := map[string][]string{}
	err = it.ForEach(func(ref *plumbing.Reference) error {
		h := ref.Hash().String()
		m[h] = append(m[h], ref.Name().Short())
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("iterate branches: %w", err)
	}

	for k := range m {
		sort.Strings(m[k])
	}
	return m, nil
}

func detachHEAD(repo *git.Repository, hash plumbing.Hash) error {
	if err := repo.Storer.SetReference(plumbing.NewHashReference(plumbing.HEAD, hash)); err != nil {
		return fmt.Errorf("detach HEAD: %w", err)
	}
	return nil
}

func openRepo(path string) (*git.Repository, string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, "", err
	}

	root, err := findRepoRoot(abs)
	if err != nil {
		return nil, "", err
	}

	repo, err := git.PlainOpenWithOptions(abs, &git.PlainOpenOptions{DetectDotGit: true})
	if err != nil {
		return nil, "", fmt.Errorf("open git repository from %s: %w", abs, err)
	}
	return repo, root, nil
}

func findRepoRoot(start string) (string, error) {
	cur := start
	for {
		gitDir := filepath.Join(cur, ".git")
		if _, err := os.Stat(gitDir); err == nil {
			return cur, nil
		}
		next := filepath.Dir(cur)
		if next == cur {
			return "", fmt.Errorf("unable to find .git directory from %s", start)
		}
		cur = next
	}
}

func loadState(path string) (*State, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("missing tav state: %s (run `tav init`)", path)
		}
		return nil, err
	}
	var s State
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, fmt.Errorf("parse state %s: %w", path, err)
	}
	if s.Changes == nil {
		s.Changes = map[string]*Change{}
	}
	if s.CommitIndex == nil {
		s.CommitIndex = map[string]string{}
	}
	return &s, nil
}

func saveState(path string, s *State) error {
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(path, append(b, '\n'), 0o644); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}

func randomChangeID() string {
	const letters = "abcdefghijklmnopqrstuvwxyz"
	b := make([]byte, 8)
	rb := make([]byte, 8)
	if _, err := rand.Read(rb); err != nil {
		panic(err)
	}
	for i := range b {
		b[i] = letters[int(rb[i])%len(letters)]
	}
	return string(b)
}

func fallback(value, dflt string) string {
	if strings.TrimSpace(value) == "" {
		return dflt
	}
	return value
}

func firstLine(s string) string {
	idx := strings.IndexByte(s, '\n')
	if idx == -1 {
		return s
	}
	return s[:idx]
}

func short(h string) string {
	if len(h) < 8 {
		return h
	}
	return h[:8]
}

func syntheticRevision(chg *Change) string {
	// Render a stable pseudo-revision for non-materialized working changes.
	s := chg.ID
	if len(s) > 8 {
		s = s[:8]
	}
	return s
}

func relativeTime(when string) string {
	t, err := time.Parse(time.RFC3339, when)
	if err != nil {
		return "unknown"
	}
	d := time.Since(t)
	if d < time.Minute {
		return "just now"
	}
	if d < time.Hour {
		m := int(d / time.Minute)
		if m == 1 {
			return "1 minute ago"
		}
		return fmt.Sprintf("%d minutes ago", m)
	}
	if d < 24*time.Hour {
		h := int(d / time.Hour)
		if h == 1 {
			return "1 hour ago"
		}
		return fmt.Sprintf("%d hours ago", h)
	}
	days := int(d / (24 * time.Hour))
	if days == 1 {
		return "1 day ago"
	}
	return fmt.Sprintf("%d days ago", days)
}

func authorFromCommitOrEnv(c *object.Commit) (string, string) {
	name := strings.TrimSpace(c.Author.Name)
	email := strings.TrimSpace(c.Author.Email)
	if name == "" {
		name = strings.TrimSpace(os.Getenv("GIT_AUTHOR_NAME"))
	}
	if email == "" {
		email = strings.TrimSpace(os.Getenv("GIT_AUTHOR_EMAIL"))
	}
	return name, email
}

func authorFromEnv() (string, string) {
	return strings.TrimSpace(os.Getenv("GIT_AUTHOR_NAME")), strings.TrimSpace(os.Getenv("GIT_AUTHOR_EMAIL"))
}
