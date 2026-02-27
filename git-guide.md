# Git & GitHub Guide

## Core Concepts

- **Working directory** — your local files
- **Staging area** — changes queued for the next commit (`git add`)
- **Local repo** — committed history on your machine
- **Remote** — the copy on GitHub (`origin`)

---

## Daily Workflow

```bash
git status                        # see what's changed
git diff                          # see line-level changes

git add <file>                    # stage a specific file
git add .                         # stage everything
git commit -m "describe the why"  # record staged changes locally
git push                          # send commits to GitHub
```

```bash
git pull                          # fetch + merge remote changes into current branch
git fetch origin                  # fetch only (doesn't touch local branch)
```

---

## Commits

```bash
git log                           # full commit history
git log --oneline                 # compact view
git log --oneline -10             # last 10 commits
git show <commit-hash>            # see diff for a specific commit
```

Good commit messages describe **why**, not what:
- `Add require_read_auth to booking endpoints` — good
- `changed bookings.py` — bad

---

## Branches

```bash
git branch                        # list local branches
git branch <name>                 # create branch
git switch <name>                 # switch to branch
git switch -c <name>              # create and switch in one step

git merge <name>                  # merge branch into current branch
git branch -d <name>              # delete branch (after merging)
```

Push a new branch to GitHub:
```bash
git push -u origin <name>         # -u sets upstream so future pushes just need `git push`
```

---

## Rolling Back

### Undo unstaged changes (working directory)
```bash
git restore <file>                # discard changes to a file
git restore .                     # discard all unstaged changes
```

### Unstage (undo git add)
```bash
git restore --staged <file>
```

### Undo the last commit (keep changes in working directory)
```bash
git reset HEAD~1
```

### Revert a commit (safe — creates a new undo commit)
```bash
git revert <commit-hash>          # use this on shared/pushed commits
```

### Hard reset to a previous commit (destructive — discards all changes after)
```bash
git reset --hard <commit-hash>    # local only — don't use on pushed commits
```

### View a previous version of a file without switching branches
```bash
git show <commit-hash>:<path/to/file>
```

---

## Stashing

Temporarily shelve uncommitted changes so you can do something else:

```bash
git stash                         # stash all uncommitted changes
git stash pop                     # restore the stash
git stash list                    # see all stashes
git stash drop                    # discard the stash
```

---

## Remote Management

```bash
git remote -v                     # show remote URLs
git remote set-url origin <url>   # change remote URL
```

SSH URL format:    `git@github.com:username/repo.git`
HTTPS URL format:  `https://github.com/username/repo.git`

---

## Authentication

### SSH (recommended)

Generate a key:
```bash
ssh-keygen -t ed25519 -C "your@email.com"
```

Add the public key to GitHub: **Settings → SSH and GPG keys → New SSH key**
```bash
cat ~/.ssh/id_ed25519.pub         # copy this into GitHub
```

Load key into the agent for the session:
```bash
ssh-add ~/.ssh/id_ed25519
```

Persist across reboots (macOS):
```bash
ssh-add --apple-use-keychain ~/.ssh/id_ed25519
```

Test connection:
```bash
ssh -T git@github.com
```

### Multiple GitHub Accounts

Create separate keys per account:
```bash
ssh-keygen -t ed25519 -f ~/.ssh/id_work -C "work@email.com"
ssh-keygen -t ed25519 -f ~/.ssh/id_personal -C "personal@email.com"
```

Add to `~/.ssh/config`:
```
Host github-work
    HostName github.com
    User git
    IdentityFile ~/.ssh/id_work

Host github-personal
    HostName github.com
    User git
    IdentityFile ~/.ssh/id_personal
```

Then use the alias in remote URLs:
```bash
git remote set-url origin git@github-work:org/repo.git
git remote set-url origin git@github-personal:username/repo.git
```

### HTTPS with Personal Access Token (PAT)

Use when SSH isn't available (e.g. server without your keys).

Generate a PAT: **GitHub → Settings → Developer settings → Personal access tokens → Tokens (classic)**

Embed in remote URL (store permanently):
```bash
git remote set-url origin https://<token>@github.com/username/repo.git
```

Or use the credential helper to cache it:
```bash
git config --global credential.helper store   # persists to ~/.git-credentials
git pull                                       # enter token once; cached after that
```

---

## Tags (Version Management)

```bash
git tag                           # list tags
git tag v1.0.0                    # create lightweight tag at HEAD
git tag -a v1.0.0 -m "Release"   # annotated tag (recommended)
git push origin v1.0.0            # push a specific tag
git push origin --tags            # push all tags
git checkout v1.0.0               # check out code at a tag (detached HEAD)
```

---

## Useful One-Liners

```bash
# See what's changed since the last push
git diff origin/main..HEAD

# Who changed what and when (line by line)
git blame <file>

# Search commit messages
git log --oneline --grep="auth"

# See all branches including remote
git branch -a

# Clean up remote-tracking branches that no longer exist
git remote prune origin

# Amend the last commit message (before pushing)
git commit --amend -m "corrected message"
```
