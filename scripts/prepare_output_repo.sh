#!/bin/bash
# Prepare the output repository without deleting an unpushed commit.
# Usage: prepare_output_repo.sh <repo-path> <expected-origin-url>
set -Eeuo pipefail

repo_path="${1:?output repository path is required}"
expected_origin="${2:?expected origin URL is required}"

if ! git -C "$repo_path" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    echo "output repository is not a Git worktree: $repo_path" >&2
    exit 72
fi

# version.txt is a generated release marker. An interrupted/no-op conversion can
# leave only its unstaged worktree copy dirty before run.sh reaches the normal
# post-conversion cleanup. Recover that one proven-safe residue, while preserving
# every staged change and every other dirty path for explicit inspection.
dirty_state=$(git -C "$repo_path" status --porcelain --untracked-files=all)
if [ "$dirty_state" = " M version.txt" ]; then
    echo "output repository: restoring generated version.txt residue"
    git -C "$repo_path" restore --worktree -- version.txt
    dirty_state=""
fi

if [ -n "$dirty_state" ]; then
    echo "output repository has uncommitted changes; refusing destructive alignment" >&2
    git -C "$repo_path" status --short >&2
    exit 76
fi

git -C "$repo_path" remote set-url origin "$expected_origin"
git -C "$repo_path" fetch origin main
git -C "$repo_path" checkout main

local_head=$(git -C "$repo_path" rev-parse main)
remote_head=$(git -C "$repo_path" rev-parse origin/main)

if [ "$local_head" = "$remote_head" ]; then
    echo "output repository aligned at $local_head"
elif git -C "$repo_path" merge-base --is-ancestor origin/main main; then
    echo "output repository has an unpushed local commit; retrying push before conversion"
    git -C "$repo_path" push origin main
elif git -C "$repo_path" merge-base --is-ancestor main origin/main; then
    echo "output repository is behind origin/main; fast-forwarding"
    git -C "$repo_path" merge --ff-only origin/main
else
    echo "output repository diverged from origin/main; refusing automatic reset" >&2
    exit 78
fi

local_head=$(git -C "$repo_path" rev-parse main)
remote_head=$(git -C "$repo_path" rev-parse origin/main)
if [ "$local_head" != "$remote_head" ]; then
    echo "output repository alignment verification failed: $local_head != $remote_head" >&2
    exit 79
fi
