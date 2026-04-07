import re

def callback(commit, metadata):
    commit.message = re.sub(
        rb'Co-Authored-By: Claude Opus 4\.6 <noreply@anthropic\.com>\r?\n?',
        b'',
        commit.message,
        flags=re.IGNORECASE
    )
