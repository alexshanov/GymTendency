import re
from collections import Counter

log_file = "scraper_orchestrator.log"
failure_pattern = re.compile(r"\[FAIL\] ([A-Za-z0-9_]+):")

failures = []
with open(log_file, "r") as f:
    for line in f:
        match = failure_pattern.search(line)
        if match:
            failures.append(match.group(1))

c = Counter(failures)
print(f"Total failure events: {len(failures)}")
print(f"Unique failed meets: {len(c)}")
print("\nTop 10 failing meets:")
for meet_id, count in c.most_common(10):
    print(f"{meet_id}: {count} failures")

print(f"\nFailures for 9576: {c.get('9576', 0)}")
