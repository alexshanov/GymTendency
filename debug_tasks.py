import pandas as pd
import os

KSIS_CSV = "discovered_meet_ids_ksis.csv"
KSCORE_CSV = "discovered_meet_ids_kscore.csv"
LIVEMEET_CSV = "discovered_meet_ids_livemeet.csv"
MSO_CSV = "discovered_meet_ids_mso.csv"

all_tasks = []

# Load KScore
if os.path.exists(KSCORE_CSV):
    df = pd.read_csv(KSCORE_CSV)
    id_col = [c for c in df.columns if 'MeetID' in c][0]
    name_col = [c for c in df.columns if 'MeetName' in c][0]
    for _, row in df.iterrows():
        all_tasks.append(('kscore', str(row[id_col]), str(row[name_col])))

# Load LiveMeet
if os.path.exists(LIVEMEET_CSV):
    df = pd.read_csv(LIVEMEET_CSV)
    id_col = [c for c in df.columns if 'MeetID' in c][0]
    name_col = [c for c in df.columns if 'MeetName' in c][0]
    for _, row in df.iterrows():
        all_tasks.append(('livemeet', str(row[id_col]), str(row[name_col])))

# Load MSO (PAUSED - Replicating Logic)
if False and os.path.exists(MSO_CSV):
    pass

# Manual MSO
all_tasks.append(('mso', '33704', '2025 Mens HNI'))
all_tasks.append(('mso', '33619', 'Vegas Cup 2025 - Men'))

# Load KSIS
if os.path.exists(KSIS_CSV):
    df = pd.read_csv(KSIS_CSV)
    id_col = [c for c in df.columns if 'MeetID' in c][0]
    name_col = [c for c in df.columns if 'MeetName' in c][0]
    for _, row in df.iterrows():
        all_tasks.append(('ksis', str(row[id_col]), str(row[name_col])))

print(f"Total tasks: {len(all_tasks)}")
from collections import Counter
counts = Counter([t[0] for t in all_tasks])
print("Task Counts by Type:")
for k, v in counts.items():
    print(f"  {k}: {v}")
