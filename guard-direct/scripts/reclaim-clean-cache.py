"""Reclaim clean file cache from this stopped benchmark container only."""
import errno
import json
import os
from pathlib import Path
import subprocess
import time


def plan_reclaim(current,file_bytes,shmem,dirty,writeback,target):
    clean=max(0,file_bytes-shmem-dirty-writeback)
    return min(max(0,current-target),clean)


def main():
    if os.environ.get('B200_RECLAIM_CLEAN_CACHE')!='1':return
    group=Path('/sys/fs/cgroup')
    limit=(group/'memory.max').read_text().strip()
    assert limit!='max' and int(limit)>0, 'Refusing host-root cgroup cleanup'
    assert 'kubepods' in Path('/proc/self/cgroup').read_text(), 'Expected Kubernetes container cgroup'
    before=int((group/'memory.current').read_text())
    stat={k:int(v) for k,v in (l.split() for l in (group/'memory.stat').read_text().splitlines())}
    amount=plan_reclaim(before,stat['file'],stat['shmem'],stat['file_dirty'],stat['file_writeback'],128*1024**3)
    record={'wall_ns':time.time_ns(),'before_bytes':before,'requested_clean_reclaim_bytes':amount,
            'target_charge_bytes':128*1024**3,'cgroup_limit_bytes':int(limit)}
    if amount:
        subprocess.run(['mount','-o','remount,bind,rw',str(group)],check=True)
        try:
            try:(group/'memory.reclaim').write_text(str(amount))
            except OSError as error:
                if error.errno!=errno.EAGAIN:raise
                record['partial_reclaim_errno']=error.errno
        finally:
            subprocess.run(['mount','-o','remount,bind,ro',str(group)],check=True)
    record['after_bytes']=int((group/'memory.current').read_text())
    out=Path(os.environ['ROOT'])/'logs/clean-cache-reclaims.jsonl'
    with out.open('a') as f:f.write(json.dumps(record)+'\n')
    print(json.dumps(record))


if __name__=='__main__':main()
