"""CPU contracts for source pins, command construction and process ownership."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import xml.etree.ElementTree as ET

HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE / 'scripts'))
import workflow

class Contracts(unittest.TestCase):
    def test_manifest_matches_exact_locks(self):
        pins=json.loads((HERE/'config/pins.json').read_text())
        projects={p.attrib['path']:p.attrib for p in ET.parse(HERE.parent/'manifests/kvcr-guard-direct-vllm.xml').findall('project')}
        for name,row in pins['sources'].items():
            self.assertEqual(projects[name]['revision'],row['revision'])
        self.assertFalse(pins['sources']['dynamo']['wheel_build_commit_verified'])

    def test_plan_preserves_agentx_cache_and_single_rnic(self):
        plan=workflow.build_plan(Path('/workspace/reproduction'), 'unit-r1')
        self.assertEqual(plan['environment']['NIXL_UCX_SGL_ENABLE'],'1')
        self.assertEqual(plan['environment']['UCX_NET_DEVICES'],'mlx5_0:1')
        self.assertEqual(plan['environment']['UCX_TLS'],'rc,cuda_copy')
        self.assertIn('--router-session-affinity-mode',plan['commands']['frontend'])
        self.assertIn('soft',plan['commands']['frontend'])
        self.assertEqual(plan['kv_transfer']['kv_connector_extra_config']['cpu_bytes_to_use'],0)
        self.assertEqual(plan['kv_transfer']['kv_connector_extra_config']['guard_pool_bytes'],124*1024**3)
        args=plan['commands']['replay']
        self.assertEqual(args[args.index('--concurrency')+1],'72')
        self.assertEqual(args[args.index('--num-dataset-entries')+1],'393')
        self.assertNotIn('--fixed-schedule',args)
        self.assertNotIn('kv_connector_module_path',plan['kv_transfer'])

    def test_run_ids_cannot_escape_or_inject(self):
        for value in ('../escape','a/b','a;touch bad','', '.', '--bad'):
            with self.subTest(value=value),self.assertRaises(ValueError):workflow.validate_run_id(value)

    def test_pod_reserves_eight_gpus_and_can_request_rdma(self):
        pod=workflow.render_pod('unit-pod','unit-ns','rdma/example',None)
        limits=pod['spec']['containers'][0]['resources']['limits']
        self.assertEqual(limits['nvidia.com/gpu'],'8')
        self.assertEqual(limits['rdma/example'],'1')
        text=json.dumps(pod)
        self.assertNotIn('kvcr-dedicated',text)
        self.assertNotIn('tolerationSeconds',text)

    def test_stop_refuses_changed_identity(self):
        child=subprocess.Popen([sys.executable,'-c','import time;time.sleep(60)'],start_new_session=True)
        try:
            record=workflow.process_identity(child.pid)
            record['start_ticks']+=1
            with self.assertRaises(RuntimeError):workflow.stop_owned(record,timeout=1)
            self.assertIsNone(child.poll())
        finally:
            child.terminate();child.wait(timeout=5)

    def test_source_commands_and_docs_never_delete_pods(self):
        source=(HERE/'scripts/workflow.py').read_text()
        self.assertNotIn("['delete'",source)
        self.assertNotIn('pkill',source)
        self.assertNotIn('/proc/sys/vm/drop_caches',source)


class LifecycleContracts(unittest.TestCase):
    def test_pod_render_does_not_require_cluster_access(self):
        result=subprocess.run([sys.executable,str(HERE/'scripts/workflow.py'),'pod',
                               '--pod','offline-probe','--hostpath-rdma','--render-only'],
                              env=dict(os.environ,PATH='/nonexistent'),text=True,capture_output=True)
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertEqual(json.loads(result.stdout)['metadata']['name'],'offline-probe')

    def test_stop_owned_child_and_archive_integrity(self):
        import hashlib
        import tarfile
        process=subprocess.Popen([sys.executable,'-c','import time;time.sleep(60)'],start_new_session=True)
        try:
            workflow.stop_owned(workflow.process_identity(process.pid),timeout=5)
            self.assertIsNotNone(process.poll())
        finally:
            if process.poll() is None:process.terminate();process.wait()
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);(root/'raw').mkdir();(root/'raw/evidence.json').write_text('{"verified":true}\n')
            workflow.atomic(root/'status.json',{'state':'completed'})
            workflow.collect_run(root)
            receipt=workflow.read(root/'collection.json')
            self.assertEqual(receipt['sha256'],workflow.digest(root/'artifacts.tar.gz'))
            self.assertTrue(receipt['verified'])
            with tarfile.open(root/'artifacts.tar.gz') as archive:
                for row in workflow.read(root/'artifacts.json'):
                    self.assertEqual(hashlib.sha256(archive.extractfile(row['path']).read()).hexdigest(),row['sha256'])

    def test_collection_refuses_live_services(self):
        process=subprocess.Popen([sys.executable,'-c','import time;time.sleep(60)'],start_new_session=True)
        try:
            with tempfile.TemporaryDirectory() as folder:
                root=Path(folder);workflow.atomic(root/'processes/worker.json',workflow.process_identity(process.pid))
                with self.assertRaisesRegex(RuntimeError,'live'):workflow.collect_run(root)
        finally:
            process.terminate();process.wait()

    def test_frozen_analysis_has_not_drifted(self):
        rows=workflow.read(HERE/'config/frozen-analysis-hashes.json')
        for name,expected in rows.items():
            with self.subTest(name=name):self.assertEqual(workflow.digest(HERE/name),expected)

class CaptureContracts(unittest.TestCase):
    def test_finalization_supplies_analyzer_receipt_and_rejects_open_health(self):
        spec=importlib.util.spec_from_file_location('validate_closed',HERE/'scripts/validate-closed.py')
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);result=root/'raw/kvcr/phases/measured/results/guard'
            health=root/'raw/kvcr/phases/measured/telemetry-health'
            identities=[]
            for rank in range(8):
                agent=f'worker-{rank}';guard=f'KVCR-Guard-{rank}';identities.extend([agent,guard])
                workflow.atomic(result/f'option-a-rank-{rank}.json',dict(rank=rank,pid=rank+100,agent=agent))
                for pid,identity in [(rank+100,agent),(rank+200,guard)]:
                    workflow.atomic(health/f'kvcr-{pid}.json',dict(pid=pid,closed=True,identities=[identity]))
            def validator(path,expected):
                self.assertEqual(set(expected),set(identities))
                return dict(valid=True,files=[dict(pid=pid) for pid in [*range(100,108),*range(200,208)]])
            receipt=module.validate(root,result,validator)
            self.assertTrue(receipt['valid'])
            self.assertEqual(len(receipt['expected_pids']),16)
            self.assertEqual(receipt,workflow.read(result/'capture-validity.json'))
            workflow.atomic(health/'kvcr-100.json',dict(pid=100,closed=False,identities=['worker-0']))
            self.assertFalse(module.validate(root,result,validator)['valid'])

if __name__=='__main__':unittest.main()
