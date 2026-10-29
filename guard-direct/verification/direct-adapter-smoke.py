#!/usr/bin/env python3
"""Real installed spec/metadata/worker/failure bridge on two GPUs, no model.

The isolated two-Guard pools are16MiB each. Their actual sizes are recorded;
this probe does not certify the serving124GiB budget or model geometry.
"""
import argparse,hashlib,json,os,signal,socket,subprocess,sys,threading,time,uuid
from pathlib import Path

PAGE=1_228_800
DIGEST=hashlib.sha256(b'direct-adapter-native-interface-v1').hexdigest()
def free_port():
 with socket.socket() as s:s.bind(('127.0.0.1',0));return s.getsockname()[1]
def until(function,seconds=30):
 end=time.monotonic()+seconds
 while time.monotonic()<end:
  value=function()
  if value:return value
  time.sleep(.001)
 raise TimeoutError('Native adapter interface condition did not complete')
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
 if a.output.exists():raise FileExistsError(a.output)
 assert os.environ['NIXL_UCX_SGL_ENABLE']=='1' and os.environ['UCX_NET_DEVICES']=='mlx5_0:1'
 root=a.output.parent/'direct-interface';root.mkdir(exist_ok=False)
 socket_root=Path('/dev/shm')/f'kvcr-direct-interface-{os.getpid()}';socket_root.mkdir()
 (socket_root/'meta').mkdir();(socket_root/'pools').mkdir();os.environ['KVCR_DIRECT_META_DIR']=str(socket_root/'meta')
 os.environ['RUN_DIR']=str(root);os.environ['KVCR_TELEMETRY_DIR']=str(root/'nixl-transfers')
 import torch
 from types import SimpleNamespace as N
 from prometheus_client import CollectorRegistry,Counter,Gauge,Histogram,generate_latest
 from functools import partial
 from vllm.v1.kv_offload.config import OffloadingConfig,OffloadingGroupConfig,OffloadingParallelConfig,OffloadingModelConfig,OffloadingCacheConfig
 from vllm.v1.kv_offload.base import CanonicalKVCaches,CanonicalKVCacheTensor,CanonicalKVCacheRef,GPULoadStoreSpec,ReqContext,LookupResult
 from vllm.v1.kv_offload.factory import OffloadingSpecFactory
 from vllm.distributed.kv_transfer.kv_connector.v1.offloading.metrics import OffloadPromMetrics
 from vllm.distributed.kv_transfer.kv_connector.v1.offloading.common import OffloadingWorkerMetadata
 from vllm.distributed.kv_transfer.kv_connector.v1.kvcr.guard_direct.connector import DirectConnectorWorker,KVCRDirectGuardConnector
 ports=[free_port(),free_port()];workers=[];managers=[];specs=[];service=None;proof=None;log=None
 keys=[(i+1).to_bytes(8,'big')+group.to_bytes(4,'big') for i,group in enumerate([0,2])]
 tensors=[];records=[]
 try:
  log=(root/'service.log').open('wb')
  service=subprocess.Popen([sys.executable,'-m','kvcr.kvcr_service','--socket-path',str(socket_root/'service.sock'),'--pool-dir',str(socket_root/'pools'),'--guard-count','2','--pool-sizes-gb','0.015625','--compatibility-digest',DIGEST],stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
  def service_ready():
   if service.poll() is not None:raise RuntimeError('Native interface Guard service exited: '+(root/'service.log').read_text(errors='replace')[-4000:])
   return (socket_root/'service.sock').exists()
  until(service_ready,90)
  for rank in range(2):
   torch.cuda.set_device(rank);tensor=torch.full((9,PAGE),0x5a,dtype=torch.int8,device=f'cuda:{rank}');tensors.append(tensor)
   extra=dict(spec_name='KVCRDirectGuardSpec',spec_module_path='vllm.distributed.kv_transfer.kv_connector.v1.kvcr.guard_direct.spec',cpu_bytes_to_use=0,guard_pool_bytes=124*1024**3,
       guard_socket_path=str(socket_root/'service.sock'),compatibility_digest=DIGEST,control_ports=ports,control_advertise_host='127.0.0.1',enable_telemetry=True,self_describing_kv_events=True,router_hint_block_size=256)
   config=OffloadingConfig(groups=(OffloadingGroupConfig(256,('packed',),0),OffloadingGroupConfig(256,('packed',),2)),worker_kv_bytes_per_block=PAGE,enable_kv_cache_events=True,extra_config=extra,engine_id='direct-interface-'+str(rank),model=OffloadingModelConfig('native-interface','uint8'),cache=OffloadingCacheConfig(256,1),parallel=OffloadingParallelConfig(rank=0,world_size=1,tp_size=1,pp_size=1,pcp_size=1,dcp_size=1,data_parallel_index=rank,data_parallel_size=2,data_parallel_rank_local=rank,is_parallelism_agnostic=True))
   spec=OffloadingSpecFactory.create_spec(config);specs.append(spec);manager=spec.get_manager();managers.append(manager)
   worker=spec.get_worker(CanonicalKVCaches([CanonicalKVCacheTensor(tensor,PAGE)],[[CanonicalKVCacheRef(0,PAGE)],[CanonicalKVCacheRef(0,PAGE)]]));workers.append(worker)
   records.append(dict(rank=rank,extra_config=extra,registered_region=worker.layout.region_kwargs(),visibility=worker.actor.visibility.proof(),actual_guard_bytes=sum(p[1] for p in worker.actor.client._core._local_dram._pools.values())))
  source,target=workers;source_manager,target_manager=managers
  context=ReqContext('direct-source');source_manager.on_new_request(context)
  torch.cuda.set_device(0);tensors[0][1].fill_(65);tensors[0][6].fill_(66)
  store=source_manager.prepare_store(keys,context)
  source.submit_store(1,GPULoadStoreSpec([1,6],[1,1],[0,0]),store.store_spec);source.wait({1})
  result=source.get_finished();assert len(result)==1 and result[0].success,result
  def source_ready():
   result=[source_manager.lookup(k,context) for k in keys];source_manager.on_schedule_end(None)
   return all(r is LookupResult.HIT for r in result)
  until(source_ready)
  source.submit_load(2,source_manager.prepare_load(keys,context),GPULoadStoreSpec([3,8],[1,1],[0,0]));source.wait({2});assert source.get_finished()[0].success
  torch.cuda.synchronize(0);assert bool(torch.all(tensors[0][3]==65)) and bool(torch.all(tensors[0][8]==66))
  # Reproduce the r3 failure without a model: pause only the source native
  # progress thread, with no accepted remote writes, past its real watchdog.
  watchdog=source.actor.client._core._remote_fw_dram._dangling_ops
  assert not any(s.accepted for s in watchdog.source_writes.values())
  epoch_before=watchdog.source_progress_epoch
  begin_poll=watchdog.begin_poll;paused=threading.Event();resume=threading.Event()
  watchdog_seconds=source.actor.client._core.config.operation_timeout_ms/1000
  def idle_pause():
   if not paused.is_set():
    paused.set()
    if not resume.wait(watchdog_seconds+15):raise TimeoutError('Interface idle pause was not released')
   begin_poll()
  watchdog.begin_poll=idle_pause
  try:
   assert paused.wait(30),'Source progress thread never reached idle pause'
   idle_start=time.monotonic();time.sleep(watchdog_seconds+.2)
   resume_at=time.monotonic();resume.set()
   until(lambda:watchdog._last_progress_at>=resume_at)
   assert not watchdog._source_stalled,'Idle gap poisoned source progress'
   assert watchdog.source_progress_epoch>epoch_before,'Idle gap did not fence the old epoch'
   idle_proof=dict(seconds=resume_at-idle_start,watchdog_ms=source.actor.client._core.config.operation_timeout_ms,
       accepted_source_writes=0,source_stalled=False,progress_epoch_before=epoch_before,
       progress_epoch=watchdog.source_progress_epoch)
  finally:
   resume.set();watchdog.begin_poll=begin_poll
  hint=lambda numbers:{'protocol_version':'0.1','message_id':uuid.uuid4().hex,'actions':[{'action_type':'kv.fetch','action_version':'1.0','payload':{'source_control_endpoint':f'tcp://127.0.0.1:{ports[0]}','block_hashes':numbers}}]}
  remote=ReqContext('direct-remote');remote.kv_hints=hint([1,2]);target_manager.on_new_request(remote)
  for key in keys:remote.set_offload_key_position(key,int.from_bytes(key[:-4],'big')*256)
  def remote_ready():
   result=[target_manager.lookup(k,remote) for k in keys];target_manager.on_schedule_end(None)
   return all(r is LookupResult.HIT for r in result)
  # The source framework/caller cannot make progress under this lock. Both
  # READY metadata and direct GET must be served by independent progress.
  with source.actor.client._core._state_lock:
   until(remote_ready)
   torch.cuda.set_device(1);target.submit_load(3,target_manager.prepare_load(keys,remote),GPULoadStoreSpec([3,8],[1,1],[0,0]));target.wait({3});assert target.get_finished()[0].success
   torch.cuda.synchronize(1);assert bool(torch.all(tensors[1][3]==65)) and bool(torch.all(tensors[1][8]==66))
  missing=(3).to_bytes(8,'big')+(2).to_bytes(4,'big');partial=ReqContext('direct-partial');partial.kv_hints=hint([1,3]);target_manager.on_new_request(partial)
  for key in [keys[0],missing]:partial.set_offload_key_position(key,int.from_bytes(key[:-4],'big')*256)
  def partial_ready():
   results=[target_manager.lookup(k,partial) for k in [keys[0],missing]];target_manager.on_schedule_end(None)
   return results[0] is LookupResult.HIT and results[1] is LookupResult.MISS
  until(partial_ready)
  missing_admission_verified=target_manager.lookup(missing,partial) is LookupResult.MISS
  assert missing_admission_verified
  race=ReqContext('direct-readiness-race');race.kv_hints=hint([1,2]);target_manager.on_new_request(race)
  for key in keys:race.set_offload_key_position(key,int.from_bytes(key[:-4],'big')*256)
  def race_ready():
   results=[target_manager.lookup(k,race) for k in keys];target_manager.on_schedule_end(None)
   return all(r is LookupResult.HIT for r in results)
  until(race_ready)
  race_spec=target_manager.prepare_load(keys,race)
  storage=source.actor.client._core._local_dram
  with storage._export_lock:
   withdrawn=storage._withdraw_locked(keys[1])
  assert withdrawn is not None,'Race key was not genuinely exported READY'
  storage._journal_withdrawal(keys[1],withdrawn,'verification_readiness_to_get_race')
  try:
   target.submit_load(4,race_spec,GPULoadStoreSpec([4,7],[1,1],[0,0]));target.wait({4})
   bridge=DirectConnectorWorker(specs[1],None,N(kv_cache_groups=[1,2]))
   bridge.worker=target;bridge._load_jobs={4:'direct-readiness-race'}
   connector=KVCRDirectGuardConnector.__new__(KVCRDirectGuardConnector);connector.connector_worker=bridge
   received=connector.get_transfer_results(set());assert received.finished_recving==received.failed_recving=={'direct-readiness-race'}
   assert connector.get_block_ids_with_load_errors()==set(),'Hybrid flat IDs are forbidden by vLLM'
   assert bridge.build_connector_worker_meta().completed_jobs=={4:1}
   torch.cuda.synchronize(1);assert bool(torch.all(tensors[1][4]==65)) and bool(torch.all(tensors[1][7]==0x5a))
   target_manager.mark_load_failure('direct-readiness-race');assert target_manager.lookup(keys[0],race) is LookupResult.MISS
  finally:
   storage._publish_export(keys[1],withdrawn[0])
  # Exercise the actual installed scheduler/IPC/worker cold lookup path at
  # production-sized metadata cardinality, independently of Guard capacity.
  cold=ReqContext('direct-large-cold');source_manager.on_new_request(cold)
  cold_keys=[i.to_bytes(8,'big')+(0).to_bytes(4,'big') for i in range(100000,116384)]
  cpu_start=time.thread_time();wall_start=time.monotonic()
  assert all(source_manager.lookup(k,cold) is LookupResult.RETRY for k in cold_keys)
  lookup_cpu=time.thread_time()-cpu_start
  source_manager.on_schedule_end(None)
  def cold_done():
   results=[source_manager.lookup(k,cold) for k in cold_keys];source_manager.on_schedule_end(None)
   return all(r is LookupResult.MISS for r in results)
  until(cold_done,60)
  metadata_proof=dict(keys=len(cold_keys),lookup_cpu_seconds=lookup_cpu,
      roundtrip_wall_seconds=time.monotonic()-wall_start,status='all MISS',framework_host_data_bytes=0)
  source_manager.on_request_finished(cold);source_manager.on_schedule_end(None)
  for manager in managers:
   manager.on_schedule_end(None);time.sleep(.02);manager.on_schedule_end(None)
   stats=manager.get_stats()
   if stats is not None:
    registry=CollectorRegistry();recorder=OffloadPromMetrics(N(kv_transfer_config=N(kv_connector_extra_config=extra)),{Counter:partial_metric(Counter,registry),Gauge:partial_metric(Gauge,registry),Histogram:partial_metric(Histogram,registry)},['model_name'],{0:['direct-interface']})
    recorder.observe(stats.data);assert generate_latest(registry)
  proof=dict(verified=True,scope='Real installed direct spec/actor/metadata/connector interface;16MiB isolated Guard pools, not serving budget',source_put_local_get_bytes_verified=True,remote_direct_gpu_bytes_verified=True,hybrid_partial_failure_verified=True,guard_ready_admission_verified=missing_admission_verified,source_caller_lock_held_for_remote_query_and_get=True,readiness_to_get_race_verified=True,failed_request='direct-readiness-race',hybrid_flat_invalid_ids=[],no_framework_cpu_data=True,idle_progress_recovery=idle_proof,large_cold_metadata=metadata_proof,records=records,worker_actor_error=[w.actor.error for w in workers])
 finally:
  errors=[]
  for manager in managers:
   try:manager.shutdown()
   except Exception as error:errors.append(repr(error))
  for worker in reversed(workers):
   try:worker.shutdown()
   except Exception as error:errors.append(repr(error))
  if service is not None:
   service.terminate()
   try:service.wait(timeout=60)
   except subprocess.TimeoutExpired:service.kill();service.wait(timeout=15)
  if log is not None:log.close()
  if errors:raise RuntimeError('Native interface cleanup failed: '+str(errors))
 if proof is None:raise RuntimeError('Missing native interface outcome')
 proof['native_quiescent']=[w.actor.quiescent and w.actor.client._core.is_quiescent() for w in workers]
 assert all(proof['native_quiescent'])
 a.output.write_text(json.dumps(proof,indent=2)+'\n');print(json.dumps(proof))

def partial_metric(metric,registry):
 from functools import partial
 return partial(metric,registry=registry)
if __name__=='__main__':main()
