import shutil
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Optional

import ray

from rooster.logger import logger
from rooster.server import JobStatus, ServerCluster, ServerJob, ServerProc


class HeldJobError(Exception):
    pass


class RayServerJob(ServerJob):
    def __init__(
        self,
        *args,
        port: int = 6379,
        address: Optional[str] = None,
        **kwargs,
    ) -> None:
        arguments = "start --disable-usage-stats --block"
        if address is None:
            arguments += f" --port {port} --head"
        else:
            arguments += f" --address {address}:{port}"

        existing = kwargs.pop("arguments", None)
        if existing is not None:
            arguments += " " + existing

        super().__init__(*args, arguments=arguments, **kwargs)


@dataclass
class RayCluster:
    head: ServerProc
    workers: ServerCluster

    @property
    def ip(self):
        return self.head.ip

    def connect(self, **kwargs):
        ray.init(f"ray://{self.ip}:10001", **kwargs)


@contextmanager
def deploy_ray_cluster(
    name: str,
    num_clients: int,
    log_dir: str,
    output_dir: Optional[str] = None,
    error_dir: Optional[str] = None,
    port: int = 6379,
    image: Optional[str] = None,
    head_kwargs: dict = None,
    client_kwargs: dict = None,
) -> RayCluster:
    if image is None:
        executable = shutil.which("ray")
        getenv = True
    else:
        executable = "ray"
        getenv = False

    default_kwargs = {
        "universe": "vanilla",
        "request_memory": 1024,
        "request_cpus": 1,
        "getenv": getenv,
    }

    head_kwargs = head_kwargs or default_kwargs
    head_job = RayServerJob(
        name=f"{name}-head",
        executable=executable,
        port=port,
        queue=1,
        log=log_dir,
        output=output_dir or log_dir,
        error=error_dir or log_dir,
        image=image,
        **head_kwargs,
    )

    # spin up a job for the head node of the cluster
    with head_job.deploy() as head_cluster:
        logger.info(
            "Head node job {} submitted with cluster id {}".format(
                f"{name}-head", head_cluster.id
            )
        )

        head_proc = head_cluster.procs[0]

        # wait until the job has been scheduled
        # on a node and get its IP address
        ip = None
        while True:
            # condor_q -l the config of the job to
            # check its status
            config = head_proc.query()

            if not config:
                # if condor_q didn't return anything, something
                # has gone wrong with the job and we can't continue.
                # Direct the user to the log and stderr file
                raise RuntimeError(
                    "Head node failed to start. Consult condor logs "
                    "and stderr at {} and {}".format(
                        head_proc.log_file, head_proc.err
                    )
                )

            # check the status of the job
            job_status = JobStatus(int(config["jobstatus"]))
            if job_status is JobStatus.RUNNING:
                # if it's currently running, then it should have
                # an associated IP address we can parse
                ip = head_proc._parse_ip(config)
                head_proc._ip = ip
                break
            elif job_status is JobStatus.HELD:
                # If it's held, raise an error and instruct
                # the user to figure out why that is
                raise HeldJobError(
                    "Head node job has been held. Run "
                    "'condor_q -analyze {}' for more information.".format(
                        head_proc.id
                    )
                )
            elif job_status.value > 2:
                # any job status greater than 2 indicates that
                # the job has either completed or otherwise been
                # interrupted or suspended.
                raise RuntimeError(
                    "Head node job status is {}. Consult condor logs "
                    "and stderr at {} and {}".format(
                        job_status, head_proc.log_file, head_proc.err
                    )
                )

            # Otherwise, we have a job status of <2 which
            # indicates that the job is still starting, so
            # sleep for a beat and then check again
            time.sleep(1)
            logger.debug("Waiting for head node to come online")
        logger.info(f"Head node online at IP address {ip}")

        # now spin up a batch of worker nodes, pointed
        # at the ip address of the head node
        client_kwargs = client_kwargs or default_kwargs
        worker_job = RayServerJob(
            name=f"{name}-worker",
            executable=executable,
            address=ip,
            port=port,
            queue=num_clients,
            log=log_dir,
            output=output_dir or log_dir,
            error=error_dir or log_dir,
            image=image,
            **head_kwargs,
        )

        # deploy the worker nodes in another context
        # so that they'll get spun down at the end
        with worker_job.deploy() as worker_cluster:
            logger.info(
                "Worker jobs {} submitted with cluster id {}".format(
                    f"{name}-worker", worker_cluster.id
                )
            )

            # wait until all the worker nodes have come online
            # TODO: should we add an argument like `ready_when`
            # that takes either "any" or "all" as arguments to
            # decide whether to return when at least one node
            # is ready or when all of them are?
            while True:
                # break the loop once all the jobs
                # indicate that they're running
                statuses = [p.get_status() for p in worker_cluster.procs]
                if all([s is JobStatus.RUNNING for s in statuses]):
                    break

                # check if any jobs have been held or errored
                # out for some reason
                held_procs = []
                completed_procs = []
                for status, proc in zip(statuses, worker_cluster.procs):
                    if status is JobStatus.HELD:
                        held_procs.append(proc)
                    elif status.value > 2:
                        completed_procs.append(proc)

                # if there are no issues and the jobs are just
                # still waiting to be scheduled, sleep a beat
                # and try again
                if not held_procs and not completed_procs:
                    time.sleep(1)
                    logger.debug("Waiting for clients to come online")
                    continue

                # otherwise raise an error indicating which
                # jobs have had which issues
                msg = "Some worker jobs in cluster {} failed to run: ".format(
                    worker_cluster.id
                )
                if held_procs:
                    msg += "\nJobs {} have been held".format(
                        ", ".join([i.id for i in held_procs])
                    )
                if completed_procs:
                    msg += "\nJobs {} have stopped running".format(
                        ", ".join([i.id for i in completed_procs])
                    )
                raise RuntimeError(msg)

            logger.info("Worker nodes are all online")
            yield RayCluster(head_proc, worker_cluster)
