import re
import subprocess
from configparser import RawConfigParser
from contextlib import contextmanager
from enum import Enum
from typing import Optional

from pycondor.job import Job
from pycondor.utils import split_command_string


class FailedSubmitError(Exception):
    pass


class JobStatus(Enum):
    IDLE = 1
    RUNNING = 2
    REMOVING = 3
    COMPLETED = 4
    HELD = 5
    TRANSFERRING_OUTPUT = 6
    SUSPENDED = 7
    CANCELLED = 8
    FAILED = 9


def query(id: int) -> list[dict[str, str]]:
    result = subprocess.check_output(["condor_q", "-l", str(id)], text=True)
    if not result:
        return []

    raw_configs = [i for i in result.split("\n\n") if i]
    configs = []
    for raw in raw_configs:
        config = RawConfigParser()
        config.read_string(rf"[DEFAULT]\n{raw}")
        config = dict(config["DEFAULT"])
        config = {k: v.strip('"') for k, v in config.items()}
        configs.append(config)
    return configs


class ServerCluster:
    def __init__(self, id: int):
        self._id = id
        configs = query(id)
        procs = []
        for config in configs:
            proc = ServerProc(
                self,
                int(config["procid"]),
                log_file=config["userlog"],
                out=config["out"],
                err=config["err"],
            )
            procs.append(proc)

        self.procs: list[ServerProc] = procs

    def get_statuses(self):
        configs = query(self.id)
        configs = {int(c.pop("procid")): c for c in configs}
        statuses = []
        for proc in self.procs:
            try:
                config = configs.pop(proc.proc_id)
            except KeyError:
                status = proc.determine_exit_status()
            else:
                status = JobStatus(int(config["jobstatus"]))
            statuses.append(status)
        return statuses

    def is_ready(self):
        statuses = self.get_statuses()
        return all([i == JobStatus.RUNNING for i in statuses])

    def has_completed(self):
        statuses = self.get_statuses()
        completed = [
            JobStatus.COMPLETED,
            JobStatus.FAILED,
            JobStatus.SUSPENDED,
            JobStatus.CANCELLED,
        ]
        return all([i in completed for i in statuses])

    def has_failed(self, how="any"):
        statuses = self.get_statuses()
        fails = [i == JobStatus.FAILED for i in statuses]

        if how == "any":
            return any(fails)
        elif how == "all":
            return all(fails)
        else:
            raise ValueError(f"Unrecoganized argument {how} to how")

    @property
    def id(self):
        return self._id

    def rm(self) -> None:
        response = subprocess.run(
            ["condor_rm", str(self.id)], stderr=subprocess.STDOUT, text=True
        )
        if response.returncode:
            stdout = response.stdout
            if stdout.startswith("Couldn't find/remove all jobs"):
                return
            raise RuntimeError(
                "condor_rm for cluster {} failed with message {}".format(
                    self.id, stdout
                )
            )


def override(method):
    method.__doc__ = getattr(Job, method.__name__).__doc__
    return method


class ServerProc:
    def __init__(
        self,
        cluster: ServerCluster,
        proc_id: int,
        log_file: Optional[str] = None,
        out: Optional[str] = None,
        err: Optional[str] = None,
    ) -> None:
        self.cluster = cluster
        self.proc_id = proc_id

        self._ip = None
        if any([i is None for i in [log_file, out, err]]):
            config = query(self.id)
            if config:
                self._log_file = config["userlog"]
                self._err = config["err"]
                self._out = config["out"]

                if config["jobstatus"] == "2":
                    self._ip = self._parse_ip(config)
        else:
            self._log_file = log_file
            self._out = out
            self._err = err

    @property
    def cluster_id(self):
        return self.cluster.id

    @property
    def id(self):
        proc_id = str(self.proc_id).zfill(3)
        return f"{self.cluster_id}.{proc_id}"

    def query(self):
        result = query(self.id)
        if not result:
            return {}
        return result[0]

    def read_log(self):
        with open(self.log_file, "r") as f:
            return f.read()

    def parse_exit_code(self, log: str):
        for line in log.splitlines():
            if line.strip("\t ").startswith(
                "Job terminated of its own accord"
            ):
                match = re.search("(?<=with exit-code )[0-9]+", line)
                if match is None:
                    return None
                else:
                    return int(match.group(0))
        else:
            return None

    def parse_abort(self, log: str):
        for line in log.splitlines():
            if line.endswith("Job was aborted."):
                return True
        return False

    def determine_exit_status(self):
        # there's no config for this job, so assume
        # that it has finished and check its log
        # for any exit info
        log = self.read_log()
        exit_code = self.parse_exit_code(log)

        if exit_code is not None and exit_code > 0:
            # job exited of its own accord but with
            # a non-zero exit code, indicating a failure
            return JobStatus.FAILED
        elif exit_code == 0:
            # job exited of its own accord with exit
            # code zero, meaning the job completed
            # successfully
            return JobStatus.COMPLETED
        elif self.parse_abort(log):
            # the job didn't exit of its own accord,
            # but the log indicated it was aborted
            return JobStatus.CANCELLED
        else:
            # Anything else that might have caused
            # the job not to exist. TODO: this should
            # probably have its own status, e.g. UNKNOWN
            return JobStatus.COMPLETED

    def get_status(self):
        config = self.query()
        if not config:
            return self.determine_exit_status()
        return JobStatus(int(config["jobstatus"]))

    def _parse_ip(self, config):
        pci = config["publicclaimid"]
        match = re.search("(?<=^<)[0-9.]+", pci)
        if match is None:
            raise ValueError(
                f"Couldn't parse IP address from public claim id {pci}"
            )
        return match.group(0)

    @property
    def ip(self):
        if self._ip is not None:
            return self._ip

        config = self.query()
        status = config["jobstatus"]
        if status != "2":
            return None
        self._ip = self._parse_ip(config)
        return self._ip

    @property
    def log_file(self):
        if self._log_file is not None:
            return self._log_file

        config = self.query()
        if config:
            self._log_file = config["userlog"]
            return self._log_file
        else:
            return None

    @property
    def err(self):
        if self._err is not None:
            return self._err


class ServerJob(Job):
    def __init__(self, *args, image: Optional[str] = None, **kwargs) -> None:
        if image is not None:
            kwargs["getenv"] = False
            kwargs["transfer_executable"] = False

            requirements = kwargs.get("requirements")
            if requirements is None:
                requirements = "HasSingularity"
            else:
                requirements += " && HasSingularity"
            kwargs["requirements"] = requirements

        super().__init__(*args, **kwargs)
        self.image = image

    @override
    def submit_job(self, submit_options):
        """
        Overriding parent submit because it has no
        mechanism for catching submit errors.
        """
        # Ensure that submit file has been written
        if not self._built:
            raise ValueError("build() must be called before submit()")
        # Ensure that there are no parent relationships
        if len(self.parents) != 0:
            raise ValueError(
                "Attempting to submit a Job with parents. "
                "Interjob relationships requires Dagman."
            )
        # Ensure that there are no child relationships
        if len(self.children) != 0:
            raise ValueError(
                "Attempting to submit a Job with children. "
                "Interjob relationships requires Dagman."
            )

        # Construct and execute condor_submit command
        command = "condor_submit"
        if submit_options is not None:
            command += " {}".format(submit_options)
        command += " {}".format(self.submit_file)

        proc = subprocess.Popen(
            split_command_string(command),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        out, err = proc.communicate()
        if err:
            msg = err.strip().replace("ERROR: ", "")
            raise FailedSubmitError(msg)
        return out

    # TODO: override build to include +SingularityImage row

    @override
    def build_submit(self, makedirs=True, fancyname=True, submit_options=None):
        """
        Overriding build_submit to get output
        from submit_job
        """

        self.build(makedirs, fancyname)
        out = self.submit_job(submit_options=submit_options)
        match = re.search(r"(?<=submitted\sto\scluster )[0-9]+", out)
        if match is None:
            raise ValueError(
                "Something went wrong, couldn't retrieve cluster id "
                "from daemon response: '{}'".format(out)
            )
        cluster_id = match.group(0)
        return ServerCluster(cluster_id)

    @contextmanager
    def deploy(self, **submit_kwargs) -> ServerCluster:
        cluster = self.build_submit(**submit_kwargs)
        try:
            yield cluster
        finally:
            statuses = [i.get_status() for i in cluster.procs]
            cancellable = (JobStatus.RUNNING, JobStatus.IDLE, JobStatus.HELD)
            cancel = any([i in cancellable for i in statuses])
            if cancel:
                cluster.rm()
