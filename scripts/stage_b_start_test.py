#!/usr/bin/env python3
"""Measure how fast Stage-B-shaped Cloud Batch jobs start, per machine family and size.

Each job copies the Stage B job that terraform/workflow.yaml submits: STANDARD provisioning,
a 50 GB hyperdisk-balanced boot disk, no external IP, one task group, 2,000 mCPU and
7,168 MiB per task, `allowedLocations: regions/<region>`, and the packing rule in
src/epycloud/commands/run/validation.py (as many tasks per VM as fit). The task runs in a
container like Stage B and sleeps instead of calibrating.

For every job the script records when Batch accepted it, when each VM was created and
reached RUNNING, the Batch status events, and the task timeline. `collect` adds every VM
insert from the Compute Engine audit log, with error codes. Each job is deleted as soon as
it ends or reaches its deadline, and everything stops if the estimated spend passes
--cap-stop.

Usage:
  scripts/stage_b_start_test.py plan    --pass-id p1 --regions us-east4,us-central1
  scripts/stage_b_start_test.py run     --pass-id p1 --regions us-east4,us-central1 --out DIR
  scripts/stage_b_start_test.py collect   --pass-id p1 --out DIR
  scripts/stage_b_start_test.py summarize --out DIR
  scripts/stage_b_start_test.py cleanup   --pass-id p1 --regions us-east4,us-central1 --out DIR

Defaults match terraform/: network epymodelingsuite-network, subnets
epymodelingsuite-subnet-<region> (override per region with --subnet REGION=NAME, since the
control-plane region's subnet has no suffix), service account batch-runtime@<project>.
The subnets need Private Google Access. Batch's default VM image lives in project
batch-custom-image, so a compute.trustedImageProjects policy must allow that project.

Matrix: c4d, n4d, n4, c3d, c3 x -standard-8, -highcpu-8, -highmem-8, -standard-16
(c3-standard-22 for C3, which has no 16-vCPU size). Each family's jobs run at once, in
waves when the family's regional vCPU quota can't hold all four.

Needs gcloud (logged in) and Python 3.11+. No other packages.
"""

import argparse
import concurrent.futures
import csv
import datetime as dt
import json
import os
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

IMAGE = "mirror.gcr.io/library/busybox:1.36"

TASK_CPU_MILLI = 2000  # Stage B default, src/epycloud/commands/run/cloud/workflow.py
TASK_MEMORY_MIB = 7168
RESERVED_MEMORY_MIB = 1024  # src/epycloud/commands/run/validation.py
BOOT_DISK_GB = 50

FAMILIES = ["c4d", "n4d", "n4", "c3d", "c3"]
SIZES = ["standard-8", "highcpu-8", "highmem-8", "standard-16"]
# C3 has no 16-vCPU size, so c3-standard-22 takes the packing slot.
SIZE_FOR = {("c3", "standard-16"): "standard-22"}
VMS_PER_JOB = {
    "standard-8": 15,
    "highcpu-8": 15,
    "highmem-8": 15,
    "standard-16": 8,
    "standard-22": 6,
}
REGION_SHORT = {"us-east4": "use4", "us-central1": "usc1", "us-east5": "use5", "us-east1": "use1"}

# On-demand USD per hour, Compute Engine pricing pages, 2026-10-22. us-east4 and us-central1
# list the same rates for these types. Only the spend cap uses them.
PRICE = {
    "c4d-standard-8": 0.377981797,
    "c4d-highcpu-8": 0.317928224,
    "c4d-highmem-8": 0.498088943,
    "c4d-standard-16": 0.755963594,
    "n4d-standard-8": 0.3388,
    "n4d-highcpu-8": 0.28584,
    "n4d-highmem-8": 0.44472,
    "n4d-standard-16": 0.6776,
    "n4-standard-8": 0.3628,
    "n4-highcpu-8": 0.30616,
    "n4-highmem-8": 0.47608,
    "n4-standard-16": 0.7256,
    "c3d-standard-8": 0.363192,
    "c3d-highcpu-8": 0.299848,
    "c3d-highmem-8": 0.48988,
    "c3d-standard-16": 0.726384,
    "c3-standard-8": 0.403216,
    "c3-highcpu-8": 0.340208,
    "c3-highmem-8": 0.529232,
    "c3-standard-22": 1.108844,
}
UNKNOWN_PRICE = 1.2
# 50 GiB Hyperdisk Balanced at default performance (3,300 IOPS, 215 MiB/s): space plus the
# IOPS and throughput above the free 3,000 IOPS and 140 MiB/s.
DISK_PER_HOUR = 50 * 0.000109589 + 300 * 0.000006849 + 75 * 0.000054795

TERMINAL = {"SUCCEEDED", "FAILED", "CANCELLED"}
BATCH = "https://batch.googleapis.com/v1/"
COMPUTE = "https://compute.googleapis.com/compute/v1/projects/"


def utcnow():
    return dt.datetime.now(dt.UTC)


def iso(t):
    return t.astimezone(dt.UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ") if t else ""


def parse_ts(s):
    """Parse RFC 3339 from Batch (nanoseconds, Z) or Compute (milliseconds, offset)."""
    if not s:
        return None
    s = s.replace("Z", "+00:00")
    if "." in s:
        head, frac = s.split(".", 1)
        digits, rest = "", ""
        for i, ch in enumerate(frac):
            if ch.isdigit():
                digits += ch
            else:
                rest = frac[i:]
                break
        s = f"{head}.{digits[:6].ljust(6, '0')}{rest}"
    return dt.datetime.fromisoformat(s)


class Api:
    """Minimal REST client that uses the gcloud access token."""

    def __init__(self, log):
        self.token = None
        self.token_at = 0.0
        self.log = log

    def _token(self):
        if not self.token or time.time() - self.token_at > 1500:
            cmd = ["gcloud", "auth", "print-access-token"]
            self.token = subprocess.check_output(cmd, text=True).strip()
            self.token_at = time.time()
        return self.token

    def call(self, method, url, body=None, params=None, quota_project=None, ok404=False):
        if params:
            url += ("&" if "?" in url else "?") + urllib.parse.urlencode(params)
        data = json.dumps(body).encode() if body is not None else None
        for attempt in range(7):
            req = urllib.request.Request(url, data=data, method=method)
            req.add_header("Authorization", "Bearer " + self._token())
            req.add_header("Content-Type", "application/json")
            if quota_project:
                req.add_header("X-Goog-User-Project", quota_project)
            try:
                with urllib.request.urlopen(req, timeout=90) as r:
                    raw = r.read()
                    return json.loads(raw) if raw else {}
            except urllib.error.HTTPError as e:
                text = e.read().decode(errors="replace")
                if e.code == 404 and ok404:
                    return None
                if e.code in (429, 500, 502, 503, 504) and attempt < 6:
                    self.log(f"retry {method} {url} after HTTP {e.code}: {text[:300]}")
                    time.sleep(min(2**attempt, 30))
                    continue
                raise RuntimeError(f"{method} {url} -> HTTP {e.code}: {text}") from None
            except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
                if attempt < 6:
                    self.log(f"retry {method} {url} after {e!r}")
                    time.sleep(min(2**attempt, 30))
                    continue
                raise

    def paged(self, url, key, params=None):
        params = dict(params or {})
        out = []
        while True:
            page = self.call("GET", url, params=params) or {}
            if key == "items":
                out.append(page.get("items", {}))
            else:
                out.extend(page.get(key, []))
            tok = page.get("nextPageToken")
            if not tok:
                return out
            params["pageToken"] = tok


def family_quota(api, project, region):
    """vCPU quota per family in a region, read live from the Cloud Quotas API."""
    base = (
        f"https://cloudquotas.googleapis.com/v1/projects/{project}/locations/global/"
        "services/compute.googleapis.com/quotaInfos/"
    )
    out = {}
    info = api.call("GET", base + "CPUS-PER-VM-FAMILY-per-project-region", quota_project=project)
    for d in info.get("dimensionsInfos", []):
        dims = d.get("dimensions", {})
        value = d.get("details", {}).get("value")
        if dims.get("region") == region and dims.get("vm_family") and value is not None:
            out[dims["vm_family"].lower()] = int(value)
    info = api.call("GET", base + "C3-CPUS-per-project-region", quota_project=project)
    for d in info.get("dimensionsInfos", []):
        value = d.get("details", {}).get("value")
        if d.get("dimensions", {}).get("region") == region and value is not None:
            out["c3"] = int(value)
    return out


def machine_spec(api, project, region, machine_type, cache):
    if machine_type in cache:
        return cache[machine_type]
    reg = api.call("GET", f"{COMPUTE}{project}/regions/{region}")
    for zurl in sorted(reg["zones"]):
        zone = zurl.rsplit("/", 1)[1]
        url = f"{COMPUTE}{project}/zones/{zone}/machineTypes/{machine_type}"
        mt = api.call("GET", url, ok404=True)
        if mt:
            cache[machine_type] = (mt["guestCpus"], mt["memoryMb"])
            return cache[machine_type]
    raise RuntimeError(f"{machine_type} is not offered in any {region} zone")


def tasks_per_vm(vcpus, memory_mib):
    """The epycloud packing rule from src/epycloud/commands/run/validation.py."""
    by_cpu = vcpus * 1000 // TASK_CPU_MILLI
    by_memory = max(1, (memory_mib - RESERVED_MEMORY_MIB) // TASK_MEMORY_MIB)
    return min(by_cpu, by_memory)


def subnet_for(args, region):
    overrides = dict(item.split("=", 1) for item in args.subnet)
    name = overrides.get(region, args.subnet_template.format(region=region))
    return f"projects/{args.project}/regions/{region}/subnetworks/{name}"


def job_body(args, region, family, size, machine_type, vm_count, pack):
    tasks = vm_count * pack
    p = args.prefix
    labels = {
        f"{p}_pass": args.pass_id,
        f"{p}_region": region,
        f"{p}_family": family,
        f"{p}_size": size,
        f"{p}_machine": machine_type,
    }
    # Stage B: '[ "$BATCH_TASK_INDEX" -ge "$NUM_TASKS" ] && exit 0; exec /scripts/run_runner.sh'
    command = (
        '[ "$BATCH_TASK_INDEX" -ge "$NUM_TASKS" ] && exit 0; '
        'echo "task start index=$BATCH_TASK_INDEX"; sleep "$SLEEP_SECONDS"; '
        'echo "task done index=$BATCH_TASK_INDEX"'
    )
    return {
        "labels": labels,
        "taskGroups": [
            {
                "taskCount": tasks,
                "parallelism": tasks,
                "taskCountPerNode": pack,
                "taskSpec": {
                    "runnables": [
                        {
                            "container": {
                                "imageUri": args.image,
                                "entrypoint": "/bin/sh",
                                "commands": ["-c", command],
                            }
                        }
                    ],
                    "environment": {
                        "variables": {"NUM_TASKS": str(tasks), "SLEEP_SECONDS": str(args.sleep)}
                    },
                    "computeResource": {"cpuMilli": TASK_CPU_MILLI, "memoryMib": TASK_MEMORY_MIB},
                    "maxRunDuration": f"{args.sleep + 900}s",
                },
            }
        ],
        "logsPolicy": {"destination": "CLOUD_LOGGING"},
        "allocationPolicy": {
            "location": {"allowedLocations": [f"regions/{region}"]},
            "serviceAccount": {"email": args.service_account},
            "network": {
                "networkInterfaces": [
                    {
                        "network": f"projects/{args.project}/global/networks/{args.network}",
                        "subnetwork": subnet_for(args, region),
                        "noExternalIpAddress": True,
                    }
                ]
            },
            "instances": [
                {
                    "installGpuDrivers": False,
                    "policy": {
                        "machineType": machine_type,
                        "provisioningModel": "STANDARD",
                        "bootDisk": {"type": "hyperdisk-balanced", "sizeGb": BOOT_DISK_GB},
                    },
                }
            ],
            "labels": labels,
        },
    }


def build_plan(api, args, log):
    only = set(args.only.split(",")) if args.only else None
    specs = {}
    plan = []
    for region in args.regions.split(","):
        quota = family_quota(api, args.project, region)
        log(f"{region} vCPU quota by family: {json.dumps(quota, sort_keys=True)}")
        for family in FAMILIES:
            fam_jobs = []
            for size in SIZES:
                size = SIZE_FOR.get((family, size), size)
                machine_type = f"{family}-{size}"
                if only and machine_type not in only:
                    continue
                vcpus, memory_mib = machine_spec(api, args.project, region, machine_type, specs)
                vm_count = args.vms or VMS_PER_JOB[size]
                pack = tasks_per_vm(vcpus, memory_mib)
                short = REGION_SHORT.get(region, region.replace("-", ""))
                fam_jobs.append(
                    {
                        "id": f"{args.prefix}-{args.pass_id}-{short}-{machine_type}",
                        "region": region,
                        "family": family,
                        "size": size,
                        "machine_type": machine_type,
                        "vcpus_per_vm": vcpus,
                        "memory_mib_per_vm": memory_mib,
                        "vms": vm_count,
                        "tasks_per_vm": pack,
                        "tasks": vm_count * pack,
                        "job_vcpus": vm_count * vcpus,
                        "price_per_vm_hour": PRICE.get(machine_type, UNKNOWN_PRICE),
                    }
                )
            # Waves keep each family inside its regional vCPU quota.
            limit = quota.get(family, 0)
            wave, used = 1, 0
            for j in fam_jobs:
                if j["job_vcpus"] > limit:
                    raise RuntimeError(
                        f"{j['id']} needs {j['job_vcpus']} vCPU; {region} {family} quota is {limit}"
                    )
                if used + j["job_vcpus"] > limit:
                    wave, used = wave + 1, 0
                j["wave"] = wave
                j["after"] = [p["id"] for p in fam_jobs if p.get("wave") == wave - 1]
                used += j["job_vcpus"]
            for j in fam_jobs:
                j["quota_family_vcpus"] = limit
                j["body"] = job_body(
                    args,
                    j["region"],
                    j["family"],
                    j["size"],
                    j["machine_type"],
                    j["vms"],
                    j["tasks_per_vm"],
                )
            plan.extend(fam_jobs)
    return plan


def print_plan(plan, minutes, log):
    log(f"{'job':40} {'wave':>4} {'VMs':>4} {'vCPU':>5} {'tasks/VM':>8} {'tasks':>5} {'$/h':>7}")
    for j in plan:
        per_hour = j["vms"] * (j["price_per_vm_hour"] + DISK_PER_HOUR)
        log(
            f"{j['id']:40} {j['wave']:>4} {j['vms']:>4} {j['job_vcpus']:>5} "
            f"{j['tasks_per_vm']:>8} {j['tasks']:>5} {per_hour:>7.2f}"
        )
    vms = sum(j["vms"] for j in plan)
    usd = sum(j["vms"] * (j["price_per_vm_hour"] + DISK_PER_HOUR) for j in plan) * minutes / 60
    log(
        f"{len(plan)} jobs, {vms} VMs, {sum(j['job_vcpus'] for j in plan)} vCPU; "
        f"at {minutes} billed minutes per VM: {vms * minutes / 60:.1f} VM-hours, about ${usd:.2f}"
    )


class Run:
    def __init__(self, api, args, plan, out, log):
        self.api, self.args, self.plan, self.out, self.log = api, args, plan, out, log
        self.jobs = {
            j["id"]: dict(
                j,
                submitted_at=None,
                create_time=None,
                state=None,
                counts={},
                finalized_at=None,
                outcome=None,
                deleted_at=None,
                uid=None,
                name=None,
            )
            for j in plan
        }
        self.inst = {}
        self.t0 = utcnow()
        self.stop_reason = None
        os.makedirs(os.path.join(out, "jobs"), exist_ok=True)
        self.transitions = open(os.path.join(out, "transitions.jsonl"), "a")  # noqa: SIM115
        self.quota_rows = []
        self.lock = threading.Lock()
        self.label = f"{args.prefix}_"

    def note(self, **rec):
        rec["t"] = iso(utcnow())
        with self.lock:
            self.transitions.write(json.dumps(rec, sort_keys=True) + "\n")
            self.transitions.flush()

    def save(self, name, obj):
        with open(os.path.join(self.out, "jobs", name), "w") as f:
            json.dump(obj, f, indent=1, sort_keys=True)

    def submit(self, j):
        url = f"{BATCH}projects/{self.args.project}/locations/{j['region']}/jobs"
        self.save(f"{j['id']}.request.json", j["body"])
        self.log(f"submit {j['id']} payload: {json.dumps(j['body'], sort_keys=True)}")
        j["submitted_at"] = utcnow()
        resp = self.api.call("POST", url, j["body"], params={"job_id": j["id"]})
        self.save(f"{j['id']}.created.json", resp)
        j["name"], j["uid"] = resp["name"], resp.get("uid")
        j["create_time"] = parse_ts(resp.get("createTime"))
        self.note(
            kind="job_submitted",
            job=j["id"],
            uid=j["uid"],
            create_time=resp.get("createTime"),
            client_submit=iso(j["submitted_at"]),
        )
        self.log(f"submitted {j['id']} uid={j['uid']} createTime={resp.get('createTime')}")

    def submit_ready(self):
        ready = []
        for j in self.jobs.values():
            if j["submitted_at"] or self.stop_reason:
                continue
            deps = [self.jobs[a] for a in j["after"]]
            deps_vms_left = any(
                i["job"] in j["after"] and not i["gone_at"] for i in self.inst.values()
            )
            if all(d["finalized_at"] for d in deps) and not deps_vms_left:
                ready.append(j)
        if ready:
            with concurrent.futures.ThreadPoolExecutor(max_workers=24) as pool:
                list(pool.map(self.submit, ready))

    def poll_jobs(self, now):
        for region in sorted({j["region"] for j in self.jobs.values()}):
            url = f"{BATCH}projects/{self.args.project}/locations/{region}/jobs"
            by_name = {x["name"]: x for x in self.api.paged(url, "jobs", {"pageSize": 200})}
            for j in self.jobs.values():
                if j["region"] != region or not j["name"] or j["finalized_at"]:
                    continue
                x = by_name.get(j["name"])
                if not x:
                    continue
                st = x.get("status", {})
                state = st.get("state")
                counts = st.get("taskGroups", {}).get("group0", {}).get("counts", {})
                if state != j["state"] or counts != j["counts"]:
                    self.note(kind="job_state", job=j["id"], state=state, counts=counts)
                    self.log(f"{j['id']}: {state} {json.dumps(counts, sort_keys=True)}")
                j["state"], j["counts"] = state, counts
                if state in TERMINAL:
                    self.finalize(j, state)

    def poll_instances(self, now):
        url = f"{COMPUTE}{self.args.project}/aggregated/instances"
        params = {
            "filter": f'labels.{self.label}pass="{self.args.pass_id}"',
            "returnPartialSuccess": "true",
            "maxResults": 500,
        }
        seen = set()
        for items in self.api.paged(url, "items", params):
            for block in items.values():
                for x in block.get("instances", []):
                    name = x["name"]
                    seen.add(name)
                    rec = self.inst.get(name) or self._new_instance(x, now)
                    rec["last_seen"] = iso(now)
                    if x.get("lastStartTimestamp"):
                        rec["last_start"] = x["lastStartTimestamp"]
                    if x["status"] not in rec["status_first_seen"]:
                        rec["status_first_seen"][x["status"]] = iso(now)
                        self.note(
                            kind="instance_status",
                            name=name,
                            job=rec["job"],
                            status=x["status"],
                            last_start=rec["last_start"],
                        )
        for name, rec in self.inst.items():
            if name not in seen and not rec["gone_at"]:
                rec["gone_at"] = iso(now)
                self.note(kind="instance_gone", name=name, job=rec["job"])

    def _new_instance(self, x, now):
        labels = x.get("labels", {})
        job = labels.get("batch-job-id")
        if job not in self.jobs:
            job = next(
                (
                    j["id"]
                    for j in self.jobs.values()
                    if j["region"] == labels.get(self.label + "region")
                    and j["machine_type"] == labels.get(self.label + "machine")
                ),
                None,
            )
        rec = self.inst[x["name"]] = {
            "name": x["name"],
            "job": job,
            "zone": x["zone"].rsplit("/", 1)[1],
            "machine_type": x["machineType"].rsplit("/", 1)[1],
            "created": x.get("creationTimestamp"),
            "last_start": None,
            "first_seen": iso(now),
            "last_seen": None,
            "gone_at": None,
            "status_first_seen": {},
            "batch_job_id": labels.get("batch-job-id"),
        }
        self.note(
            kind="instance_seen",
            name=x["name"],
            job=job,
            zone=rec["zone"],
            status=x["status"],
            created=rec["created"],
        )
        return rec

    def poll_quota(self, now):
        row = {"t": iso(now)}
        for region in sorted({j["region"] for j in self.jobs.values()}):
            reg = self.api.call("GET", f"{COMPUTE}{self.args.project}/regions/{region}")
            for q in reg["quotas"]:
                if q["metric"] in ("CPUS", "C3_CPUS", "INSTANCES"):
                    row[f"{region}:{q['metric']}"] = q["usage"]
        for rec in self.inst.values():
            if not rec["gone_at"] and "RUNNING" in rec["status_first_seen"]:
                j = self.jobs.get(rec["job"]) or {}
                key = f"{j.get('region')}:{j.get('family')}_vcpus_running"
                row[key] = row.get(key, 0) + (j.get("vcpus_per_vm") or 0)
        self.quota_rows.append(row)

    def finalize(self, j, outcome):
        """Save the job and its tasks, then delete it. The delete runs even if a read fails."""
        j["finalized_at"], j["outcome"] = utcnow(), outcome
        state = None
        try:
            final = self.api.call("GET", BATCH + j["name"])
            self.save(f"{j['id']}.final.json", final)
            state = final.get("status", {}).get("state")
            url = BATCH + j["name"] + "/taskGroups/group0/tasks"
            self.save(f"{j['id']}.tasks.json", self.api.paged(url, "tasks", {"pageSize": 500}))
        except Exception as e:
            self.log(f"WARNING: reading {j['id']} before delete failed: {e}")
        finally:
            self.api.call("DELETE", BATCH + j["name"], ok404=True)
            j["deleted_at"] = utcnow()
        self.note(kind="job_finalized", job=j["id"], outcome=outcome, state=state)
        self.log(f"finalized {j['id']}: {outcome}; job deleted")

    def teardown_all(self, reason):
        """Delete every submitted job that isn't finalized yet. Used on a crash or Ctrl-C."""
        for j in self.jobs.values():
            if j["name"] and not j["finalized_at"]:
                try:
                    self.finalize(j, reason)
                except Exception as e:
                    self.log(f"ERROR: could not delete {j['id']}: {e}")

    def accrued(self, now):
        usd = 0.0
        for rec in self.inst.values():
            end = parse_ts(rec["gone_at"]) or now
            price = PRICE.get(rec["machine_type"], UNKNOWN_PRICE)
            if rec["last_start"]:
                hours = max(0.0, (end - parse_ts(rec["last_start"])).total_seconds()) / 3600
                usd += hours * price
            if rec["created"]:
                hours = max(0.0, (end - parse_ts(rec["created"])).total_seconds()) / 3600
                usd += hours * DISK_PER_HOUR
        return usd

    def loop(self):
        try:
            self._loop()
        except BaseException as e:
            self.log(f"ERROR: run stopped by {e!r}; deleting every open job")
            self.stop_reason = f"aborted: {e!r}"
            self.teardown_all("ABORTED")
            raise
        finally:
            self.write_outputs(utcnow())

    def _loop(self):
        self.submit_ready()
        last_quota = 0.0
        clean_polls = 0
        errors = 0
        while True:
            now = utcnow()
            try:
                self.poll_jobs(now)
                self.poll_instances(now)
                if time.time() - last_quota > 60:
                    self.poll_quota(now)
                    last_quota = time.time()
                errors = 0
            except RuntimeError as e:
                errors += 1
                self.log(f"WARNING: poll failed ({errors} in a row): {e}")
                if errors >= 6:
                    raise
                time.sleep(self.args.poll)
                continue
            for j in self.jobs.values():
                if j["submitted_at"] and not j["finalized_at"]:
                    if (now - j["submitted_at"]).total_seconds() > self.args.deadline_min * 60:
                        self.finalize(j, "DEADLINE")
            usd = self.accrued(now)
            live = sum(1 for r in self.inst.values() if not r["gone_at"])
            open_jobs = sum(
                1 for j in self.jobs.values() if j["submitted_at"] and not j["finalized_at"]
            )
            waiting = sum(1 for j in self.jobs.values() if not j["submitted_at"])
            self.log(
                f"[{iso(now)}] live VMs {live}, accrued about ${usd:.2f}, "
                f"jobs open {open_jobs}, waiting {waiting}"
            )
            if usd + self.args.prior_usd > self.args.cap_stop and not self.stop_reason:
                self.stop_reason = f"cost stop at ${usd + self.args.prior_usd:.2f}"
                self.log("STOP: " + self.stop_reason)
                for j in self.jobs.values():
                    if j["submitted_at"] and not j["finalized_at"]:
                        self.finalize(j, "COST_STOP")
            self.submit_ready()
            pending = [
                j
                for j in self.jobs.values()
                if not j["finalized_at"] and (j["submitted_at"] or not self.stop_reason)
            ]
            if not pending and live == 0:
                clean_polls += 1
                if clean_polls >= 2:
                    break
            else:
                clean_polls = 0
            if not pending and live:
                last_final = max(j["finalized_at"] for j in self.jobs.values() if j["finalized_at"])
                if (now - last_final).total_seconds() > 1200:
                    self.stop_reason = (self.stop_reason or "") + (
                        f"; {live} VMs still listed 20 min after the last job ended"
                    )
                    self.log("WARNING: " + self.stop_reason + " - check them by hand")
                    break
            time.sleep(self.args.poll)

    def write_outputs(self, now):
        rows = []
        for rec in sorted(self.inst.values(), key=lambda r: (r["job"] or "", r["created"] or "")):
            j = self.jobs.get(rec["job"]) or {}
            ct = j.get("create_time")
            created, started = parse_ts(rec["created"]), parse_ts(rec["last_start"])
            rows.append(
                {
                    "job": rec["job"],
                    "region": j.get("region"),
                    "zone": rec["zone"],
                    "machine_type": rec["machine_type"],
                    "instance": rec["name"],
                    "job_create_time": iso(ct),
                    "vm_created": rec["created"],
                    "vm_running": rec["last_start"] or "",
                    "submit_to_created_s": (
                        round((created - ct).total_seconds(), 1) if created and ct else ""
                    ),
                    "submit_to_running_s": (
                        round((started - ct).total_seconds(), 1) if started and ct else ""
                    ),
                    "first_seen_poll": rec["first_seen"],
                    "gone_poll": rec["gone_at"] or "",
                    "statuses_seen": " ".join(
                        f"{k}@{v}" for k, v in rec["status_first_seen"].items()
                    ),
                }
            )
        write_csv(os.path.join(self.out, "instances.csv"), rows)
        keys = sorted({k for r in self.quota_rows for k in r})
        with open(os.path.join(self.out, "quota_usage.csv"), "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=keys)
            w.writeheader()
            w.writerows(self.quota_rows)
        jobs_out = [
            {
                k: (iso(v) if isinstance(v, dt.datetime) else v)
                for k, v in j.items()
                if k not in ("body",)
            }
            for j in self.jobs.values()
        ]
        meta = {
            "project": self.args.project,
            "pass_id": self.args.pass_id,
            "prefix": self.args.prefix,
            "regions": self.args.regions,
            "started": iso(self.t0),
            "finished": iso(now),
            "image": self.args.image,
            "sleep_s": self.args.sleep,
            "deadline_min": self.args.deadline_min,
            "poll_s": self.args.poll,
            "stop_reason": self.stop_reason,
            "accrued_usd_estimate": round(self.accrued(now), 2),
            "jobs": jobs_out,
        }
        with open(os.path.join(self.out, "run.json"), "w") as f:
            json.dump(meta, f, indent=1, sort_keys=True)
        self.log(
            f"wrote {self.out}: {len(rows)} VMs, accrued about ${meta['accrued_usd_estimate']}"
        )


def write_csv(path, rows):
    if not rows:
        return
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def collect(args, log):
    """Pull every VM insert for this pass from the Compute Engine audit log."""
    meta = json.load(open(os.path.join(args.out, "run.json")))
    start = parse_ts(meta["started"]) - dt.timedelta(minutes=2)
    end = parse_ts(meta["finished"]) + dt.timedelta(minutes=2)
    project = meta["project"]
    flt = (
        f'logName="projects/{project}/logs/cloudaudit.googleapis.com%2Factivity" '
        'AND resource.type="gce_instance" AND protoPayload.methodName:"instances.insert" '
        f'AND protoPayload.resourceName:"{meta["prefix"]}-{meta["pass_id"]}-" '
        f'AND timestamp>="{iso(start)}" AND timestamp<="{iso(end)}"'
    )
    log(f"filter: {flt}")
    cmd = [
        "gcloud",
        "logging",
        "read",
        flt,
        f"--project={project}",
        "--format=json",
        "--limit=100000",
        "--order=asc",
    ]
    entries = json.loads(subprocess.check_output(cmd, text=True) or "[]")
    with open(os.path.join(args.out, "audit_inserts.json"), "w") as f:
        json.dump(entries, f, indent=1)
    rows = []
    for e in entries:
        pp = e.get("protoPayload", {})
        status = pp.get("status", {})
        info = {}
        for d in status.get("details", []):
            for ei in d.get("value", {}).get("errorInfo", []):
                info = {"reason": ei.get("reason", ""), **ei.get("metadata", {})}
        rows.append(
            {
                "timestamp": e.get("timestamp"),
                "severity": e.get("severity"),
                "instance": pp.get("resourceName", "").rsplit("/", 1)[-1],
                "zone": e.get("resource", {}).get("labels", {}).get("zone"),
                "first": e.get("operation", {}).get("first", False),
                "last": e.get("operation", {}).get("last", False),
                "status_code": status.get("code", ""),
                "status_message": status.get("message", ""),
                "error_reason": info.get("reason", ""),
                "error_vm_type": info.get("vmType", ""),
                "caller": pp.get("authenticationInfo", {}).get("principalEmail", ""),
            }
        )
    write_csv(os.path.join(args.out, "audit_inserts.csv"), rows)
    errors = [r for r in rows if r["status_code"] not in ("", 0)]
    log(f"{len(entries)} audit entries, {len(errors)} with an error status")
    tally = {}
    for r in errors:
        key = (
            r["error_vm_type"] or r["instance"],
            r["zone"],
            r["status_message"],
            r["error_reason"],
        )
        tally[key] = tally.get(key, 0) + 1
    with open(os.path.join(args.out, "audit_insert_errors_by_type.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["vm_type", "zone", "status_message", "reason", "count"])
        for key, n in sorted(tally.items()):
            w.writerow([*key, n])
            log(f"{n:>5} {key[0]:20} {key[1]:16} {key[2]} {key[3]}")


def cleanup(args, log):
    """Save and delete any job left from a pass, then wait until its VMs are gone."""
    api = Api(log)
    os.makedirs(os.path.join(args.out, "jobs"), exist_ok=True)
    for region in args.regions.split(","):
        url = f"{BATCH}projects/{args.project}/locations/{region}/jobs"
        for x in api.paged(url, "jobs", {"pageSize": 200}):
            job_id = x["name"].rsplit("/", 1)[1]
            if not job_id.startswith(f"{args.prefix}-{args.pass_id}-"):
                continue
            with open(os.path.join(args.out, "jobs", f"{job_id}.final.json"), "w") as f:
                json.dump(x, f, indent=1, sort_keys=True)
            try:
                tasks = api.paged(
                    BATCH + x["name"] + "/taskGroups/group0/tasks", "tasks", {"pageSize": 500}
                )
                with open(os.path.join(args.out, "jobs", f"{job_id}.tasks.json"), "w") as f:
                    json.dump(tasks, f, indent=1, sort_keys=True)
            finally:
                api.call("DELETE", BATCH + x["name"], ok404=True)
            log(f"{job_id}: {x.get('status', {}).get('state')}; saved and deleted")
    url = f"{COMPUTE}{args.project}/aggregated/instances"
    params = {
        "filter": f'labels.{args.prefix}_pass="{args.pass_id}"',
        "returnPartialSuccess": "true",
    }
    for _ in range(90):
        names = [
            i["name"]
            for page in api.paged(url, "items", params)
            for block in page.values()
            for i in block.get("instances", [])
        ]
        log(f"{len(names)} VMs with {args.prefix}_pass={args.pass_id}: {' '.join(names)}")
        if not names:
            return
        time.sleep(10)
    log("WARNING: VMs still listed after 15 minutes")


def pct(values, q):
    if not values:
        return ""
    values = sorted(values)
    k = (len(values) - 1) * q
    lo, hi = int(k), min(int(k) + 1, len(values) - 1)
    return round(values[lo] + (values[hi] - values[lo]) * (k - lo), 1)


def summarize(args, log):
    """Write results.csv, results.md, job_events.csv and tasks.csv from a finished pass."""
    meta = json.load(open(os.path.join(args.out, "run.json")))
    insts = list(csv.DictReader(open(os.path.join(args.out, "instances.csv"))))
    audit_path = os.path.join(args.out, "audit_inserts.csv")
    audit = list(csv.DictReader(open(audit_path))) if os.path.exists(audit_path) else []
    rows, event_rows, task_rows = [], [], []
    for j in meta["jobs"]:
        mine = [r for r in insts if r["job"] == j["id"]]
        run_s = [float(r["submit_to_running_s"]) for r in mine if r["submit_to_running_s"]]
        final_path = os.path.join(args.out, "jobs", f"{j['id']}.final.json")
        events = []
        if os.path.exists(final_path):
            events = json.load(open(final_path)).get("status", {}).get("statusEvents", [])
        for e in events:
            event_rows.append(
                {
                    "job": j["id"],
                    "event_time": e.get("eventTime"),
                    "type": e.get("type", ""),
                    "task_state": e.get("taskState", ""),
                    "description": e.get("description", ""),
                }
            )
        tasks_path = os.path.join(args.out, "jobs", f"{j['id']}.tasks.json")
        for t in json.load(open(tasks_path)) if os.path.exists(tasks_path) else []:
            first = {}
            for e in t.get("status", {}).get("statusEvents", []):
                first.setdefault(e.get("taskState", ""), e.get("eventTime"))
            task_rows.append(
                {
                    "job": j["id"],
                    "task": t.get("name", "").rsplit("/", 1)[-1],
                    "final_state": t.get("status", {}).get("state", ""),
                    "assigned": first.get("ASSIGNED", ""),
                    "running": first.get("RUNNING", ""),
                    "succeeded": first.get("SUCCEEDED", ""),
                    "failed": first.get("FAILED", ""),
                }
            )
        descriptions = [e.get("description", "") for e in events]
        exhausted = sum(1 for d in descriptions if "RESOURCE_POOL_EXHAUSTED" in d)
        quota = sum(1 for d in descriptions if "QUOTA" in d.upper())
        # Batch names each VM <job uid>-group0-<n>-<suffix>.
        prefix = j.get("uid")
        audit_err = [
            a
            for a in audit
            if prefix and a["instance"].startswith(prefix) and a["status_code"] not in ("", "0")
        ]
        target = j["vms"]
        rows.append(
            {
                "region": j["region"],
                "machine_type": j["machine_type"],
                "wave": j["wave"],
                "vms_target": target,
                "tasks_per_vm": j["tasks_per_vm"],
                "tasks": j["tasks"],
                "job_vcpus": j["job_vcpus"],
                "vms_running": len(run_s),
                "fill_pct": round(100 * len(run_s) / target, 1) if target else "",
                "first_running_s": pct(run_s, 0),
                "median_running_s": pct(run_s, 0.5),
                "p90_running_s": pct(run_s, 0.9),
                "last_running_s": pct(run_s, 1.0),
                "zones": " ".join(sorted({r["zone"] for r in mine if r["vm_running"]})),
                "outcome": j["outcome"],
                "batch_events_exhausted": exhausted,
                "batch_events_quota": quota,
                "audit_insert_errors": len(audit_err),
                "audit_error_zones": " ".join(sorted({a["zone"] for a in audit_err})),
            }
        )
    write_csv(os.path.join(args.out, "results.csv"), rows)
    write_csv(os.path.join(args.out, "job_events.csv"), event_rows)
    write_csv(os.path.join(args.out, "tasks.csv"), task_rows)
    lines = [
        "| Region | Machine type | VMs RUNNING / target | Tasks per VM "
        "| First / median / last VM RUNNING (s after submit) | Zones with VMs "
        "| VM insert errors (zones) | Batch outcome |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for r in rows:
        lines.append(
            f"| {r['region']} | `{r['machine_type']}` | {r['vms_running']} / {r['vms_target']} "
            f"| {r['tasks_per_vm']} | {r['first_running_s']} / {r['median_running_s']} / "
            f"{r['last_running_s']} | {r['zones']} | {r['audit_insert_errors']} "
            f"({r['audit_error_zones']}) | {r['outcome']} |"
        )
    with open(os.path.join(args.out, "results.md"), "w") as f:
        f.write("\n".join(lines) + "\n")
    log("\n".join(lines))


def default_project():
    try:
        out = subprocess.check_output(
            ["gcloud", "config", "get-value", "project"], text=True, stderr=subprocess.DEVNULL
        )
        return out.strip()
    except (OSError, subprocess.CalledProcessError):
        return ""


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("cmd", choices=["plan", "run", "collect", "summarize", "cleanup"])
    ap.add_argument("--project", default="")
    ap.add_argument("--network", default="epymodelingsuite-network")
    ap.add_argument("--subnet-template", default="epymodelingsuite-subnet-{region}")
    ap.add_argument(
        "--subnet",
        action="append",
        default=[],
        metavar="REGION=NAME",
        help="subnet name for one region, e.g. us-central1=epymodelingsuite-subnet",
    )
    ap.add_argument("--service-account", default="", help="default batch-runtime@<project>")
    ap.add_argument("--image", default=IMAGE)
    ap.add_argument("--prefix", default="sbt", help="job ID prefix and label key prefix")
    ap.add_argument("--pass-id", default="p1")
    ap.add_argument("--regions", default="us-east4,us-central1")
    ap.add_argument("--only", default="", help="comma-separated machine types to keep")
    ap.add_argument("--vms", type=int, default=0, help="override VMs per job")
    ap.add_argument("--sleep", type=int, default=600, help="task length in seconds")
    ap.add_argument("--deadline-min", type=float, default=30)
    ap.add_argument("--poll", type=float, default=10)
    ap.add_argument("--cap-stop", type=float, default=88.0, help="stop past this estimated USD")
    ap.add_argument("--prior-usd", type=float, default=0.0, help="spend already counted")
    ap.add_argument("--billed-minutes", type=float, default=15, help="for the plan estimate")
    ap.add_argument("--out", default="")
    args = ap.parse_args()
    args.project = args.project or default_project()
    if not args.project and args.cmd in ("plan", "run", "cleanup"):
        sys.exit("No project: pass --project or set the gcloud config project")
    args.service_account = (
        args.service_account or f"batch-runtime@{args.project}.iam.gserviceaccount.com"
    )

    logf = None
    if args.out and args.cmd == "run":
        os.makedirs(args.out, exist_ok=True)
        logf = open(os.path.join(args.out, "run.log"), "a")  # noqa: SIM115
    log_lock = threading.Lock()

    def log(msg):
        line = f"{dt.datetime.now(dt.UTC).strftime('%H:%M:%SZ')} {msg}"
        with log_lock:
            print(line, flush=True)
            if logf:
                logf.write(line + "\n")
                logf.flush()

    if args.cmd in ("plan", "run"):
        api = Api(log)
        plan = build_plan(api, args, log)
        print_plan(plan, args.billed_minutes, log)
        if args.cmd == "run":
            if not args.out:
                sys.exit("--out is required for run")
            with open(os.path.join(args.out, "plan.json"), "w") as f:
                json.dump(plan, f, indent=1, sort_keys=True)
            Run(api, args, plan, args.out, log).loop()
    elif args.cmd == "collect":
        collect(args, log)
    elif args.cmd == "cleanup":
        cleanup(args, log)
    else:
        summarize(args, log)


if __name__ == "__main__":
    main()
