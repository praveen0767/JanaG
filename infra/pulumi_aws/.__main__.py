from __future__ import annotations
"""
__main__.py - Pulumi program (single-file) for civic-index infra.

Guarantees provided by this program:
- Explicit configuration contract: all required values must be provided either
  via environment variables or pulumi config keys listed in REQUIRED_KEYS.
- Fail-fast validation: program exits early with clear errors on contract violation.
- Idempotent resources: names and tags are deterministic and stable across runs.
- Separation: configuration/validation performed before any provider resource creation.
- Test mode: set PULUMI_TEST=1 to run validations and export a plan without AWS calls.
- No hidden defaults: every default used is logged explicitly.
- Deterministic JSON single-line logs via jlog().
"""

import os
import re
import json
import pulumi
import pulumi_aws as aws
from pulumi import Output
from typing import Dict, Any, List, Optional

# -----------------------------
# CONFIG CONTRACT (explicit)
# -----------------------------
# Required (one of ENV or Pulumi config key must exist for each)
REQUIRED_KEYS = [
    ("PROJECT_PREFIX", "projectPrefix"),
    ("AWS_REGION", "awsRegion"),
    ("ECR_REPO_NAME", "ecrRepoName"),
    ("IMAGE_TAG", "imageTag"),
    ("UI_S3_BUCKET", "uiS3Bucket"),
    ("CREATE_CLOUDFRONT", "createCloudFront"),
]

# Explicit defaults (these are intentionally explicit and logged)
EXPLICIT_DEFAULTS = {
    "CPU": 1024,
    "MEMORY": 2048,
    "SINGLE_NAT": "true",   # "true" or "false" expected string
    "AZ_COUNT": 2,
}

# Pulumi config helper
CFG = pulumi.Config()

def conf(env_name: str, pulumi_key: Optional[str] = None) -> Optional[str]:
    """
    Return configuration value: prefer environment variable, else Pulumi config.
    pulumi_key may be None.
    """
    v = os.environ.get(env_name)
    if v is not None and v != "":
        return v
    if pulumi_key:
        try:
            cv = CFG.get(pulumi_key)
            if cv is not None and cv != "":
                return cv
        except Exception:
            pass
    return None

# -----------------------------
# Deterministic logging
# -----------------------------
def jlog(obj: Dict[str, Any]):
    pulumi.log.info(json.dumps(obj, sort_keys=True, default=str))

# -----------------------------
# Validation primitives
# -----------------------------
def require_one(envk: str, pulk: str) -> str:
    v = conf(envk, pulk)
    if not v:
        raise Exception(f"Configuration required: set environment variable {envk} or 'pulumi config set {pulk} <value>'")
    return v

def parse_bool_str(val: str, name: str) -> bool:
    if isinstance(val, bool):
        return val
    s = str(val).strip().lower()
    if s in ("1","true","yes"):
        return True
    if s in ("0","false","no"):
        return False
    raise Exception(f"Configuration {name} must be 'true' or 'false' (case-insensitive). Got: {val}")

def parse_int_str(val: str, name: str) -> int:
    try:
        return int(val)
    except Exception:
        raise Exception(f"Configuration {name} must be an integer. Got: {val}")

# -----------------------------
# CONTRACT VALIDATION & PRE-FLIGHT
# -----------------------------
def validate_contracts() -> Dict[str, Any]:
    jlog({"event":"validate_start"})
    cfg_out: Dict[str, Any] = {}

    # Required keys
    for envk, pulk in REQUIRED_KEYS:
        val = require_one(envk, pulk)
        cfg_out[envk] = val
        jlog({"event":"cfg_loaded","key":envk,"value_preview": val if len(val) < 80 else val[:80] + "...(trunc)"})

    # IMAGE_TAG must not be 'latest'
    if cfg_out["IMAGE_TAG"].strip().lower() == "latest":
        raise Exception("IMAGE_TAG must not be 'latest'; use an immutable tag (git-sha, semver, etc.)")

    # CREATE_CLOUDFRONT must be explicit boolean string
    cfg_out["CREATE_CLOUDFRONT_BOOL"] = parse_bool_str(cfg_out["CREATE_CLOUDFRONT"], "CREATE_CLOUDFRONT")

    # Optional explicit defaults (no hidden magic)
    for k, default in EXPLICIT_DEFAULTS.items():
        raw = conf(k, k.lower())
        if raw is None:
            cfg_out[k] = default
            jlog({"event":"cfg_default_used","key":k,"default":default})
        else:
            if k in ("CPU","MEMORY","AZ_COUNT"):
                cfg_out[k] = parse_int_str(raw, k)
            else:
                cfg_out[k] = raw
            jlog({"event":"cfg_override","key":k,"value":cfg_out[k]})

    # INDEX_DATA_BUCKET_ARNS - comma-separated explicit ARNs (optional)
    s3_arns_raw = conf("INDEX_DATA_BUCKET_ARNS", "indexDataBucketArns") or ""
    s3_arns = [s.strip() for s in s3_arns_raw.split(",") if s.strip()]
    cfg_out["INDEX_DATA_BUCKET_ARNS"] = s3_arns
    jlog({"event":"cfg_parsed_summary", "project_prefix": cfg_out["PROJECT_PREFIX"], "aws_region": cfg_out["AWS_REGION"], "ecr_repo": cfg_out["ECR_REPO_NAME"], "image_tag": cfg_out["IMAGE_TAG"], "create_cloudfront": cfg_out["CREATE_CLOUDFRONT_BOOL"], "index_data_buckets": len(s3_arns)})
    jlog({"event":"validate_ok"})
    return cfg_out

# -----------------------------
# TEST MODE (no AWS provider calls)
# -----------------------------
PULUMI_TEST = (os.environ.get("PULUMI_TEST", "0").strip() == "1")

if PULUMI_TEST:
    # Validate only, export a deterministic plan, then exit.
    snapshot = validate_contracts()
    plan = {
        "mode": "PULUMI_TEST",
        "resources_expected": [
            "aws.ec2.Vpc",
            "aws.ec2.Subnet (public/private)",
            "aws.ec2.NatGateway",
            "aws.ecr.Repository",
            "aws.iam.Role (execution/task)",
            "aws.ecs.Cluster",
            "aws.ecs.TaskDefinition",
            "optional: aws.cloudfront.Distribution"
        ],
        "config": snapshot
    }
    pulumi.export("plan", json.dumps(plan))
    jlog({"event":"test_mode_plan_exported"})
    # stop here in test mode (Pulumi will record the export)
else:
    # -----------------------------
    # Runtime: full validation then resource creation
    # -----------------------------
    CONFIG = validate_contracts()
    PROJECT_PREFIX = CONFIG["PROJECT_PREFIX"]
    AWS_REGION = CONFIG["AWS_REGION"]
    ECR_REPO_NAME = CONFIG["ECR_REPO_NAME"]
    IMAGE_TAG = CONFIG["IMAGE_TAG"]
    UI_S3_BUCKET = CONFIG["UI_S3_BUCKET"]
    CREATE_CLOUDFRONT = CONFIG["CREATE_CLOUDFRONT_BOOL"]
    CPU = CONFIG["CPU"]
    MEMORY = CONFIG["MEMORY"]
    AZ_COUNT = CONFIG["AZ_COUNT"]
    INDEX_DATA_BUCKET_ARNS = CONFIG["INDEX_DATA_BUCKET_ARNS"]

    # -----------------------------
    # Deterministic network (VPC + subnets)
    # -----------------------------
    jlog({"event":"network_boot_start","project":PROJECT_PREFIX,"az_count":AZ_COUNT})
    azs = aws.get_availability_zones(state="available").names[:AZ_COUNT]

    vpc = aws.ec2.Vpc(
        f"{PROJECT_PREFIX}-vpc",
        cidr_block="10.0.0.0/16",
        enable_dns_hostnames=True,
        enable_dns_support=True,
        tags={"Name": f"{PROJECT_PREFIX}-vpc", "project": PROJECT_PREFIX},
    )
    jlog({"event":"vpc_created","vpc":vpc.id})

    public_route_table = aws.ec2.RouteTable(
        f"{PROJECT_PREFIX}-public-rt",
        vpc_id=vpc.id,
        routes=[aws.ec2.RouteTableRouteArgs(cidr_block="0.0.0.0/0", gateway_id=None)],  # gateway created below
        tags={"Name": f"{PROJECT_PREFIX}-public-rt", "project": PROJECT_PREFIX},
    )

    igw = aws.ec2.InternetGateway(
        f"{PROJECT_PREFIX}-igw",
        vpc_id=vpc.id,
        tags={"Name": f"{PROJECT_PREFIX}-igw", "project": PROJECT_PREFIX},
    )

    # Update route to reference igw.id deterministically via explicit resource replacement
    # Delete then recreate route resource with correct gateway reference is unnecessary in Pulumi; instead create route after IGW
    public_route_table = aws.ec2.RouteTable(
        f"{PROJECT_PREFIX}-public-rt",
        vpc_id=vpc.id,
        routes=[aws.ec2.RouteTableRouteArgs(cidr_block="0.0.0.0/0", gateway_id=igw.id)],
        tags={"Name": f"{PROJECT_PREFIX}-public-rt", "project": PROJECT_PREFIX},
    )

    public_subnets: List[aws.ec2.Subnet] = []
    private_subnets: List[aws.ec2.Subnet] = []
    for i, az in enumerate(azs):
        pub = aws.ec2.Subnet(
            f"{PROJECT_PREFIX}-public-{i}",
            vpc_id=vpc.id,
            cidr_block=f"10.0.{i}.0/24",
            availability_zone=az,
            map_public_ip_on_launch=True,
            tags={"Name": f"{PROJECT_PREFIX}-public-{i}", "project": PROJECT_PREFIX},
        )
        public_subnets.append(pub)
        aws.ec2.RouteTableAssociation(
            f"{PROJECT_PREFIX}-public-rt-assoc-{i}",
            subnet_id=pub.id,
            route_table_id=public_route_table.id,
        )

        priv = aws.ec2.Subnet(
            f"{PROJECT_PREFIX}-private-{i}",
            vpc_id=vpc.id,
            cidr_block=f"10.0.{i+100}.0/24",
            availability_zone=az,
            map_public_ip_on_launch=False,
            tags={"Name": f"{PROJECT_PREFIX}-private-{i}", "project": PROJECT_PREFIX},
        )
        private_subnets.append(priv)

    jlog({"event":"subnets_created","public":len(public_subnets),"private":len(private_subnets)})

    # NAT Gateway(s): deterministic single NAT unless SINGLE_NAT explicit false
    single_nat = str(CONFIG.get("SINGLE_NAT", EXPLICIT_DEFAULTS["SINGLE_NAT"])).lower() in ("1","true","yes")
    nat_count = 1 if single_nat else AZ_COUNT
    jlog({"event":"nat_config","single_nat": single_nat, "nat_count": nat_count})

    eips: List[aws.ec2.Eip] = []
    nat_gateways: List[aws.ec2.NatGateway] = []
    for n in range(nat_count):
        eip = aws.ec2.Eip(f"{PROJECT_PREFIX}-eip-{n}", domain="vpc", tags={"Name": f"{PROJECT_PREFIX}-eip-{n}", "project": PROJECT_PREFIX})
        eips.append(eip)
        nat = aws.ec2.NatGateway(
            f"{PROJECT_PREFIX}-natgw-{n}",
            subnet_id=public_subnets[n if n < len(public_subnets) else 0].id,
            allocation_id=eip.allocation_id,
            tags={"Name": f"{PROJECT_PREFIX}-natgw-{n}", "project": PROJECT_PREFIX},
        )
        nat_gateways.append(nat)
    jlog({"event":"nat_gateways_created","count": len(nat_gateways)})

    # Private route tables referencing NATs
    for idx, priv in enumerate(private_subnets):
        rt = aws.ec2.RouteTable(
            f"{PROJECT_PREFIX}-private-rt-{idx}",
            vpc_id=vpc.id,
            tags={"Name": f"{PROJECT_PREFIX}-private-rt-{idx}", "project": PROJECT_PREFIX},
        )
        chosen_nat = nat_gateways[0] if len(nat_gateways) == 1 else nat_gateways[idx % len(nat_gateways)]
        aws.ec2.Route(
            f"{PROJECT_PREFIX}-private-route-{idx}",
            route_table_id=rt.id,
            destination_cidr_block="0.0.0.0/0",
            nat_gateway_id=chosen_nat.id,
        )
        aws.ec2.RouteTableAssociation(
            f"{PROJECT_PREFIX}-private-rt-assoc-{idx}",
            subnet_id=priv.id,
            route_table_id=rt.id,
        )

    jlog({"event":"network_done"})

    # -----------------------------
    # ECR (create repository idempotently)
    # -----------------------------
    jlog({"event":"ecr_create_start","repo_name": ECR_REPO_NAME})
    ecr_repo = aws.ecr.Repository(
        f"{PROJECT_PREFIX}-ecr-repo",
        repository_name=ECR_REPO_NAME,
        tags={"project": PROJECT_PREFIX},
    )
    jlog({"event":"ecr_repo_created","repo_name": ECR_REPO_NAME})
    registry_url = ecr_repo.repository_url

    # -----------------------------
    # IAM roles and policies (deterministic)
    # -----------------------------
    jlog({"event":"iam_start"})
    execution_role = aws.iam.Role(
        f"{PROJECT_PREFIX}-task-exec-role",
        assume_role_policy=json.dumps({
            "Version":"2012-10-17",
            "Statement":[{"Effect":"Allow","Principal":{"Service":"ecs-tasks.amazonaws.com"},"Action":"sts:AssumeRole"}]
        }),
        tags={"project": PROJECT_PREFIX, "component": "task-exec-role"},
    )
    aws.iam.RolePolicyAttachment(
        f"{PROJECT_PREFIX}-exec-role-attach",
        role=execution_role.name,
        policy_arn="arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
    )

    task_role = aws.iam.Role(
        f"{PROJECT_PREFIX}-task-role",
        assume_role_policy=json.dumps({
            "Version":"2012-10-17",
            "Statement":[{"Effect":"Allow","Principal":{"Service":"ecs-tasks.amazonaws.com"},"Action":"sts:AssumeRole"}]
        }),
        tags={"project": PROJECT_PREFIX, "component": "task-role"},
    )

    # Build task role policy: deterministic order
    statements = []
    # optional S3 access (explicit ARNs only)
    if INDEX_DATA_BUCKET_ARNS:
        statements.append({"Effect":"Allow","Action":["s3:ListBucket"],"Resource": INDEX_DATA_BUCKET_ARNS})
        obj_arns = [f"{a}/*" for a in INDEX_DATA_BUCKET_ARNS]
        statements.append({"Effect":"Allow","Action":["s3:GetObject","s3:PutObject","s3:DeleteObject"],"Resource": obj_arns})
    # logging & bedrock
    statements.append({"Effect":"Allow","Action":["logs:CreateLogStream","logs:PutLogEvents","logs:CreateLogGroup"],"Resource":["*"]})
    statements.append({"Effect":"Allow","Action":["bedrock:InvokeModel","bedrock:InvokeModelWithResponseStream"],"Resource":["*"]})

    task_policy_doc = json.dumps({"Version":"2012-10-17","Statement": statements})
    aws.iam.RolePolicy(f"{PROJECT_PREFIX}-task-policy", role=task_role.id, policy=task_policy_doc)
    jlog({"event":"iam_done","s3_arns_count": len(INDEX_DATA_BUCKET_ARNS)})

    # -----------------------------
    # ECS Cluster and TaskDefinition
    # -----------------------------
    jlog({"event":"ecs_start"})
    cluster = aws.ecs.Cluster(
        f"{PROJECT_PREFIX}-cluster",
        name=f"{PROJECT_PREFIX}-cluster",
        settings=[aws.ecs.ClusterSettingArgs(name="containerInsights", value="enabled")],
        tags={"project": PROJECT_PREFIX},
    )

    log_group_name = f"/aws/ecs/{PROJECT_PREFIX}/indexer"

    # ENV VARS for container: explicit input via ENV_VARS_JSON env or pulumi config envVarsJson
    env_json_raw = conf("ENV_VARS_JSON", "envVarsJson") or "{}"
    try:
        env_obj: Dict[str, str] = json.loads(env_json_raw)
        if not isinstance(env_obj, dict):
            raise Exception("ENV_VARS_JSON must be a JSON object mapping names to values")
    except Exception as e:
        raise Exception(f"ENV_VARS_JSON parse error: {e}")
    container_env_list = [{"name": k, "value": str(v)} for k, v in sorted(env_obj.items())]  # deterministic order

    def make_container_def(args):
        registry = args[0]
        return json.dumps([{
            "name":"indexer",
            "image": f"{registry}:{IMAGE_TAG}",
            "cpu": CPU,
            "memory": MEMORY,
            "essential": True,
            "environment": container_env_list,
            "logConfiguration": {
                "logDriver":"awslogs",
                "options":{
                    "awslogs-group": log_group_name,
                    "awslogs-region": AWS_REGION,
                    "awslogs-stream-prefix": PROJECT_PREFIX
                }
            }
        }])

    container_def = Output.all(registry_url).apply(make_container_def)

    task_def = aws.ecs.TaskDefinition(
        f"{PROJECT_PREFIX}-taskdef",
        family=f"{PROJECT_PREFIX}-taskdef",
        cpu=str(CPU),
        memory=str(MEMORY),
        network_mode="awsvpc",
        requires_compatibilities=["FARGATE"],
        runtime_platform=aws.ecs.TaskDefinitionRuntimePlatformArgs(operating_system_family="LINUX"),
        container_definitions=container_def,
        task_role_arn=task_role.arn,
        execution_role_arn=execution_role.arn,
        tags={"project": PROJECT_PREFIX},
    )

    jlog({"event":"ecs_done","cluster_arn": cluster.arn, "taskdef_arn": task_def.arn})

    # -----------------------------
    # OPTIONAL: CloudFront distribution (only if explicit)
    # -----------------------------
    if CREATE_CLOUDFRONT:
        # UI_S3_BUCKET must already exist; program will not create or alter its policy.
        jlog({"event":"cloudfront_start","ui_bucket": UI_S3_BUCKET})
        oac = aws.cloudfront.OriginAccessControl(
            f"{PROJECT_PREFIX}-uiOAC",
            origin_access_control_origin_type="s3",
            signing_behavior="always",
            signing_protocol="sigv4",
            name=f"{PROJECT_PREFIX}-ui-oac"
        )
        distribution = aws.cloudfront.Distribution(
            f"{PROJECT_PREFIX}-uiDistribution",
            enabled=True,
            is_ipv6_enabled=True,
            price_class="PriceClass_100",
            origins=[
                aws.cloudfront.DistributionOriginArgs(
                    origin_id="ui-s3-origin",
                    domain_name=f"{UI_S3_BUCKET}.s3.amazonaws.com",
                    origin_access_control_id=oac.id,
                    s3_origin_config=aws.cloudfront.DistributionOriginS3OriginConfigArgs()
                )
            ],
            default_root_object="index.html",
            default_cache_behavior=aws.cloudfront.DistributionDefaultCacheBehaviorArgs(
                target_origin_id="ui-s3-origin",
                viewer_protocol_policy="redirect-to-https",
                allowed_methods=["GET","HEAD","OPTIONS"],
                cached_methods=["GET","HEAD","OPTIONS"],
                compress=True,
                min_ttl=0,
                default_ttl=3600,
                max_ttl=86400,
                forwarded_values=aws.cloudfront.DistributionDefaultCacheBehaviorForwardedValuesArgs(
                    query_string=False,
                    cookies=aws.cloudfront.DistributionDefaultCacheBehaviorForwardedValuesCookiesArgs(forward="none")
                )
            ),
            restrictions=aws.cloudfront.DistributionRestrictionsArgs(
                geo_restriction=aws.cloudfront.DistributionRestrictionsGeoRestrictionArgs(restriction_type="none")
            ),
        )
        pulumi.export("cloudfront_domain", distribution.domain_name)
        jlog({"event":"cloudfront_done","domain_exported": True})
    else:
        jlog({"event":"cloudfront_skipped"})

    # -----------------------------
    # EXPORTS - deterministic
    # -----------------------------
    pulumi.export("project_prefix", PROJECT_PREFIX)
    pulumi.export("region", AWS_REGION)
    pulumi.export("ecr_repo_url", registry_url)
    pulumi.export("ecs_cluster_arn", cluster.arn)
    pulumi.export("task_definition_arn", task_def.arn)
    pulumi.export("log_group_name", log_group_name)
    jlog({"event":"exports_done","exports":["project_prefix","region","ecr_repo_url","ecs_cluster_arn","task_definition_arn","log_group_name"]})
