import os
os.environ.setdefault("AWS_REGION", "ap-south-1")
os.environ.setdefault("PROJECT_PREFIX", "civic-index")
os.environ.setdefault("IMAGE_URI", "athithya5354/civic-indexing:latest")
os.environ.setdefault("SINGLE_NAT", "true")
import json
import pulumi
import pulumi_aws as aws

project_prefix = os.environ["PROJECT_PREFIX"]
aws_region = os.environ.get("AWS_REGION", "ap-south-1")
image_uri = os.environ.get("IMAGE_URI")
schedule_expression = os.environ.get("SCHEDULE", "cron(0 3 * * ? *)")
cpu = int(os.environ.get("CPU", "1024"))
memory = int(os.environ.get("MEMORY", "2048"))
container_name = os.environ.get("CONTAINER_NAME", "indexer")
env_vars = {}
env_json = os.environ.get("ENV_VARS", "{}")
try:
    env_vars = json.loads(env_json) if env_json else {}
except Exception:
    env_vars = {}
retain_buckets = os.environ.get("RETAIN_BUCKETS", "false").lower() in ("1", "true", "yes")
az_count = int(os.environ.get("AZ_COUNT", "2"))

single_nat = os.environ.get("SINGLE_NAT", "true").lower() in ("1", "true", "yes")
nat_env = os.environ.get("NAT_GATEWAYS")
if nat_env is not None and nat_env != "":
    nat_gateways_val = int(nat_env)
else:
    nat_gateways_val = 1 if single_nat else az_count

azs = aws.get_availability_zones(state="available").names[:az_count]

def make_bucket(suffix):
    name = f"{project_prefix}-{suffix}"
    b = aws.s3.Bucket(
        name,
        bucket=name,
        force_destroy=not retain_buckets,
        tags={"project": project_prefix, "component": suffix},
    )
    aws.s3.BucketVersioning(
        f"{name}-versioning",
        bucket=b.id,
        versioning_configuration={"status": "Enabled"},
        opts=pulumi.ResourceOptions(parent=b),
    )
    aws.s3.BucketServerSideEncryptionConfiguration(
        f"{name}-sse",
        bucket=b.id,
        rules=[{"apply_server_side_encryption_by_default": {"sse_algorithm": "AES256"}}],
        opts=pulumi.ResourceOptions(parent=b),
    )
    return b

raw_bucket = make_bucket("raw")
chunks_bucket = make_bucket("chunks")
meta_bucket = make_bucket("meta")

vpc = aws.ec2.Vpc(
    f"{project_prefix}-vpc",
    cidr_block="10.0.0.0/16",
    enable_dns_hostnames=True,
    enable_dns_support=True,
    tags={"Name": f"{project_prefix}-vpc", "project": project_prefix},
)

igw = aws.ec2.InternetGateway(
    f"{project_prefix}-igw",
    vpc_id=vpc.id,
    tags={"Name": f"{project_prefix}-igw", "project": project_prefix},
)

public_route_table = aws.ec2.RouteTable(
    f"{project_prefix}-public-rt",
    vpc_id=vpc.id,
    routes=[aws.ec2.RouteTableRouteArgs(cidr_block="0.0.0.0/0", gateway_id=igw.id)],
    tags={"Name": f"{project_prefix}-public-rt", "project": project_prefix},
)

public_subnets = []
private_subnets = []

for i, az in enumerate(azs):
    pub_cidr = f"10.0.{i}.0/24"
    priv_cidr = f"10.0.{i+100}.0/24"
    pub = aws.ec2.Subnet(
        f"{project_prefix}-public-{i}",
        vpc_id=vpc.id,
        cidr_block=pub_cidr,
        availability_zone=az,
        map_public_ip_on_launch=True,
        tags={"Name": f"{project_prefix}-public-{i}", "project": project_prefix},
    )
    public_subnets.append(pub)
    aws.ec2.RouteTableAssociation(
        f"{project_prefix}-public-rt-assoc-{i}",
        subnet_id=pub.id,
        route_table_id=public_route_table.id,
    )

    priv = aws.ec2.Subnet(
        f"{project_prefix}-private-{i}",
        vpc_id=vpc.id,
        cidr_block=priv_cidr,
        availability_zone=az,
        map_public_ip_on_launch=False,
        tags={"Name": f"{project_prefix}-private-{i}", "project": project_prefix},
    )
    private_subnets.append(priv)

nat_gateways = []
eips = []

for n in range(nat_gateways_val):
    target_pub_subnet = public_subnets[n if n < len(public_subnets) else 0]
    eip = aws.ec2.Eip(f"{project_prefix}-eip-{n}", domain="vpc", tags={"Name": f"{project_prefix}-eip-{n}", "project": project_prefix})
    eips.append(eip)
    nat = aws.ec2.NatGateway(
        f"{project_prefix}-natgw-{n}",
        subnet_id=target_pub_subnet.id,
        allocation_id=eip.allocation_id,
        tags={"Name": f"{project_prefix}-natgw-{n}", "project": project_prefix},
    )
    nat_gateways.append(nat)

private_rt_tables = []

for idx, priv in enumerate(private_subnets):
    rt = aws.ec2.RouteTable(
        f"{project_prefix}-private-rt-{idx}",
        vpc_id=vpc.id,
        tags={"Name": f"{project_prefix}-private-rt-{idx}", "project": project_prefix},
    )
    private_rt_tables.append(rt)
    chosen_nat = nat_gateways[0] if len(nat_gateways) == 1 else nat_gateways[idx % len(nat_gateways)]
    aws.ec2.Route(
        f"{project_prefix}-private-route-{idx}",
        route_table_id=rt.id,
        destination_cidr_block="0.0.0.0/0",
        nat_gateway_id=chosen_nat.id,
    )
    aws.ec2.RouteTableAssociation(
        f"{project_prefix}-private-rt-assoc-{idx}",
        subnet_id=priv.id,
        route_table_id=rt.id,
    )

private_subnet_ids = [s.id for s in private_subnets]
public_subnet_ids = [s.id for s in public_subnets]

task_sg = aws.ec2.SecurityGroup(
    f"{project_prefix}-task-sg",
    vpc_id=vpc.id,
    description="ECS task security group - outbound allowed",
    egress=[aws.ec2.SecurityGroupEgressArgs(protocol="-1", from_port=0, to_port=0, cidr_blocks=["0.0.0.0/0"])],
    ingress=[],
    tags={"project": project_prefix, "component": "ecs-task-sg"},
)

log_group = aws.cloudwatch.LogGroup(f"{project_prefix}-log", name=f"/{project_prefix}/indexer", retention_in_days=30)

ecr = aws.ecr.Repository(
    f"{project_prefix}-repo",
    image_scanning_configuration=aws.ecr.RepositoryImageScanningConfigurationArgs(scan_on_push=False),
    tags={"project": project_prefix},
)

execution_role = aws.iam.Role(
    f"{project_prefix}-task-exec-role",
    assume_role_policy=json.dumps({
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Principal": {"Service": "ecs-tasks.amazonaws.com"}, "Action": "sts:AssumeRole"}],
    }),
    tags={"project": project_prefix, "component": "task-exec-role"},
)
aws.iam.RolePolicyAttachment(f"{project_prefix}-exec-role-attach", role=execution_role.name, policy_arn="arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy")

task_role = aws.iam.Role(
    f"{project_prefix}-task-role",
    assume_role_policy=json.dumps({
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Principal": {"Service": "ecs-tasks.amazonaws.com"}, "Action": "sts:AssumeRole"}],
    }),
    tags={"project": project_prefix, "component": "task-role"},
)

bedrock_actions = ["bedrock:InvokeModel", "bedrock:InvokeModelWithResponseStream"]
task_policy = aws.iam.RolePolicy(
    f"{project_prefix}-task-policy",
    role=task_role.id,
    policy=pulumi.Output.all(raw_bucket.arn, chunks_bucket.arn, meta_bucket.arn).apply(
        lambda arns: json.dumps({
            "Version": "2012-10-17",
            "Statement": [
                {"Effect": "Allow", "Action": ["s3:ListBucket"], "Resource": [arns[0], arns[1], arns[2]]},
                {"Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"], "Resource": [f"{arns[0]}/*", f"{arns[1]}/*", f"{arns[2]}/*"]},
                {"Effect": "Allow", "Action": ["logs:CreateLogStream", "logs:PutLogEvents", "logs:CreateLogGroup"], "Resource": ["*"]},
                {"Effect": "Allow", "Action": bedrock_actions, "Resource": ["*"]},
            ],
        })
    ),
)

cluster = aws.ecs.Cluster(
    f"{project_prefix}-cluster",
    name=f"{project_prefix}-cluster",
    settings=[aws.ecs.ClusterSettingArgs(name="containerInsights", value="enabled")],
    tags={"project": project_prefix},
)

container_definitions = pulumi.Output.all(log_group.name, ecr.repository_url).apply(
    lambda args: json.dumps([{
        "name": container_name,
        "image": image_uri if image_uri else f"{args[1]}:latest",
        "cpu": cpu,
        "memory": memory,
        "essential": True,
        "environment": [{"name": k, "value": str(v)} for k, v in (env_vars or {}).items()],
        "logConfiguration": {"logDriver": "awslogs", "options": {"awslogs-group": args[0], "awslogs-region": aws_region, "awslogs-stream-prefix": project_prefix}},
    }])
)

task_def = aws.ecs.TaskDefinition(
    f"{project_prefix}-taskdef",
    family=f"{project_prefix}-taskdef",
    cpu=str(cpu),
    memory=str(memory),
    network_mode="awsvpc",
    requires_compatibilities=["FARGATE"],
    runtime_platform=aws.ecs.TaskDefinitionRuntimePlatformArgs(operating_system_family="LINUX"),
    container_definitions=container_definitions,
    task_role_arn=task_role.arn,
    execution_role_arn=execution_role.arn,
    tags={"project": project_prefix},
)

events_role = aws.iam.Role(
    f"{project_prefix}-events-role",
    assume_role_policy=json.dumps({
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Principal": {"Service": "events.amazonaws.com"}, "Action": "sts:AssumeRole"}],
    }),
    tags={"project": project_prefix, "component": "events-role"},
)

events_policy = aws.iam.RolePolicy(
    f"{project_prefix}-events-policy",
    role=events_role.id,
    policy=pulumi.Output.all(cluster.arn, task_def.arn, execution_role.arn, task_role.arn).apply(
        lambda vals: json.dumps({
            "Version": "2012-10-17",
            "Statement": [
                {"Effect": "Allow", "Action": ["ecs:RunTask"], "Resource": vals[1]},
                {"Effect": "Allow", "Action": ["ecs:RunTask"], "Resource": vals[0]},
                {"Effect": "Allow", "Action": ["iam:PassRole"], "Resource": [vals[2], vals[3]]},
            ],
        })
    ),
)

rule = aws.cloudwatch.EventRule(f"{project_prefix}-schedule", description="Schedule to run the indexing task", schedule_expression=schedule_expression)

target = aws.cloudwatch.EventTarget(
    f"{project_prefix}-target",
    rule=rule.name,
    arn=cluster.arn,
    role_arn=events_role.arn,
    ecs_target={
        "task_definition_arn": task_def.arn,
        "task_count": 1,
        "launch_type": "FARGATE",
        "network_configuration": {
            "subnets": private_subnet_ids,
            "security_groups": [task_sg.id],
            "assign_public_ip": False,
        },
    },
    opts=pulumi.ResourceOptions(depends_on=[events_policy, task_def, cluster, events_role]),
)

pulumi.export("project_prefix", project_prefix)
pulumi.export("region", aws_region)
pulumi.export("raw_bucket_name", raw_bucket.id)
pulumi.export("chunks_bucket_name", chunks_bucket.id)
pulumi.export("meta_bucket_name", meta_bucket.id)
pulumi.export("ecr_repo", ecr.repository_url)
pulumi.export("ecs_cluster", cluster.arn)
pulumi.export("task_definition_arn", task_def.arn)
pulumi.export("schedule_rule_name", rule.name)
pulumi.export("log_group", log_group.name)
