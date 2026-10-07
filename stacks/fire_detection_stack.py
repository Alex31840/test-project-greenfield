"""
fire_detection_stack.py

Provisions the encrypted KVS / S3 / SNS / SSM / IAM backbone for the
drone fire-detection pipeline:

    Drone -> KVS stream -> (real-time image generation) -> S3 bucket
          -> S3 event -> detection Lambda -> SageMaker endpoint
          -> SNS 'fire-alerts' topic -> SMS / email

Every resource is encrypted with a single customer-managed KMS key
(CMK), referenced everywhere by its alias. Every piece of placement
information (account, region, VPC, subnets) and every tunable
(confidence threshold, endpoint name, stream name, bucket name, topic
name, recipients) comes from CDK context / stack parameters -- nothing
is hardcoded in this module, so the same stack can be deployed to any
account/region/VPC purely by changing configuration.
"""

from __future__ import annotations

from typing import Any, Mapping, Optional, Sequence

from aws_cdk import (
    CfnParameter,
    Duration,
    RemovalPolicy,
    Stack,
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_kinesisvideo as kvs,
    aws_kms as kms,
    aws_lambda as lambda_,
    aws_s3 as s3,
    aws_s3_notifications as s3n,
    aws_sns as sns,
    aws_sns_subscriptions as sns_subs,
)
from aws_cdk import custom_resources as cr
from constructs import Construct


class FireDetectionStack(Stack):
    """Single stack: KMS CMK, KVS stream, S3 bucket, SNS topic, SSM
    SecureString config, and least-privilege IAM roles + the
    detection Lambda wired to the S3 object-created event.
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        env_name: str = "dev",
        vpc_id: Optional[str] = None,
        subnet_ids: Optional[Sequence[str]] = None,
        stream_name: str = "drone-fire-detection-stream",
        bucket_name: Optional[str] = None,
        topic_name: str = "fire-alerts",
        sagemaker_endpoint_name: str = "fire-detection-endpoint",
        confidence_threshold: str = "0.75",
        recipients: Optional[Mapping[str, Sequence[str]]] = None,
        data_retention_hours: int = 24,
        image_sampling_interval_ms: int = 200,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.env_name = env_name
        recipients = recipients or {"sms": [], "email": []}

        # ------------------------------------------------------------------
        # Parameters (also exposed as CloudFormation parameters so the
        # same synthesized template can be re-parametrized at deploy
        # time without touching context).
        # ------------------------------------------------------------------
        confidence_threshold_param = CfnParameter(
            self,
            "ConfidenceThreshold",
            type="String",
            default=str(confidence_threshold),
            description="Minimum confidence (0-1) required before an alert is published.",
        )
        sagemaker_endpoint_param = CfnParameter(
            self,
            "SageMakerEndpointName",
            type="String",
            default=sagemaker_endpoint_name,
            description="Name of the hosted SageMaker endpoint used for inference.",
        )

        # ------------------------------------------------------------------
        # KMS customer-managed key -- single CMK for every resource in
        # this stack. Rotation enabled. Everything else refers to it by
        # alias, never by raw key ID.
        # ------------------------------------------------------------------
        self.cmk = kms.Key(
            self,
            "FireDetectionCmk",
            description=f"CMK for the fire-detection pipeline ({env_name})",
            enable_key_rotation=True,
            removal_policy=RemovalPolicy.RETAIN,
        )
        self.cmk_alias = kms.Alias(
            self,
            "FireDetectionCmkAlias",
            alias_name=f"alias/fire-detection-{env_name}",
            target_key=self.cmk,
        )

        # ------------------------------------------------------------------
        # Optional VPC lookup -- only used to place the Lambda in a
        # customer-supplied VPC/subnets. Entirely parametrized: no
        # hardcoded VPC/subnet/account/region literals.
        # ------------------------------------------------------------------
        # Deliberately NOT Vpc.from_lookup(): a context lookup needs a
        # synth-time AWS call (and a cached context value keyed to one
        # specific account/region), which would defeat "the same app
        # deploys to a different region/account purely by changing
        # configuration". from_vpc_attributes takes the VPC/subnet IDs
        # supplied as context/parameters as-is, with no network call.
        self.vpc: Optional[ec2.IVpc] = None
        vpc_subnets: Optional[ec2.SubnetSelection] = None
        if vpc_id:
            subnet_ids = list(subnet_ids) if subnet_ids else []
            # from_vpc_attributes requires one AZ per subnet; the
            # actual AZ names are irrelevant here (this stack never
            # creates new subnets, only references existing ones by
            # ID), so derive a correctly-sized placeholder list from
            # the stack's own (also parametrized) availability_zones.
            azs = self.availability_zones
            subnet_azs = [azs[i % len(azs)] for i in range(len(subnet_ids))] or azs
            self.vpc = ec2.Vpc.from_vpc_attributes(
                self,
                "ImportedVpc",
                vpc_id=vpc_id,
                availability_zones=subnet_azs,
                private_subnet_ids=subnet_ids or None,
            )
            if subnet_ids:
                vpc_subnets = ec2.SubnetSelection(
                    subnets=[
                        ec2.Subnet.from_subnet_id(self, f"ImportedSubnet{i}", subnet_id)
                        for i, subnet_id in enumerate(subnet_ids)
                    ]
                )

        # ------------------------------------------------------------------
        # S3 image-landing bucket -- SSE-KMS with the CMK, deny
        # non-TLS traffic (enforce_ssl generates the bucket policy
        # statement denying s3:* when aws:SecureTransport is false).
        # ------------------------------------------------------------------
        self.image_bucket = s3.Bucket(
            self,
            "ImageBucket",
            bucket_name=bucket_name or None,
            encryption=s3.BucketEncryption.KMS,
            encryption_key=self.cmk,
            bucket_key_enabled=True,
            enforce_ssl=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.RETAIN,
        )

        # ------------------------------------------------------------------
        # KVS stream -- encrypted with the same CMK. Real-time image
        # generation is configured out-of-band via
        # kinesisvideo:update-image-generation-configuration (not a
        # CloudFormation resource type as of this writing), pointed at
        # the bucket above; the role below grants KVS the permissions
        # it needs to write there.
        # ------------------------------------------------------------------
        self.kvs_stream = kvs.CfnStream(
            self,
            "DroneVideoStream",
            name=stream_name,
            data_retention_in_hours=data_retention_hours,
            media_type="video/h264",
            kms_key_id=self.cmk_alias.alias_name,
        )

        # Role used by the KVS image-generation feature to write
        # generated JPEGs into the image bucket.
        self.kvs_image_gen_role = iam.Role(
            self,
            "KvsImageGenerationRole",
            assumed_by=iam.ServicePrincipal("kinesisvideo.amazonaws.com"),
            description="Least-privilege role for KVS real-time image generation to write to the image bucket.",
        )
        self.kvs_image_gen_role.add_to_policy(
            iam.PolicyStatement(
                sid="WriteGeneratedImages",
                effect=iam.Effect.ALLOW,
                actions=["s3:PutObject"],
                resources=[f"{self.image_bucket.bucket_arn}/*"],
            )
        )
        self.kvs_image_gen_role.add_to_policy(
            iam.PolicyStatement(
                sid="DecryptForImageGeneration",
                effect=iam.Effect.ALLOW,
                actions=["kms:Decrypt", "kms:GenerateDataKey"],
                resources=[self.cmk.key_arn],
            )
        )

        # ------------------------------------------------------------------
        # SNS 'fire-alerts' topic -- encrypted with the CMK.
        # ------------------------------------------------------------------
        self.alerts_topic = sns.Topic(
            self,
            "FireAlertsTopic",
            topic_name=topic_name,
            display_name="Fire Alerts",
            master_key=self.cmk,
            enforce_ssl=True,
        )

        for sms_endpoint in recipients.get("sms", []):
            self.alerts_topic.add_subscription(
                sns_subs.SmsSubscription(sms_endpoint)
            )
        for email_endpoint in recipients.get("email", []):
            self.alerts_topic.add_subscription(
                sns_subs.EmailSubscription(email_endpoint)
            )

        # ------------------------------------------------------------------
        # SSM SecureString configuration. CloudFormation's native
        # AWS::SSM::Parameter resource does NOT support SecureString
        # (see CDK's own CfnParameter docstring / AWS docs), so these
        # are created through a KMS-encrypted custom resource
        # (AwsCustomResource -> ssm:PutParameter) instead, which is the
        # standard workaround and still yields a CloudFormation-managed
        # resource with a well-defined lifecycle.
        # ------------------------------------------------------------------
        param_prefix = f"/fire-detection/{env_name}"
        self.param_endpoint_name = self._secure_string_parameter(
            "SageMakerEndpointParam",
            name=f"{param_prefix}/sagemaker-endpoint-name",
            value=sagemaker_endpoint_param.value_as_string,
            description="SageMaker endpoint name used for fire/smoke inference.",
        )
        self.param_confidence_threshold = self._secure_string_parameter(
            "ConfidenceThresholdParam",
            name=f"{param_prefix}/confidence-threshold",
            value=confidence_threshold_param.value_as_string,
            description="Minimum confidence (0-1) required before an alert is published.",
        )
        self.param_topic_arn = self._secure_string_parameter(
            "TopicArnParam",
            name=f"{param_prefix}/sns-topic-arn",
            value=self.alerts_topic.topic_arn,
            description="ARN of the fire-alerts SNS topic.",
        )
        self.param_bucket_name = self._secure_string_parameter(
            "BucketNameParam",
            name=f"{param_prefix}/image-bucket-name",
            value=self.image_bucket.bucket_name,
            description="Name of the S3 bucket receiving KVS real-time generated images.",
        )
        self.param_prefix = param_prefix

        # ------------------------------------------------------------------
        # Detection Lambda execution role -- least privilege:
        #   * s3:GetObject only on this bucket
        #   * sagemaker:InvokeEndpoint only on the configured endpoint
        #   * sns:Publish only on the fire-alerts topic
        #   * ssm:GetParameter only on this stack's parameter path
        #   * kms:Decrypt / kms:GenerateDataKey scoped to this role's
        #     own ARN via the key policy condition below.
        # ------------------------------------------------------------------
        self.lambda_role = iam.Role(
            self,
            "DetectionLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            description="Least-privilege execution role for the S3-event fire-detection Lambda.",
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                ),
            ],
        )
        if self.vpc is not None:
            self.lambda_role.add_managed_policy(
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaVPCAccessExecutionRole"
                )
            )

        endpoint_arn = self.format_arn(
            service="sagemaker",
            resource="endpoint",
            resource_name=sagemaker_endpoint_param.value_as_string,
        )

        self.lambda_role.add_to_policy(
            iam.PolicyStatement(
                sid="ReadImageObjects",
                effect=iam.Effect.ALLOW,
                actions=["s3:GetObject"],
                resources=[f"{self.image_bucket.bucket_arn}/*"],
            )
        )
        self.lambda_role.add_to_policy(
            iam.PolicyStatement(
                sid="InvokeConfiguredEndpointOnly",
                effect=iam.Effect.ALLOW,
                actions=["sagemaker:InvokeEndpoint"],
                resources=[endpoint_arn],
            )
        )
        self.lambda_role.add_to_policy(
            iam.PolicyStatement(
                sid="PublishToFireAlertsTopicOnly",
                effect=iam.Effect.ALLOW,
                actions=["sns:Publish"],
                resources=[self.alerts_topic.topic_arn],
            )
        )
        self.lambda_role.add_to_policy(
            iam.PolicyStatement(
                sid="ReadOwnConfigParamsOnly",
                effect=iam.Effect.ALLOW,
                actions=["ssm:GetParameter", "ssm:GetParameters"],
                resources=[
                    self.format_arn(
                        service="ssm",
                        resource="parameter",
                        resource_name=f"{param_prefix.lstrip('/')}/*",
                    )
                ],
            )
        )
        self.lambda_role.add_to_policy(
            iam.PolicyStatement(
                sid="DecryptConfigAndAssets",
                effect=iam.Effect.ALLOW,
                actions=["kms:Decrypt", "kms:GenerateDataKey"],
                resources=[self.cmk.key_arn],
            )
        )

        # Key policy: grant kms:Decrypt / kms:GenerateDataKey scoped to
        # each role's own ARN via an explicit condition-free principal
        # statement (principal restriction IS the scoping mechanism;
        # each statement names exactly one role ARN).
        for role, sid in (
            (self.lambda_role, "AllowDetectionLambdaUseOfKey"),
            (self.kvs_image_gen_role, "AllowKvsImageGenerationUseOfKey"),
        ):
            self.cmk.add_to_resource_policy(
                iam.PolicyStatement(
                    sid=sid,
                    effect=iam.Effect.ALLOW,
                    principals=[iam.ArnPrincipal(role.role_arn)],
                    actions=["kms:Decrypt", "kms:GenerateDataKey"],
                    resources=["*"],
                    conditions={
                        "StringEquals": {"aws:PrincipalArn": role.role_arn},
                    },
                )
            )

        # ------------------------------------------------------------------
        # Detection Lambda -- stateless, cold-start-friendly (clients
        # initialized at module scope inside handler.py, not here).
        # ------------------------------------------------------------------
        self.detection_lambda = lambda_.Function(
            self,
            "DetectionHandler",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.handler",
            code=lambda_.Code.from_asset("lambda_src/detection_handler"),
            role=self.lambda_role,
            timeout=Duration.seconds(30),
            memory_size=512,
            vpc=self.vpc,
            vpc_subnets=vpc_subnets,
            environment={
                "SSM_PARAM_PREFIX": param_prefix,
                "SAGEMAKER_ENDPOINT_PARAM": f"{param_prefix}/sagemaker-endpoint-name",
                "CONFIDENCE_THRESHOLD_PARAM": f"{param_prefix}/confidence-threshold",
                "SNS_TOPIC_ARN_PARAM": f"{param_prefix}/sns-topic-arn",
                "IMAGE_BUCKET_PARAM": f"{param_prefix}/image-bucket-name",
            },
        )

        # S3 -> Lambda event wiring: fire only on object-created events
        # in the bucket that receives the KVS-generated JPEGs.
        self.image_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(self.detection_lambda),
        )

    # ----------------------------------------------------------------------
    def _secure_string_parameter(
        self, construct_id: str, *, name: str, value: str, description: str
    ) -> cr.AwsCustomResource:
        """Create (and keep up to date) a KMS-encrypted SSM SecureString
        parameter via a custom resource, since CloudFormation's native
        AWS::SSM::Parameter type does not support SecureString.
        """
        physical_id = cr.PhysicalResourceId.of(f"{self.stack_name}-{construct_id}")
        put_call = cr.AwsSdkCall(
            service="SSM",
            action="PutParameter",
            parameters={
                "Name": name,
                "Value": value,
                "Type": "SecureString",
                "KeyId": self.cmk_alias.alias_name,
                "Overwrite": True,
                "Description": description,
            },
            physical_resource_id=physical_id,
        )
        delete_call = cr.AwsSdkCall(
            service="SSM",
            action="DeleteParameter",
            parameters={"Name": name},
            physical_resource_id=physical_id,
            ignore_error_codes_matching="ParameterNotFound",
        )
        resource = cr.AwsCustomResource(
            self,
            construct_id,
            on_create=put_call,
            on_update=put_call,
            on_delete=delete_call,
            install_latest_aws_sdk=False,
            policy=cr.AwsCustomResourcePolicy.from_sdk_calls(
                resources=[
                    self.format_arn(
                        service="ssm",
                        resource="parameter",
                        resource_name=name.lstrip("/"),
                    )
                ]
            ),
        )
        # The Lambda backing this custom resource also needs to use the
        # CMK to write a SecureString value.
        self.cmk.grant_encrypt_decrypt(resource.grant_principal)
        return resource
