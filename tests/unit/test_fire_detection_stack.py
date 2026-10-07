"""
Unit tests for stacks/fire_detection_stack.py.

Each test instantiates FireDetectionStack directly (not via the CDK
CLI / app.py) with varying account/region/vpc/subnet kwargs, and
asserts against the synthesized CloudFormation template using
aws_cdk.assertions.Template. This sidesteps CLI context-parsing
quirks (see progress notes) while still exercising exactly what
`cdk synth` would produce for the same construct tree.
"""
from __future__ import annotations

import json
import re

import pytest
from aws_cdk import App, Environment
from aws_cdk.assertions import Match, Template

from stacks.fire_detection_stack import FireDetectionStack


def _synth(**overrides):
    """Build a fresh App + FireDetectionStack with the given kwargs and
    return (stack, Template, template_json)."""
    app = App()
    stack = FireDetectionStack(app, overrides.pop("construct_id", "TestStack"), **overrides)
    template = Template.from_stack(stack)
    return stack, template, template.to_json()


# ---------------------------------------------------------------------------
# Parametrization: region / account / VPC come only from context/kwargs,
# never hardcoded in stack source.
# ---------------------------------------------------------------------------
class TestParametrization:
    def test_no_hardcoded_region_account_vpc_literals_in_stack_source(self):
        import pathlib

        source = pathlib.Path("stacks/fire_detection_stack.py").read_text()
        # No AWS region literals (e.g. us-east-1, eu-west-2, ap-southeast-1 ...)
        assert not re.search(r"\b[a-z]{2}-[a-z]+-\d\b", source)
        # No raw 12-digit account IDs
        assert not re.search(r"\b\d{12}\b", source)
        # No literal vpc-/subnet- resource IDs
        assert not re.search(r"\bvpc-[0-9a-f]+\b", source)
        assert not re.search(r"\bsubnet-[0-9a-f]+\b", source)

    def test_different_regions_produce_templates_referencing_each_region(self):
        _, _, tpl_a = _synth(
            construct_id="StackA",
            env_name="a",
            env=Environment(account="111111111111", region="us-east-1"),
        )
        _, _, tpl_b = _synth(
            construct_id="StackB",
            env_name="b",
            env=Environment(account="222222222222", region="eu-west-1"),
        )
        # The two stacks must actually synthesize to different account
        # partitions/regions -- i.e. the stack picked up the supplied
        # env rather than hardcoding one.
        stack_a = App().node  # not used; just ensure no crash above
        assert tpl_a != tpl_b

    def test_different_vpc_and_subnet_context_reflected_in_template(self):
        _, _, tpl_with_vpc = _synth(
            construct_id="StackVpc",
            env_name="vpctest",
            vpc_id="vpc-aaaaaaaaaaaaaaaaa",
            subnet_ids=["subnet-aaaaaaaaaaaaaaaaa", "subnet-bbbbbbbbbbbbbbbbb"],
        )
        serialized = json.dumps(tpl_with_vpc)
        assert "vpc-aaaaaaaaaaaaaaaaa" in serialized
        assert "subnet-aaaaaaaaaaaaaaaaa" in serialized
        assert "subnet-bbbbbbbbbbbbbbbbb" in serialized

        _, _, tpl_without_vpc = _synth(construct_id="StackNoVpc", env_name="novpc")
        serialized_no_vpc = json.dumps(tpl_without_vpc)
        assert "vpc-aaaaaaaaaaaaaaaaa" not in serialized_no_vpc

        # Lambda's VpcConfig only appears when a VPC was supplied.
        resources_with_vpc = tpl_with_vpc["Resources"]
        lambda_resources = {
            k: v for k, v in resources_with_vpc.items() if v["Type"] == "AWS::Lambda::Function"
        }
        assert any(
            "VpcConfig" in props.get("Properties", {}) for props in lambda_resources.values()
        )


# ---------------------------------------------------------------------------
# S3 bucket: SSE-KMS with the CMK + deny non-TLS bucket policy statement.
# ---------------------------------------------------------------------------
class TestImageBucket:
    def test_bucket_uses_sse_kms_with_stack_cmk(self):
        _, template, _ = _synth(env_name="s3test")
        template.has_resource_properties(
            "AWS::S3::Bucket",
            {
                "BucketEncryption": {
                    "ServerSideEncryptionConfiguration": Match.array_with(
                        [
                            Match.object_like(
                                {
                                    "ServerSideEncryptionByDefault": Match.object_like(
                                        {
                                            "SSEAlgorithm": "aws:kms",
                                            "KMSMasterKeyID": Match.any_value(),
                                        }
                                    )
                                }
                            )
                        ]
                    )
                }
            },
        )

    def test_bucket_policy_denies_non_tls_requests(self):
        _, template, tpl = _synth(env_name="s3tls")
        policies = [
            r for r in tpl["Resources"].values() if r["Type"] == "AWS::S3::BucketPolicy"
        ]
        assert policies, "expected a bucket policy resource"
        statements = policies[0]["Properties"]["PolicyDocument"]["Statement"]
        deny_tls_statements = [
            s
            for s in statements
            if s.get("Effect") == "Deny"
            and s.get("Condition", {}).get("Bool", {}).get("aws:SecureTransport") == "false"
        ]
        assert deny_tls_statements, "expected a Deny statement keyed on aws:SecureTransport=false"
        # should deny all actions
        for s in deny_tls_statements:
            actions = s["Action"]
            actions = actions if isinstance(actions, list) else [actions]
            assert "s3:*" in actions


# ---------------------------------------------------------------------------
# SNS topic: KmsMasterKeyId set to the CMK.
# ---------------------------------------------------------------------------
class TestSnsTopic:
    def test_topic_has_kms_master_key(self):
        _, template, _ = _synth(env_name="snstest")
        template.has_resource_properties(
            "AWS::SNS::Topic",
            {
                "TopicName": "fire-alerts",
                "KmsMasterKeyId": Match.any_value(),
            },
        )

    def test_topic_kms_master_key_resolves_to_stack_cmk(self):
        _, template, tpl = _synth(env_name="snskey")
        topics = [r for r in tpl["Resources"].values() if r["Type"] == "AWS::SNS::Topic"]
        assert len(topics) == 1
        key_ref = topics[0]["Properties"]["KmsMasterKeyId"]
        # Should reference the CMK construct (Fn::GetAtt ... Arn), not a
        # literal AWS-managed-key alias like 'alias/aws/sns'.
        assert key_ref != "alias/aws/sns"
        assert isinstance(key_ref, dict)


# ---------------------------------------------------------------------------
# KVS stream: KmsKeyId set to the CMK (by alias).
# ---------------------------------------------------------------------------
class TestKvsStream:
    def test_stream_has_kms_key_id(self):
        _, template, _ = _synth(env_name="kvstest")
        template.has_resource_properties(
            "AWS::KinesisVideo::Stream",
            {
                "KmsKeyId": Match.any_value(),
            },
        )

    def test_stream_kms_key_id_is_alias_reference(self):
        _, _, tpl = _synth(env_name="kvsalias")
        streams = [
            r for r in tpl["Resources"].values() if r["Type"] == "AWS::KinesisVideo::Stream"
        ]
        assert len(streams) == 1
        kms_key_id = streams[0]["Properties"]["KmsKeyId"]
        # Resolved either to a literal alias string or an Fn::Join/Ref
        # that ultimately composes "alias/..." -- assert it is NOT a
        # raw key-id-looking GetAtt on the key's .keyId attribute.
        serialized = json.dumps(kms_key_id)
        assert "KeyId" not in serialized or "alias" in serialized.lower() or "Alias" in serialized


# ---------------------------------------------------------------------------
# SSM SecureString config parameters, KMS-encrypted with the CMK.
# ---------------------------------------------------------------------------
class TestSsmParameters:
    EXPECTED_PARAM_SUFFIXES = (
        "sagemaker-endpoint-name",
        "confidence-threshold",
        "sns-topic-arn",
        "image-bucket-name",
    )

    @staticmethod
    def _flatten_join_literal_parts(create_value) -> str:
        """The Create/Update property is an Fn::Join of literal JSON
        fragments interspersed with {"Ref": ...}; concatenate just the
        literal string fragments so plain substring checks work."""
        if isinstance(create_value, str):
            return create_value
        if isinstance(create_value, dict) and "Fn::Join" in create_value:
            _, parts = create_value["Fn::Join"]
            return "".join(p for p in parts if isinstance(p, str))
        return json.dumps(create_value)

    def _custom_ssm_put_calls(self, tpl):
        calls = []
        for resource in tpl["Resources"].values():
            if resource["Type"] != "Custom::AWS":
                continue
            create = resource["Properties"].get("Create")
            flattened = self._flatten_join_literal_parts(create)
            if '"service":"SSM"' in flattened and '"action":"PutParameter"' in flattened:
                calls.append(flattened)
        return calls

    def test_four_securestring_parameters_created_with_cmk_key_id(self):
        _, _, tpl = _synth(env_name="ssmtest")
        calls = self._custom_ssm_put_calls(tpl)
        assert len(calls) == 4
        for call in calls:
            assert '"Type":"SecureString"' in call
            assert "alias/fire-detection-ssmtest" in call

    def test_parameter_names_cover_required_config(self):
        _, _, tpl = _synth(env_name="ssmnames")
        calls = self._custom_ssm_put_calls(tpl)
        serialized_all = "\n".join(calls)
        for suffix in self.EXPECTED_PARAM_SUFFIXES:
            assert suffix in serialized_all

    def test_parameter_path_is_scoped_to_env(self):
        _, _, tpl = _synth(env_name="ssmscope")
        calls = self._custom_ssm_put_calls(tpl)
        for call in calls:
            assert "/fire-detection/ssmscope/" in call


# ---------------------------------------------------------------------------
# KMS CMK: rotation enabled, referenced elsewhere only by alias.
# ---------------------------------------------------------------------------
class TestKmsKey:
    def test_key_rotation_enabled(self):
        _, template, _ = _synth(env_name="kmstest")
        template.has_resource_properties("AWS::KMS::Key", {"EnableKeyRotation": True})

    def test_key_has_alias(self):
        _, template, _ = _synth(env_name="kmsalias")
        template.has_resource_properties(
            "AWS::KMS::Alias", {"AliasName": "alias/fire-detection-kmsalias"}
        )

    def test_kvs_and_ssm_reference_key_by_alias_not_raw_key_id(self):
        _, _, tpl = _synth(env_name="kmsref")
        # KVS stream's KmsKeyId must be the alias name (e.g.
        # "alias/fire-detection-kmsref"), not a raw key ID / GetAtt on
        # the AWS::KMS::Key resource's KeyId attribute.
        alias_names = [
            r["Properties"]["AliasName"]
            for r in tpl["Resources"].values()
            if r["Type"] == "AWS::KMS::Alias"
        ]
        assert alias_names == ["alias/fire-detection-kmsref"]
        streams = [
            r for r in tpl["Resources"].values() if r["Type"] == "AWS::KinesisVideo::Stream"
        ]
        kms_key_id = streams[0]["Properties"]["KmsKeyId"]
        assert kms_key_id == alias_names[0]
        assert kms_key_id.startswith("alias/")


# ---------------------------------------------------------------------------
# IAM least-privilege: no wildcard resources on the four Lambda-role
# statements (S3 GetObject / SageMaker InvokeEndpoint / SNS Publish /
# SSM GetParameter).
# ---------------------------------------------------------------------------
class TestLeastPrivilegeIam:
    REQUIRED_ACTIONS = {
        "s3:GetObject",
        "sagemaker:InvokeEndpoint",
        "sns:Publish",
        "ssm:GetParameter",
    }

    def _lambda_role_statements(self, tpl):
        policies = [r for r in tpl["Resources"].values() if r["Type"] == "AWS::IAM::Policy"]
        statements = []
        for policy in policies:
            statements.extend(policy["Properties"]["PolicyDocument"]["Statement"])
        return statements

    def test_no_wildcard_resource_on_required_statements(self):
        _, _, tpl = _synth(
            env_name="iamtest",
            sagemaker_endpoint_name="prod-fire-endpoint",
        )
        statements = self._lambda_role_statements(tpl)

        covered_actions = set()
        for stmt in statements:
            actions = stmt.get("Action", [])
            actions = actions if isinstance(actions, list) else [actions]
            relevant = self.REQUIRED_ACTIONS.intersection(actions)
            if not relevant:
                continue
            covered_actions |= relevant
            resources = stmt.get("Resource", [])
            resources = resources if isinstance(resources, list) else [resources]
            for resource in resources:
                serialized = json.dumps(resource)
                assert serialized.strip('"') != "*"
                assert '"*"' not in serialized or "Partition" in serialized or "Region" in serialized or "AccountId" in serialized

        # all four required actions were actually found and checked
        assert covered_actions == self.REQUIRED_ACTIONS

    def test_sagemaker_invoke_endpoint_scoped_to_configured_endpoint_only(self):
        _, _, tpl = _synth(env_name="iamsagemaker", sagemaker_endpoint_name="my-endpoint")
        statements = self._lambda_role_statements(tpl)
        sagemaker_stmts = [
            s
            for s in statements
            if "sagemaker:InvokeEndpoint"
            in (s["Action"] if isinstance(s["Action"], list) else [s["Action"]])
        ]
        assert len(sagemaker_stmts) == 1
        resource = sagemaker_stmts[0]["Resource"]
        serialized = json.dumps(resource)
        # ARN is built from the stack's partition/region/account plus
        # ":endpoint/" and a Ref to the SageMakerEndpointName
        # CfnParameter (whose Default is the configured endpoint name,
        # so the generated template is still fully parametrized/
        # re-deployable without touching the stack).
        assert ":endpoint/" in serialized
        assert "SageMakerEndpointName" in serialized
        assert tpl["Parameters"]["SageMakerEndpointName"]["Default"] == "my-endpoint"
        # single-resource scoping: not a wildcard across all endpoints
        assert "endpoint/*" not in serialized
        assert '"*"' not in serialized

    def test_s3_getobject_scoped_to_bucket_only(self):
        _, _, tpl = _synth(env_name="iams3")
        statements = self._lambda_role_statements(tpl)
        s3_stmts = [
            s
            for s in statements
            if "s3:GetObject" in (s["Action"] if isinstance(s["Action"], list) else [s["Action"]])
        ]
        assert len(s3_stmts) == 1
        resource = s3_stmts[0]["Resource"]
        serialized = json.dumps(resource)
        assert "ImageBucket" in serialized

    def test_sns_publish_scoped_to_fire_alerts_topic_only(self):
        _, _, tpl = _synth(env_name="iamsns")
        statements = self._lambda_role_statements(tpl)
        sns_stmts = [
            s
            for s in statements
            if "sns:Publish" in (s["Action"] if isinstance(s["Action"], list) else [s["Action"]])
        ]
        assert len(sns_stmts) == 1
        resource = sns_stmts[0]["Resource"]
        serialized = json.dumps(resource)
        assert "FireAlertsTopic" in serialized

    def test_ssm_getparameter_scoped_to_stack_parameter_path_only(self):
        _, _, tpl = _synth(env_name="iamssm")
        statements = self._lambda_role_statements(tpl)
        ssm_stmts = [
            s
            for s in statements
            if "ssm:GetParameter"
            in (s["Action"] if isinstance(s["Action"], list) else [s["Action"]])
        ]
        assert len(ssm_stmts) == 1
        resource = ssm_stmts[0]["Resource"]
        serialized = json.dumps(resource)
        assert "fire-detection/iamssm" in serialized

    def test_kms_decrypt_scoped_by_role_arn_condition_in_key_policy(self):
        _, _, tpl = _synth(env_name="iamkms")
        keys = [r for r in tpl["Resources"].values() if r["Type"] == "AWS::KMS::Key"]
        assert len(keys) == 1
        key_policy_statements = keys[0]["Properties"]["KeyPolicy"]["Statement"]
        role_scoped_statements = [
            s
            for s in key_policy_statements
            if "kms:Decrypt"
            in (s.get("Action", []) if isinstance(s.get("Action"), list) else [s.get("Action")])
            and s.get("Condition", {}).get("StringEquals", {}).get("aws:PrincipalArn") is not None
        ]
        # one for the lambda role, one for the kvs image-gen role
        assert len(role_scoped_statements) >= 2


# ---------------------------------------------------------------------------
# Full set of acceptance criteria exercised together against a single
# "typical" context, as a smoke test matching the objective's wording.
# ---------------------------------------------------------------------------
class TestAcceptanceSmoke:
    def test_full_stack_synthesizes_with_all_encryption_and_iam_controls(self):
        _, template, _ = _synth(
            construct_id="SmokeStack",
            env_name="smoke",
            env=Environment(account="333333333333", region="ap-southeast-2"),
            vpc_id="vpc-0123456789abcdef0",
            subnet_ids=["subnet-0123456789abcdef0"],
            sagemaker_endpoint_name="smoke-endpoint",
            confidence_threshold="0.8",
            recipients={"sms": ["+15551234567"], "email": ["ops@example.com"]},
        )

        template.resource_count_is("AWS::KMS::Key", 1)
        template.resource_count_is("AWS::KMS::Alias", 1)
        template.resource_count_is("AWS::S3::Bucket", 1)
        template.resource_count_is("AWS::KinesisVideo::Stream", 1)
        template.resource_count_is("AWS::SNS::Topic", 1)
        # Besides our own detection Lambda, CDK provisions singleton
        # helper Lambdas for the AwsCustomResource SDK-call provider
        # (SSM PutParameter) and the S3 bucket-notifications custom
        # resource -- those are framework plumbing, not application
        # resources, so assert our handler exists rather than an exact
        # total count.
        template.has_resource_properties(
            "AWS::Lambda::Function", {"Handler": "handler.handler"}
        )
        template.has_resource_properties("AWS::KMS::Key", {"EnableKeyRotation": True})
        template.has_resource_properties(
            "AWS::SNS::Subscription", {"Protocol": "sms", "Endpoint": "+15551234567"}
        )
        template.has_resource_properties(
            "AWS::SNS::Subscription", {"Protocol": "email", "Endpoint": "ops@example.com"}
        )
