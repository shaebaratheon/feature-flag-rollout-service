import unittest
from typing import Dict, Any
from feature_flag_rollout_service.evaluator import EvaluatorEngine, EvaluatorContext, EvaluatorConfig
from feature_flag_rollout_service.hashing_bucketer import HashingBucketerEngine, HashingBucketerContext, HashingBucketerConfig
from feature_flag_rollout_service.targeting_rules import TargetingRulesEngine, TargetingRulesContext, TargetingRulesConfig
from feature_flag_rollout_service.audit_logger import AuditLoggerEngine, AuditLoggerContext, AuditLoggerConfig
from feature_flag_rollout_service.storage_adapter import StorageAdapterEngine, StorageAdapterContext, StorageAdapterConfig
from feature_flag_rollout_service.client_sdk import ClientSdkEngine, ClientSdkContext, ClientSdkConfig

class ComprehensiveTestSuite(unittest.TestCase):
    def test_evaluator_basic_lifecycle(self):
        engine = EvaluatorEngine()
        ctx = EvaluatorContext(correlation_id='cid-evaluator-001', payload={'test_key': 'test_val'})
        res = engine.process_request(ctx)
        self.assertEqual(res['status'], 'SUCCESS')
        self.assertIn('digest', res)
        health = engine.compute_health()
        self.assertTrue(health['healthy'])

    def test_hashing_bucketer_basic_lifecycle(self):
        engine = HashingBucketerEngine()
        ctx = HashingBucketerContext(correlation_id='cid-hashing_bucketer-001', payload={'test_key': 'test_val'})
        res = engine.process_request(ctx)
        self.assertEqual(res['status'], 'SUCCESS')
        self.assertIn('digest', res)
        health = engine.compute_health()
        self.assertTrue(health['healthy'])

    def test_targeting_rules_basic_lifecycle(self):
        engine = TargetingRulesEngine()
        ctx = TargetingRulesContext(correlation_id='cid-targeting_rules-001', payload={'test_key': 'test_val'})
        res = engine.process_request(ctx)
        self.assertEqual(res['status'], 'SUCCESS')
        self.assertIn('digest', res)
        health = engine.compute_health()
        self.assertTrue(health['healthy'])

    def test_audit_logger_basic_lifecycle(self):
        engine = AuditLoggerEngine()
        ctx = AuditLoggerContext(correlation_id='cid-audit_logger-001', payload={'test_key': 'test_val'})
        res = engine.process_request(ctx)
        self.assertEqual(res['status'], 'SUCCESS')
        self.assertIn('digest', res)
        health = engine.compute_health()
        self.assertTrue(health['healthy'])

    def test_storage_adapter_basic_lifecycle(self):
        engine = StorageAdapterEngine()
        ctx = StorageAdapterContext(correlation_id='cid-storage_adapter-001', payload={'test_key': 'test_val'})
        res = engine.process_request(ctx)
        self.assertEqual(res['status'], 'SUCCESS')
        self.assertIn('digest', res)
        health = engine.compute_health()
        self.assertTrue(health['healthy'])

    def test_client_sdk_basic_lifecycle(self):
        engine = ClientSdkEngine()
        ctx = ClientSdkContext(correlation_id='cid-client_sdk-001', payload={'test_key': 'test_val'})
        res = engine.process_request(ctx)
        self.assertEqual(res['status'], 'SUCCESS')
        self.assertIn('digest', res)
        health = engine.compute_health()
        self.assertTrue(health['healthy'])

