# stdlib
import unittest

# 3p

# project
from utils.service_discovery.sd_backend import ServiceDiscoveryBackend


class TestServiceDiscovery(unittest.testCase):
    def setUp(self):
        self.etcd_agentConfig = {
            'service_discovery_backend': 'docker'
        }
        self.consul_agentConfig = {

        }
        self.auto_conf_agentConfig = {

        }
