import unittest

from data_exporter import get_connection_tags
from mappings import ipv4_in


class ExporterTestCase(unittest.TestCase):
    def test_tagging(self):
        tags = get_connection_tags(('140.82.112.1', '176.32.96.1'))
        self.assertIn('amazon', tags)
        self.assertIn('github', tags)
        tags = get_connection_tags(('172.217.0.1', '142.250.1.1'))
        self.assertIn('youtube', tags)
        self.assertIn('google', tags)
        tags = get_connection_tags(('31.13.64.1', '167.43.323.4'))
        self.assertEqual(['facebook'], tags)

    def test_ipv4_range(self):
        self.assertTrue(ipv4_in('31.13.64.2', ('31.13.0.0', '31.13.255.255')))
        self.assertFalse(ipv4_in('31.13.0.0', ('31.13.0.0', '31.13.255.255')))
        self.assertFalse(ipv4_in('31.14.0.0', ('31.13.0.0', '31.13.255.255')))


if __name__ == '__main__':
    unittest.main()
