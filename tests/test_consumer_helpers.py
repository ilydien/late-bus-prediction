import unittest

from consumer.bronze_consumer import dedup_key, event_etag, s3_key_of, wanted


class ConsumerHelpersTest(unittest.TestCase):
    def test_wanted_wmata(self):
        self.assertTrue(wanted("wmata", "bronze_2020/bus_positions/a.jsonl.gz"))
        self.assertTrue(wanted("wmata", "bronze_2026/bus_positions/b.jsonl.gz"))
        self.assertFalse(wanted("wmata", "sample.json"))

    def test_wanted_traffic(self):
        self.assertTrue(wanted("traffic", "ddot/taz/a.geojson"))
        self.assertFalse(wanted("traffic", "ddot/taz/a.json"))

    def test_s3_key_of_flat_event(self):
        event = {"s3_key": "bronze_2020/bus_positions/a.jsonl.gz"}
        self.assertEqual(s3_key_of("wmata", event), event["s3_key"])

    def test_s3_key_of_bridge_event(self):
        event = {"key": "/buckets/wmata/bronze_2020/bus_positions/a.jsonl.gz"}
        self.assertEqual(
            s3_key_of("wmata", event), "bronze_2020/bus_positions/a.jsonl.gz"
        )

    def test_event_etag_flat_and_bridge(self):
        self.assertEqual(event_etag({"etag": "abc"}), "abc")
        bridge = {
            "message": {
                "new_entry": {"attributes": {"md5": "def"}},
            }
        }
        self.assertEqual(event_etag(bridge), "def")

    def test_dedup_key(self):
        self.assertEqual(
            dedup_key("wmata", "k", {"etag": "e"}),
            "wmata|k|e",
        )


if __name__ == "__main__":
    unittest.main()
