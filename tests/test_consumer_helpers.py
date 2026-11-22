import gzip
import json
import tempfile
import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from consumer.bronze_consumer import dedup_key, event_etag, s3_key_of, wanted
from dlt.poll_tomtom_traffic import (
    DEFAULT_CORRIDORS,
    _DETECTED_MODE_CACHE,
    build_move_route_analysis_payload,
    detect_api_mode,
    encode_jsonl_gz,
    make_bronze_key,
    normalize_flow_record,
    normalize_incident_records,
    normalize_move_trafficstats_report,
    run_poll_cycle,
)
from seaweedfs.bridge.app import enrich_envelope


class ConsumerHelpersTest(unittest.TestCase):
    def test_wanted_wmata(self):
        self.assertTrue(wanted("wmata", "bronze_2020/bus_positions/a.jsonl.gz"))
        self.assertTrue(wanted("wmata", "bronze_2026/bus_positions/b.jsonl.gz"))
        self.assertFalse(wanted("wmata", "sample.json"))

    def test_wanted_traffic(self):
        self.assertTrue(wanted("traffic", "ddot/taz/a.geojson"))
        self.assertFalse(wanted("traffic", "ddot/taz/a.json"))
        self.assertTrue(
            wanted("traffic", "tomtom/flow/date=2026-11-21/flow_20261121T120000Z_abc.jsonl.gz")
        )
        self.assertTrue(
            wanted(
                "traffic",
                "tomtom/incidents/date=2026-11-21/incidents_20261121T120000Z_def.jsonl.gz",
            )
        )
        self.assertFalse(wanted("traffic", "tomtom/flow/raw.json"))
        self.assertFalse(wanted("traffic", "tomtom/other/file.jsonl.gz"))

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

    def test_bridge_enrich_envelope(self):
        raw_event = {
            "key": "/buckets/traffic/tomtom/flow/date=2026-11-21/flow_1.jsonl.gz",
            "event_type": "create",
            "message": {
                "new_entry": {
                    "attributes": {"md5": "etag123", "mtime": 1795262400}
                }
            },
        }
        enriched = enrich_envelope(raw_event, "rawsha")
        self.assertEqual(enriched["bucket"], "traffic")
        self.assertEqual(
            enriched["s3_key"], "tomtom/flow/date=2026-11-21/flow_1.jsonl.gz"
        )
        self.assertEqual(enriched["etag"], "etag123")
        self.assertEqual(enriched["payload_version"], "v1")
        self.assertEqual(enriched["source"], "seaweedfs")
        self.assertTrue(enriched["event_id"])

    def test_tomtom_normalize_flow_record(self):
        snap = datetime(2026, 11, 21, 14, 30, 0, tzinfo=timezone.utc)
        corridor = DEFAULT_CORRIDORS[0]
        payload = {
            "flowSegmentData": {
                "frc": "FRC2",
                "currentSpeed": 24,
                "freeFlowSpeed": 40,
                "currentTravelTime": 150,
                "freeFlowTravelTime": 90,
                "confidence": 0.98,
                "roadClosure": False,
                "coordinates": {
                    "coordinate": [
                        {"latitude": 38.9160, "longitude": -77.0365},
                        {"latitude": 38.9180, "longitude": -77.0365},
                    ]
                },
            }
        }
        rec = normalize_flow_record(payload, corridor, snap, "tomtom/flow/test.jsonl.gz")
        self.assertEqual(rec["corridor_id"], corridor["corridor_id"])
        self.assertEqual(rec["current_speed_kmh"], 24.0)
        self.assertEqual(rec["free_flow_speed_kmh"], 40.0)
        self.assertAlmostEqual(rec["speed_ratio"], 0.6)
        self.assertEqual(rec["delay_sec"], 60)
        self.assertEqual(rec["service_date"], "2026-11-21")
        geom = json.loads(rec["geometry"])
        self.assertEqual(geom["type"], "LineString")
        self.assertEqual(len(geom["coordinates"]), 2)

    def test_tomtom_normalize_incident_records(self):
        snap = datetime(2026, 11, 21, 14, 30, 0, tzinfo=timezone.utc)
        payload = {
            "incidents": [
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "LineString",
                        "coordinates": [[-77.0365, 38.9025], [-77.0300, 38.9025]],
                    },
                    "properties": {
                        "id": "TT-INC-001",
                        "iconCategory": 6,
                        "magnitudeOfDelay": 3,
                        "delay": 420,
                        "length": 850.5,
                        "from": "17th St NW",
                        "to": "14th St NW",
                        "roadNumbers": ["US-29", "K St"],
                        "startTime": "2026-11-21T14:00:00Z",
                        "endTime": "2026-11-21T16:00:00Z",
                        "lastReportTime": "2026-11-21T14:25:00Z",
                        "probabilityOfOccurrence": "certain",
                        "numberOfReports": 5,
                        "events": [
                            {"description": "Stationary traffic", "code": 101, "iconCategory": 6}
                        ],
                    },
                }
            ]
        }
        recs = normalize_incident_records(payload, snap, "tomtom/incidents/test.jsonl.gz")
        self.assertEqual(len(recs), 1)
        inc = recs[0]
        self.assertEqual(inc["incident_id"], "TT-INC-001")
        self.assertEqual(inc["icon_category"], 6)
        self.assertEqual(inc["magnitude_of_delay"], 3)
        self.assertEqual(inc["delay_sec"], 420)
        self.assertEqual(inc["road_numbers"], "US-29,K St")
        self.assertEqual(inc["description"], "Stationary traffic")
        self.assertEqual(inc["event_code"], 101)
        self.assertAlmostEqual(inc["lat"], 38.9025)

    def test_tomtom_poll_cycle_and_quota_guard(self):
        mock_s3 = MagicMock()
        sample_corridors = [DEFAULT_CORRIDORS[0], DEFAULT_CORRIDORS[1]]
        sample_flow = {
            "flowSegmentData": {
                "frc": "FRC3",
                "currentSpeed": 30,
                "freeFlowSpeed": 40,
                "currentTravelTime": 120,
                "freeFlowTravelTime": 90,
                "confidence": 0.95,
                "roadClosure": False,
                "coordinates": {
                    "coordinate": [{"latitude": 38.91, "longitude": -77.03}]
                },
            }
        }
        sample_inc = {
            "incidents": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [-77.03, 38.91]},
                    "properties": {
                        "id": "INC-99",
                        "iconCategory": 1,
                        "magnitudeOfDelay": 2,
                        "delay": 180,
                        "events": [{"description": "Accident", "code": 201}],
                    },
                }
            ]
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            quota_file = f"{tmpdir}/quota.json"
            with (
                patch("dlt.poll_tomtom_traffic.fetch_flow_segment", return_value=sample_flow),
                patch("dlt.poll_tomtom_traffic.fetch_incidents", return_value=sample_inc),
                patch("dlt.poll_tomtom_traffic.push_metric", None),
                patch("dlt.poll_tomtom_traffic.inc_counter", None),
                patch("dlt.poll_tomtom_traffic.run_log", None),
            ):
                res = run_poll_cycle(
                    api_key="dummy-key",
                    bucket="traffic",
                    corridors=sample_corridors,
                    quota_path=quota_file,
                    max_daily_requests=5,
                    s3_client=mock_s3,
                )
                self.assertEqual(res["status"], "ok")
                self.assertEqual(res["flow_records"], 2)
                self.assertEqual(res["incident_records"], 1)
                self.assertEqual(res["requests_made"], 3)
                self.assertEqual(mock_s3.put_object.call_count, 2)

                # Verify uploaded payload is valid gzipped JSONL and wanted() accepts keys
                for call in mock_s3.put_object.call_args_list:
                    kwargs = call.kwargs
                    self.assertEqual(kwargs["Bucket"], "traffic")
                    self.assertTrue(wanted("traffic", kwargs["Key"]))
                    lines = gzip.decompress(kwargs["Body"]).decode("utf-8").strip().splitlines()
                    self.assertGreaterEqual(len(lines), 1)
                    parsed = json.loads(lines[0])
                    self.assertEqual(parsed["source_file"], kwargs["Key"])

                # Next cycle needs 3 requests (3 + 3 > 5) -> should throttle cleanly
                res2 = run_poll_cycle(
                    api_key="dummy-key",
                    bucket="traffic",
                    corridors=sample_corridors,
                    quota_path=quota_file,
                    max_daily_requests=5,
                    s3_client=mock_s3,
                )
                self.assertEqual(res2["status"], "quota_throttled")

    def test_tomtom_move_autodetect_and_report_ingest(self):
        import urllib.error

        _DETECTED_MODE_CACHE.clear()
        http_403 = urllib.error.HTTPError(
            url="https://api.tomtom.com/traffic/services/4/flowSegmentData",
            code=403,
            msg="Forbidden",
            hdrs=None,  # type: ignore[arg-type]
            fp=None,
        )
        sample_corridors = [DEFAULT_CORRIDORS[0], DEFAULT_CORRIDORS[1]]
        sample_maps = {
            "maps": [
                {"mapType": "GENESIS", "mapVersion": "2020.03"},
                {"mapType": "GENESIS", "mapVersion": "2026.09"},
            ]
        }
        sample_search = {"content": [{"jobId": 4242, "jobState": "DONE"}]}
        sample_status = {
            "jobId": 4242,
            "jobState": "DONE",
            "urls": ["https://api.tomtom.com/traffic/trafficstats/download/1/4242.json.gz"],
        }
        sample_move_report = {
            "routes": [
                {
                    "name": "dc_16th_st_nw",
                    "segmentResults": [
                        {
                            "frc": 2,
                            "speedLimit": 45,
                            "shape": [
                                {"latitude": 38.9250, "longitude": -77.0365},
                                {"latitude": 38.9080, "longitude": -77.0365},
                            ],
                            "segmentTimeResults": [
                                {
                                    "timeSet": 0,
                                    "harmonicAverageSpeed": 42.0,
                                    "averageTravelTime": 110.0,
                                    "sampleSize": 320,
                                },
                                {
                                    "timeSet": 1,
                                    "harmonicAverageSpeed": 26.0,
                                    "averageTravelTime": 175.0,
                                    "sampleSize": 580,
                                },
                            ],
                        }
                    ],
                }
            ]
        }

        payload_req = build_move_route_analysis_payload(
            sample_corridors, "2020-01-06", "2020-02-08"
        )
        self.assertEqual(len(payload_req["routes"]), 2)
        self.assertEqual(payload_req["routes"][0]["name"], "dc_16th_st_nw")

        mock_s3 = MagicMock()
        with tempfile.TemporaryDirectory() as tmpdir:
            move_state_file = f"{tmpdir}/move_state.json"
            with (
                patch("dlt.poll_tomtom_traffic.fetch_flow_segment", side_effect=http_403),
                patch("dlt.poll_tomtom_traffic.fetch_move_available_maps", return_value=sample_maps),
                patch("dlt.poll_tomtom_traffic.fetch_move_job_search", return_value=sample_search),
                patch("dlt.poll_tomtom_traffic.fetch_move_job_status", return_value=sample_status),
                patch("dlt.poll_tomtom_traffic._http_request_json", return_value=sample_move_report),
                patch("dlt.poll_tomtom_traffic.push_metric", None),
                patch("dlt.poll_tomtom_traffic.inc_counter", None),
                patch("dlt.poll_tomtom_traffic.run_log", None),
            ):
                detected = detect_api_mode("move-key-1", sample_corridors[0], "auto")
                self.assertEqual(detected, "move_trafficstats")

                res = run_poll_cycle(
                    api_key="move-key-1",
                    bucket="traffic",
                    corridors=sample_corridors,
                    api_mode="auto",
                    move_state_path=move_state_file,
                    s3_client=mock_s3,
                )
                self.assertEqual(res["api_mode"], "move_trafficstats")
                self.assertEqual(res["status"], "ok")
                self.assertEqual(res["flow_records"], 1)
                self.assertEqual(res["latest_map_version"], "2026.09")
                self.assertEqual(mock_s3.put_object.call_count, 1)

                uploaded_key = mock_s3.put_object.call_args.kwargs["Key"]
                self.assertTrue(wanted("traffic", uploaded_key))

                # Second poll cycle skips already-ingested jobId 4242
                res2 = run_poll_cycle(
                    api_key="move-key-1",
                    bucket="traffic",
                    corridors=sample_corridors,
                    api_mode="auto",
                    move_state_path=move_state_file,
                    s3_client=mock_s3,
                )
                self.assertEqual(res2["status"], "move_ready")
                self.assertEqual(res2["flow_records"], 0)


if __name__ == "__main__":
    unittest.main()
