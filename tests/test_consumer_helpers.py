import asyncio
import gzip
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from consumer.bronze_consumer import (
    SparkRunner,
    build_dlq_payload,
    build_tombstone_payload,
    collect,
    dedup_key,
    ensure_pyspark_paths,
    event_etag,
    extract_event_time,
    handle_batch_failure,
    is_delete_event,
    is_late_event,
    is_live_stream_event,
    load_consumer_state,
    prune_dedup_keys,
    run_batch_persistent,
    s3_key_of,
    save_consumer_state,
    wanted,
)
from ml.stream_inference import match_future_observations
from tools.replay_dlq import build_replay_envelope, matches_filter
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

    def test_watermark_allowed_lateness_and_live_scope(self):
        wm = datetime(2026, 11, 22, 18, 0, 0, tzinfo=timezone.utc)
        on_time = wm - timedelta(minutes=5)
        too_late = wm - timedelta(minutes=25)
        self.assertFalse(is_late_event(on_time, wm, allowed_lateness_min=20))
        self.assertTrue(is_late_event(too_late, wm, allowed_lateness_min=20))
        self.assertFalse(is_late_event(too_late, None, allowed_lateness_min=20))

        # Historical 2020 & explicit replay/backfill bypass live watermark rejection
        self.assertTrue(is_live_stream_event("wmata", "bronze_2026/bus_positions/a.jsonl.gz", {"source": "seaweedfs"}))
        self.assertFalse(is_live_stream_event("wmata", "bronze_2020/bus_positions/a.jsonl.gz", {"source": "seaweedfs"}))
        self.assertFalse(is_live_stream_event("wmata", "bronze_2026/bus_positions/a.jsonl.gz", {"source": "replay"}))
        self.assertTrue(is_live_stream_event("traffic", "tomtom/flow/date=2026-11-22/f.jsonl.gz", {}))

    def test_delete_tombstone_detection_and_payload(self):
        ev_del = {
            "event_type": "delete",
            "s3_key": "bronze_2026/bus_positions/x.jsonl.gz",
            "etag": "e1",
            "event_time": "2026-11-22T18:00:00+00:00",
        }
        ev_old_only = {"message": {"old_entry": {"attributes": {"md5": "e2"}}}}
        ev_create = {"event_type": "create", "message": {"new_entry": {"attributes": {"md5": "e3"}}}}
        self.assertTrue(is_delete_event(ev_del))
        self.assertTrue(is_delete_event(ev_old_only))
        self.assertFalse(is_delete_event(ev_create))
        tomb = build_tombstone_payload("wmata", "bronze_2026/bus_positions/x.jsonl.gz", ev_del)
        self.assertEqual(tomb["bucket"], "wmata")
        self.assertEqual(tomb["etag"], "e1")
        self.assertEqual(extract_event_time(ev_del), datetime(2026, 11, 22, 18, 0, tzinfo=timezone.utc))

    def test_dedup_state_ttl_pruning_and_legacy_migration(self):
        now_ts = 1_800_000_000.0
        pruned = prune_dedup_keys(
            {"fresh": now_ts - 60, "stale": now_ts - (80 * 3600)},
            now_ts=now_ts,
            ttl_hours=72,
            max_keys=10,
        )
        self.assertIn("fresh", pruned)
        self.assertNotIn("stale", pruned)

        # Max-keys cap keeps newest entries
        many = {f"k{i}": now_ts - (100 - i) for i in range(10)}
        capped = prune_dedup_keys(many, now_ts=now_ts, ttl_hours=72, max_keys=3)
        self.assertEqual(set(capped.keys()), {"k7", "k8", "k9"})

        with tempfile.TemporaryDirectory() as tmpdir:
            st_path = f"{tmpdir}/state.json"
            with open(st_path, "w", encoding="utf-8") as fh:
                json.dump({"keys": ["wmata|a|e1", "wmata|b|e2"]}, fh)
            loaded = load_consumer_state(st_path)
            self.assertIn("wmata|a|e1", loaded["keys"])
            loaded["watermarks"]["wmata"] = "2026-11-22T18:00:00+00:00"
            save_consumer_state(loaded, st_path)
            reloaded = load_consumer_state(st_path)
            self.assertEqual(reloaded["watermarks"]["wmata"], "2026-11-22T18:00:00+00:00")

    def test_collect_routes_malformed_late_and_delete_events(self):
        async def _run():
            js = AsyncMock()
            malformed = MagicMock(data=b"{bad json", metadata=MagicMock(num_delivered=1))
            malformed.ack = AsyncMock()
            malformed.nak = AsyncMock()

            late_ev = {
                "bucket": "wmata",
                "s3_key": "bronze_2026/bus_positions/old.jsonl.gz",
                "etag": "etag_old",
                "event_type": "create",
                "event_time": "2026-11-22T17:00:00+00:00",
                "source": "seaweedfs",
            }
            late_msg = MagicMock(data=json.dumps(late_ev).encode(), metadata=MagicMock(num_delivered=1))
            late_msg.ack = AsyncMock()
            late_msg.nak = AsyncMock()

            del_ev = {
                "bucket": "wmata",
                "s3_key": "bronze_2026/bus_positions/del.jsonl.gz",
                "etag": "etag_del",
                "event_type": "delete",
                "event_time": "2026-11-22T18:05:00+00:00",
            }
            del_msg = MagicMock(data=json.dumps(del_ev).encode(), metadata=MagicMock(num_delivered=1))
            del_msg.ack = AsyncMock()
            del_msg.nak = AsyncMock()

            sub = MagicMock()
            sub.fetch = AsyncMock(side_effect=[[malformed, late_msg, del_msg], asyncio.TimeoutError()])
            batch = []
            with patch("consumer.bronze_consumer.vm_metrics", None):
                await collect(
                    js,
                    sub,
                    "wmata",
                    state_keys={},
                    batch=batch,
                    seen=set(),
                    watermarks={"wmata": "2026-11-22T18:00:00+00:00"},
                )
            self.assertEqual(len(batch), 0)
            malformed.ack.assert_awaited_once()
            late_msg.ack.assert_awaited_once()
            del_msg.ack.assert_awaited_once()
            published_subjects = [call.args[0] for call in js.publish.call_args_list]
            self.assertIn("wmata.dlq.malformed_json", published_subjects)
            self.assertIn("wmata.dlq.late_event", published_subjects)
            self.assertIn("realtime.tombstone.wmata", published_subjects)

        asyncio.run(_run())

    def test_poison_pill_routed_to_dlq_on_max_deliver(self):
        async def _run():
            js = AsyncMock()
            poison_msg = MagicMock()
            poison_msg.ack = AsyncMock()
            poison_msg.nak = AsyncMock()

            retry_msg = MagicMock()
            retry_msg.ack = AsyncMock()
            retry_msg.nak = AsyncMock()

            batch = [
                {"bucket": "wmata", "s3_key": "bronze_2026/bus_positions/bad.jsonl.gz", "delivery_count": 3, "event": {"s3_key": "bronze_2026/bus_positions/bad.jsonl.gz"}, "msg": poison_msg},
                {"bucket": "wmata", "s3_key": "bronze_2026/bus_positions/ok.jsonl.gz", "delivery_count": 1, "event": {"s3_key": "bronze_2026/bus_positions/ok.jsonl.gz"}, "msg": retry_msg},
            ]
            with patch("consumer.bronze_consumer.vm_metrics", None):
                dlq_n, retry_n = await handle_batch_failure(js, batch, "Spark exit 1", max_deliver=3, backoff=10)
            self.assertEqual((dlq_n, retry_n), (1, 1))
            poison_msg.ack.assert_awaited_once()
            poison_msg.nak.assert_not_awaited()
            retry_msg.nak.assert_awaited_once()
            retry_msg.ack.assert_not_awaited()
            self.assertEqual(js.publish.call_args_list[0].args[0], "wmata.dlq.max_deliver_exceeded")

        asyncio.run(_run())

    def test_dlq_replay_envelope_and_filter(self):
        dlq_rec = build_dlq_payload(
            bucket="wmata",
            reason="late_event",
            event={"bucket": "wmata", "s3_key": "bronze_2026/bus_positions/x.jsonl.gz", "etag": "e9"},
            delivery_count=1,
            error="too late",
        )
        self.assertTrue(matches_filter(dlq_rec, bucket="wmata", reason="late_event"))
        self.assertFalse(matches_filter(dlq_rec, bucket="traffic", reason="late_event"))
        subject, replay_ev = build_replay_envelope(dlq_rec)
        self.assertEqual(subject, "wmata.realtime.bronze_data")
        self.assertEqual(replay_ev["source"], "replay")
        self.assertEqual(replay_ev["s3_key"], "bronze_2026/bus_positions/x.jsonl.gz")

    def test_match_future_observations_15m_ground_truth(self):
        t0 = datetime(2026, 11, 22, 17, 0, 0, tzinfo=timezone.utc)
        preds = [
            {
                "input_record_id": "t1|ts0",
                "model_version": "v1",
                "trip_key": "TRIP-1",
                "snapshot_ts": t0,
                "predicted_delay_min": 4.0,
            },
            {
                "input_record_id": "t2|ts0",
                "model_version": "v1",
                "trip_key": "TRIP-2",
                "snapshot_ts": t0,
                "predicted_delay_min": 2.0,
            },
        ]
        snaps = [
            # Too early (+10m) — ignored
            {"trip_key": "TRIP-1", "snapshot_ts": t0 + timedelta(minutes=10), "deviation_min": 1.0},
            # Inside 14-16m window (+14.5m and +15.5m) — averaged -> (6.0 + 8.0)/2 = 7.0
            {"trip_key": "TRIP-1", "snapshot_ts": t0 + timedelta(minutes=14, seconds=30), "deviation_min": 6.0},
            {"trip_key": "TRIP-1", "snapshot_ts": t0 + timedelta(minutes=15, seconds=30), "deviation_min": 8.0},
            # Too late (+20m) — ignored
            {"trip_key": "TRIP-1", "snapshot_ts": t0 + timedelta(minutes=20), "deviation_min": 20.0},
            # TRIP-2 only has +5m obs — no match yet
            {"trip_key": "TRIP-2", "snapshot_ts": t0 + timedelta(minutes=5), "deviation_min": 2.5},
        ]
        matched = match_future_observations(preds, snaps)
        self.assertEqual(len(matched), 1)
        m = matched[0]
        self.assertEqual(m["trip_key"], "TRIP-1")
        self.assertAlmostEqual(m["actual_delay_min"], 7.0)
        self.assertAlmostEqual(m["abs_error"], 3.0)
        self.assertEqual(m["actual_late"], 1)

    def test_ensure_pyspark_paths_adds_spark_python_py4j_and_jobs_dir(self):
        import os
        from pathlib import Path

        with tempfile.TemporaryDirectory() as tmpdir:
            spark_home = Path(tmpdir) / "spark"
            py_lib = spark_home / "python" / "lib"
            py_lib.mkdir(parents=True)
            py4j_zip = py_lib / "py4j-0.10.9.9-src.zip"
            py4j_zip.write_bytes(b"PK")
            jobs_dir = spark_home / "jobs"
            jobs_dir.mkdir(parents=True)
            job_file = jobs_dir / "bronze_to_silver.py"
            job_file.write_text("# stub\n", encoding="utf-8")

            fake_sys_path = ["/usr/lib/python3"]
            fake_env: dict[str, str] = {}
            with (
                patch("consumer.bronze_consumer.sys.path", fake_sys_path),
                patch.dict(os.environ, fake_env, clear=True),
            ):
                added = ensure_pyspark_paths(
                    spark_home=str(spark_home),
                    spark_job=str(job_file),
                    master="spark://spark-master:7077",
                    driver_memory="1g",
                    cores_max="2",
                    configure_env=True,
                )
                self.assertIn(str(spark_home / "python"), fake_sys_path)
                self.assertIn(str(py4j_zip), fake_sys_path)
                self.assertIn(str(jobs_dir), fake_sys_path)
                self.assertEqual(len(added), 3)
                self.assertIn("--master spark://spark-master:7077", os.environ["PYSPARK_SUBMIT_ARGS"])
                self.assertIn("--driver-memory 1g", os.environ["PYSPARK_SUBMIT_ARGS"])
                self.assertIn("spark.cores.max=2", os.environ["PYSPARK_SUBMIT_ARGS"])
                self.assertTrue(os.environ["PYSPARK_SUBMIT_ARGS"].endswith("pyspark-shell"))

    def test_spark_runner_reuses_persistent_session_across_batches(self):
        def make_fake_spark(stopped: bool = False):
            spark = MagicMock()
            spark.sparkContext._jsc.sc.return_value.isStopped.return_value = stopped
            return spark

        fake_spark = make_fake_spark(stopped=False)
        factory = MagicMock(return_value=fake_spark)
        processor = MagicMock(return_value={"wmata_files": 1})

        runner = SparkRunner(session_factory=factory, manifest_processor=processor)
        res1 = runner.run_manifest({"wmata": [{"s3_key": "a.jsonl.gz"}], "traffic": []})
        res2 = runner.run_manifest({"wmata": [{"s3_key": "b.jsonl.gz"}], "traffic": []})

        self.assertEqual(res1["wmata_files"], 1)
        self.assertEqual(res2["wmata_files"], 1)
        self.assertEqual(factory.call_count, 1)
        self.assertEqual(processor.call_count, 2)
        self.assertIs(processor.call_args_list[0].args[0], fake_spark)
        self.assertIs(processor.call_args_list[1].args[0], fake_spark)

        runner.stop()
        fake_spark.stop.assert_called_once()
        self.assertFalse(runner.is_alive())

    def test_spark_runner_recovers_only_on_fatal_error_and_keeps_warm_on_transient(self):
        def make_fake_spark(stopped: bool = False):
            spark = MagicMock()
            spark.sparkContext._jsc.sc.return_value.isStopped.return_value = stopped
            return spark

        s1 = make_fake_spark(stopped=False)
        s2 = make_fake_spark(stopped=False)
        factory = MagicMock(side_effect=[s1, s2])
        processor = MagicMock(
            side_effect=[
                RuntimeError("Transient JDBC timeout"),
                RuntimeError("Cannot call methods on a stopped SparkContext"),
                {"wmata_files": 2},
            ]
        )
        runner = SparkRunner(session_factory=factory, manifest_processor=processor)

        # Transient error while SparkContext is alive keeps s1 warm
        with self.assertRaises(RuntimeError):
            runner.run_manifest({"wmata": [], "traffic": []})
        self.assertTrue(runner.is_alive())
        s1.stop.assert_not_called()

        # Fatal SparkContext error resets s1
        with self.assertRaises(RuntimeError):
            runner.run_manifest({"wmata": [], "traffic": []})
        self.assertFalse(runner.is_alive())
        s1.stop.assert_called_once()

        # Next batch automatically creates s2 and succeeds
        res = runner.run_manifest({"wmata": [{"s3_key": "c.jsonl.gz"}], "traffic": []})
        self.assertEqual(res["wmata_files"], 2)
        self.assertEqual(factory.call_count, 2)
        self.assertIs(processor.call_args_list[2].args[0], s2)

    def test_run_batch_persistent_executes_truncate_spark_merge_in_order(self):
        order: list[str] = []

        class FakeMerger:
            def truncate_all(self):
                order.append("truncate")
                return {"wmata": 2, "traffic": 5}

            def merge_all(self, run_id=None):
                order.append(f"merge:{run_id}")
                return {"wmata.bus_trip_snapshot": {"staged": 10, "merged": 10}}

        def fake_proc(spark, manifest):
            order.append(f"spark:{len(manifest['wmata'])}")
            return {"wmata_files": len(manifest["wmata"])}

        fake_spark = MagicMock()
        fake_spark.sparkContext._jsc.sc.return_value.isStopped.return_value = False
        runner = SparkRunner(
            session_factory=lambda: fake_spark,
            manifest_processor=fake_proc,
        )
        out = asyncio.run(
            run_batch_persistent(
                runner,
                {"wmata": [{"s3_key": "a.jsonl.gz"}], "traffic": []},
                run_id="run-42",
                merger=FakeMerger(),
            )
        )
        self.assertEqual(order, ["truncate", "spark:1", "merge:run-42"])
        self.assertEqual(out["spark"]["wmata_files"], 1)
        self.assertEqual(out["merge"]["wmata.bus_trip_snapshot"]["merged"], 10)


if __name__ == "__main__":
    unittest.main()
