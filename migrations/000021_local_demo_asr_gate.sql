INSERT INTO runtime_gates(name, max_inflight) VALUES ('local_demo_asr', 2)
ON CONFLICT (name) DO NOTHING;
