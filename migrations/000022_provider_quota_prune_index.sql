CREATE INDEX provider_call_reservations_prune_idx
    ON provider_call_reservations(created_at, id);
