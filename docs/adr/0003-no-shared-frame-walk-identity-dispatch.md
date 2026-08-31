# Do not share the identity-wiring dispatch between the two frame walks

`frame_analysis._process_frame` and `replay_frames._walk` each contain a
per-`updated_actor` block that wires the identity chain: `pri → link_car_to_pri`,
`uid → set_identity`, `paint → actor_team[aid] = team`. Extracting that ~7-line
block into a shared helper (a method on `IdentityResolver`, or a free function)
was considered and rejected. The two walks share only that block — phase 1
differs entirely (archetype-set membership vs. segment open / re-seed from
`initial_trajectory`) and so does phase 3 (handler notify + state purge vs.
resolve-identity-at-close + set `seg.end`); `is_playing`, `actor_position`, and
boost components exist in one walk only. The narrowest viable middle — an
`IdentityResolver` method like `wire_from_update(actor, pri_oid, uid_oid,
car_actors) -> team_paint | None` that handles `pri`/`uid` internally and hands
each caller the `paint` team to store in its own `actor_team` — is a real
option, deferred rather than dismissed: at two call sites and ~7 shared lines it
does not yet pay for the `car_actors` + object-ID parameters it must thread
through. The genuinely reusable parts are already extracted: `IdentityResolver`
(a shared class since the replay viewer was built) and, as of the `NetObj`
change, the object-name vocabulary in `rrrocket_schema.py`. Revisit
`wire_from_update` if a third frame walk appears, or if the dispatch block grows
past the identity chain.
