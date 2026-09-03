# Do not share the identity-wiring dispatch between the two frame walks

`frame_analysis._process_frame` and `replay_frames._walk` each carry a
per-`updated_actor` block wiring the identity chain: `pri → link_car_to_pri`,
`uid → set_identity`, `paint → actor_team[aid] = team`. Extracting that ~7-line
block into a shared helper was considered and rejected: the two walks share
*only* that block — phase 1 (archetype-set membership vs. segment open / re-seed
from `initial_trajectory`) and phase 3 (handler notify + state purge vs.
resolve-identity-at-close + set `seg.end`) differ entirely, and `is_playing`,
`actor_position`, and boost components exist in one walk only. The narrowest
viable middle — an `IdentityResolver.wire_from_update(...)` that handles
`pri`/`uid` internally and returns each caller the `paint` team for its own
`actor_team` — is deferred, not dismissed: at two call sites and ~7 lines it
does not yet pay for the `car_actors` + object-ID parameters it must thread
through. The genuinely reusable parts are already shared: `IdentityResolver`
(a class since the replay viewer was built) and the `NetObj` object-name
vocabulary in `rrrocket_schema.py`. Revisit `wire_from_update` if a third frame
walk appears, or if the dispatch block grows past the identity chain.
