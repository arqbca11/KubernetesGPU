"""GPU worker for the shard-to-GPU index build scheduler.

The worker pulls; nothing pushes to it (invariant 2). Every write after the
claim is guarded by build_id and attempt (invariant 3). The build step sits
behind one interface, Builder.build(job) -> Artifact (invariant 9).
"""
