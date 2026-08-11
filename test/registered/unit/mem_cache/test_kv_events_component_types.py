"""Unit tests for per-component KV placement events.

Covers the two halves of the ``component_types`` feature that have no other
guard: the ordering invariant ``UnifiedTreeCore`` relies on to snapshot a node
only after every component has committed, and the ``KVCacheEventMixin`` payload
rules (page skipping, parent chaining, and the legacy wire shape).
"""

import unittest

import msgspec
import torch

from sglang.srt.disaggregation.kv_events import (
    KV_COMPONENT_FULL,
    KV_COMPONENT_MAMBA,
    KV_COMPONENT_SWA,
    BlockStored,
    StorageMedium,
)
from sglang.srt.mem_cache.events import KVCacheEventMixin
from sglang.srt.mem_cache.unified_cache.cache_action import (
    FreeDeviceKV,
    MambaEvictExcessPathStates,
    RecoverSWAWithLockedFull,
    ReplaceWriteThroughOnNodeSplit,
    SWARebuild,
)
from sglang.srt.mem_cache.unified_cache.unified_tree_core import UnifiedTreeCore
from sglang.srt.mem_cache.utils import hash_str_to_int64
from sglang.test.ci.ci_register import register_cpu_ci

register_cpu_ci(est_time=2, suite="base-a-test-cpu")


def _page_hash_str(page_index):
    """A SHA256-shaped hex hash, distinct in the leading 16 chars.

    ``hash_str_to_int64`` folds only those, so the varying part has to be there.
    """
    return f"{page_index + 1:016x}" + "0" * 48


def _page_hash(page_index):
    """The int64 the event carries for ``page_index``."""
    return hash_str_to_int64(_page_hash_str(page_index))


class _FakeKey:
    """Stand-in for ``RadixKey``: the event path reads only these three things.

    Bigram views only change how token payloads are shaped, which the component
    dimension never touches, so these tests stay on the plain layout.
    """

    is_bigram = False

    def __init__(self, token_ids):
        self.token_ids = token_ids

    def __len__(self):
        return len(self.token_ids)


class _FakeNode:
    def __init__(self, num_pages, token_ids):
        self.hash_value = [_page_hash_str(i) for i in range(num_pages)]
        self.key = _FakeKey(token_ids)
        self.parent = None


class _LegacyRecorder(KVCacheEventMixin):
    """Drives the mixin with the least state ``_record_store_event`` reads.

    Deliberately does not override the component hook, so it exercises the base
    (whole-block) behaviour every non-unified cache still gets.
    """

    def __init__(self, page_size):
        self.enable_kv_cache_events = True
        self.page_size = page_size
        self.kv_event_queue = []
        self.root_node = object()


class _ComponentRecorder(_LegacyRecorder):
    def __init__(self, page_size, per_page):
        super().__init__(page_size)
        self._per_page = per_page
        # Every (page_index, num_pages) pair the hook was asked about.
        self.asked = []

    def _component_types_for_page(self, node, medium, page_index, num_pages):
        self.asked.append((page_index, num_pages))
        return self._per_page(page_index, num_pages)


class TestAuxCommitActionOrdering(unittest.TestCase):
    def test_aux_commit_actions_are_not_deferrable(self):
        """The COMMIT step must suspend before TAIL.

        ``UnifiedTreeCore`` defers a component-aware node's ``BlockStored`` to
        the TAIL step so the snapshot sees the node's final SWA/Mamba placement.
        That only holds because every aux action the COMMIT step emits is
        non-deferrable, which forces the orchestrator to apply it at a barrier
        before TAIL runs. Making one of them deferrable would silently publish
        FULL-only placements for blocks that do hold SWA or Mamba.
        """
        value = torch.empty(0)
        for action in (
            SWARebuild(1, value),
            RecoverSWAWithLockedFull(1, value, value),
            MambaEvictExcessPathStates(1),
        ):
            with self.subTest(action=type(action).__name__):
                self.assertFalse(UnifiedTreeCore._is_deferrable_action(action))

    def test_only_fire_and_forget_actions_are_deferrable(self):
        """Pins the deferrable set so widening it has to come past this test."""
        self.assertTrue(
            UnifiedTreeCore._is_deferrable_action(FreeDeviceKV([torch.empty(0)]))
        )
        self.assertTrue(
            UnifiedTreeCore._is_deferrable_action(
                ReplaceWriteThroughOnNodeSplit(
                    ack_id=1, old_node_id=1, new_node_id=2, new_child_node_id=3
                )
            )
        )


class TestStoreEventComponentTypes(unittest.TestCase):
    def test_legacy_pages_carry_no_component_dimension(self):
        recorder = _LegacyRecorder(page_size=2)
        recorder._record_store_event(_FakeNode(2, [10, 11, 12, 13]))

        events = recorder.take_events()
        self.assertEqual([event.component_types for event in events], [None, None])
        self.assertEqual(
            [event.block_hashes for event in events],
            [[_page_hash(0)], [_page_hash(1)]],
        )
        self.assertEqual(
            [event.parent_block_hash for event in events],
            [None, _page_hash(0)],
        )

    def test_reported_components_reach_every_page(self):
        recorder = _ComponentRecorder(
            page_size=2,
            per_page=lambda page_index, num_pages: [
                KV_COMPONENT_FULL,
                KV_COMPONENT_SWA,
            ],
        )
        recorder._record_store_event(_FakeNode(2, [10, 11, 12, 13]))

        self.assertEqual(
            [event.component_types for event in recorder.take_events()],
            [[KV_COMPONENT_FULL, KV_COMPONENT_SWA]] * 2,
        )

    def test_page_with_nothing_resident_is_skipped_but_keeps_the_chain(self):
        """A skipped page must still parent the page after it.

        Reporting an empty set means the tier holds nothing for that page, so
        claiming placement would be wrong. Dropping it from the parent chain
        would be worse: the next page would be published as a root block under a
        parent hash no other code path reproduces.
        """
        recorder = _ComponentRecorder(
            page_size=2,
            per_page=lambda page_index, num_pages: (
                [] if page_index == 1 else [KV_COMPONENT_FULL]
            ),
        )
        recorder._record_store_event(_FakeNode(3, list(range(6))))

        events = recorder.take_events()
        self.assertEqual(
            [event.block_hashes for event in events],
            [[_page_hash(0)], [_page_hash(2)]],
        )
        self.assertEqual(
            [event.parent_block_hash for event in events],
            [None, _page_hash(1)],
        )

    def test_last_page_hook_sees_the_real_page_count(self):
        """Mamba anchors to the final page, including a short trailing one."""
        recorder = _ComponentRecorder(
            page_size=4,
            per_page=lambda page_index, num_pages: (
                [KV_COMPONENT_MAMBA]
                if page_index == num_pages - 1
                else [KV_COMPONENT_FULL]
            ),
        )
        # 9 tokens over a 4-token page is 3 pages, the last one short.
        recorder._record_store_event(_FakeNode(3, list(range(9))))

        self.assertEqual(recorder.asked, [(0, 3), (1, 3), (2, 3)])
        self.assertEqual(
            [event.component_types for event in recorder.take_events()],
            [[KV_COMPONENT_FULL], [KV_COMPONENT_FULL], [KV_COMPONENT_MAMBA]],
        )

    def test_every_page_being_empty_emits_nothing(self):
        recorder = _ComponentRecorder(
            page_size=2, per_page=lambda page_index, num_pages: []
        )
        recorder._record_store_event(_FakeNode(2, [10, 11, 12, 13]))

        self.assertEqual(recorder.take_events(), [])


class TestBlockStoredWireShape(unittest.TestCase):
    """The trailing-slot contract the sgl-kv-indexer bridge decodes against."""

    def _encode(self, component_types):
        event = BlockStored(
            block_hashes=[1],
            parent_block_hash=None,
            token_ids=[7, 8],
            block_size=2,
            lora_id=None,
            medium=StorageMedium.GPU,
            component_types=component_types,
        )
        return msgspec.msgpack.decode(msgspec.msgpack.encode(event))

    def test_unset_component_types_encodes_as_a_trailing_nil(self):
        """Legacy subscribers must keep reading an absent optional.

        ``array_like`` always encodes every field, so the component slot is
        present-but-nil rather than dropped. That is what lets a positional
        decoder built for the 7-field schema stay unaffected.
        """
        decoded = self._encode(None)
        self.assertEqual(decoded[0], "BlockStored")
        self.assertEqual(len(decoded), 8)
        self.assertIsNone(decoded[7])

    def test_populated_component_types_occupy_the_same_slot(self):
        decoded = self._encode([KV_COMPONENT_FULL, KV_COMPONENT_SWA])
        self.assertEqual(len(decoded), 8)
        self.assertEqual(decoded[7], [KV_COMPONENT_FULL, KV_COMPONENT_SWA])


if __name__ == "__main__":
    unittest.main()
