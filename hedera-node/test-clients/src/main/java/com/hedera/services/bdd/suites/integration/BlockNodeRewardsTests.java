// SPDX-License-Identifier: Apache-2.0
package com.hedera.services.bdd.suites.integration;

import static com.hedera.hapi.util.HapiUtils.asInstant;
import static com.hedera.node.app.hapi.utils.CommonPbjConverters.toPbj;
import static com.hedera.node.app.service.token.impl.schemas.V0610TokenSchema.NODE_REWARDS_STATE_ID;
import static com.hedera.services.bdd.junit.RepeatableReason.NEEDS_VIRTUAL_TIME_FOR_FAST_EXECUTION;
import static com.hedera.services.bdd.junit.TestTags.INTEGRATION;
import static com.hedera.services.bdd.junit.hedera.embedded.EmbeddedMode.REPEATABLE;
import static com.hedera.services.bdd.spec.HapiSpec.hapiTest;
import static com.hedera.services.bdd.spec.queries.QueryVerbs.getAccountBalance;
import static com.hedera.services.bdd.spec.transactions.TxnVerbs.cryptoCreate;
import static com.hedera.services.bdd.spec.transactions.TxnVerbs.cryptoTransfer;
import static com.hedera.services.bdd.spec.transactions.TxnVerbs.nodeUpdate;
import static com.hedera.services.bdd.spec.transactions.TxnVerbs.registeredNodeCreate;
import static com.hedera.services.bdd.spec.transactions.TxnVerbs.registeredNodeDelete;
import static com.hedera.services.bdd.spec.utilops.CustomSpecAssert.allRunFor;
import static com.hedera.services.bdd.spec.utilops.EmbeddedVerbs.mutateSingleton;
import static com.hedera.services.bdd.spec.utilops.UtilVerbs.doingContextual;
import static com.hedera.services.bdd.spec.utilops.UtilVerbs.overriding;
import static com.hedera.services.bdd.spec.utilops.UtilVerbs.recordStreamMustIncludePassWithoutBackgroundTrafficFrom;
import static com.hedera.services.bdd.spec.utilops.UtilVerbs.selectedItems;
import static com.hedera.services.bdd.spec.utilops.UtilVerbs.sleepForBlockPeriod;
import static com.hedera.services.bdd.spec.utilops.UtilVerbs.waitUntilStartOfNextStakingPeriod;
import static com.hedera.services.bdd.spec.utilops.UtilVerbs.withOpContext;
import static com.hedera.services.bdd.spec.utilops.streams.assertions.SelectedItemsAssertion.SELECTED_ITEMS_KEY;
import static com.hedera.services.bdd.suites.HapiSuite.GENESIS;
import static com.hedera.services.bdd.suites.HapiSuite.NODE_REWARD;
import static com.hedera.services.bdd.suites.HapiSuite.ONE_MILLION_HBARS;
import static com.hedera.services.bdd.suites.HapiSuite.TINY_PARTS_PER_WHOLE;
import static com.hederahashgraph.api.proto.java.HederaFunctionality.CryptoTransfer;
import static java.util.stream.Collectors.toMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.hedera.hapi.node.state.token.NodeActivity;
import com.hedera.hapi.node.state.token.NodeRewards;
import com.hedera.node.app.service.token.TokenService;
import com.hedera.services.bdd.junit.HapiTestLifecycle;
import com.hedera.services.bdd.junit.LeakyRepeatableHapiTest;
import com.hedera.services.bdd.junit.RepeatableHapiTest;
import com.hedera.services.bdd.junit.TargetEmbeddedMode;
import com.hedera.services.bdd.junit.support.TestLifecycle;
import com.hedera.services.bdd.spec.SpecOperation;
import com.hedera.services.bdd.spec.transactions.TxnUtils;
import com.hedera.services.bdd.spec.transactions.token.TokenMovement;
import com.hedera.services.bdd.spec.utilops.EmbeddedVerbs;
import com.hedera.services.bdd.spec.utilops.streams.assertions.VisibleItemsValidator;
import com.hederahashgraph.api.proto.java.AccountAmount;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * HapiTests for HIP-1357 block node reward distribution.
 *
 * <p>Covers the full eligibility matrix:
 * <ul>
 *   <li>Inactive node, no registered block node</li>
 *   <li>Inactive node, one registered block node</li>
 *   <li>Inactive node, multiple registered block nodes</li>
 *   <li>Active node, no registered block node</li>
 *   <li>Active node, one registered block node</li>
 *   <li>Active node, multiple registered block nodes</li>
 * </ul>
 *
 * <p>Only node 2 participates in rewards (nodes 0, 1, 3 decline rewards), so all reward
 * assertions are isolated to a single node and its account (0.0.5).
 */
@Order(7)
@Tag(INTEGRATION)
@HapiTestLifecycle
@TargetEmbeddedMode(REPEATABLE)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class BlockNodeRewardsTests {

    private static final Logger log = LogManager.getLogger(BlockNodeRewardsTests.class);

    /** Account number of node 2 in the embedded test environment. */
    private static final long NODE_2_ACCOUNT_NUM = 5L;

    /** Account number of the node reward account (0.0.801). */
    private static final long NODE_REWARD_ACCOUNT_NUM = 801L;

    /**
     * Yearly block node reward in USD used in these tests.
     * With {@code numPeriodsToTargetUsd=365} these yields a per-period reward of 100 USD.
     */
    private static final long BLOCK_NODE_YEARLY_REWARD_USD = 36500L;

    /** Minimum per-period reward in USD applied to inactive nodes in scenarios 1–3. */
    private static final long MIN_INACTIVE_REWARD_USD = 10L;

    @BeforeAll
    static void beforeAll(@NonNull final TestLifecycle testLifecycle) {
        testLifecycle.overrideInClass(Map.of(
                "nodes.nodeRewardsEnabled", "true",
                "nodes.targetYearlyBlockNodeRewardsUsd", String.valueOf(BLOCK_NODE_YEARLY_REWARD_USD),
                // Disable fee adjustment so expected reward = targetYearly / numPeriods * exchangeRate
                "nodes.adjustNodeFees", "false",
                "nodes.feeCollectionAccountEnabled", "false",
                "ledger.transfers.maxLen", "2"));
        // Only node 2 participates in rewards; all others decline to keep assertions simple.
        testLifecycle.doAdhoc(
                nodeUpdate("0").declineReward(true),
                nodeUpdate("1").declineReward(true),
                nodeUpdate("2").declineReward(false),
                nodeUpdate("3").declineReward(true));
    }

    // -------------------------------------------------------------------------
    // Scenarios 1–3: inactive node — block node associations must NOT add a bonus
    // -------------------------------------------------------------------------

    /**
     * Scenario 1: Inactive node, no registered block node.
     * Node 2 must receive only the minimum per-period reward and nothing more.
     */
    @LeakyRepeatableHapiTest(
            value = {NEEDS_VIRTUAL_TIME_FOR_FAST_EXECUTION},
            overrides = {"nodes.minPerPeriodNodeRewardUsd", "nodes.activeRoundsPercent"})
    @Order(1)
    final Stream<DynamicTest> inactiveNodeWithNoRegisteredBlockNodeGetsOnlyMinimumReward() {
        return blockNodeRewardScenario(false, 0);
    }

    /**
     * Scenario 2: Inactive node, one registered block node.
     * Even with an associated block node, an inactive node must receive only the minimum per-period reward.
     */
    @LeakyRepeatableHapiTest(
            value = {NEEDS_VIRTUAL_TIME_FOR_FAST_EXECUTION},
            overrides = {"nodes.minPerPeriodNodeRewardUsd", "nodes.activeRoundsPercent"})
    @Order(2)
    final Stream<DynamicTest> inactiveNodeWithOneRegisteredBlockNodeGetsOnlyMinimumReward() {
        return blockNodeRewardScenario(false, 1);
    }

    /**
     * Scenario 3: Inactive node, multiple registered block nodes.
     * Multiple block node associations do not change the outcome: an inactive node still receives only the minimum.
     */
    @LeakyRepeatableHapiTest(
            value = {NEEDS_VIRTUAL_TIME_FOR_FAST_EXECUTION},
            overrides = {"nodes.minPerPeriodNodeRewardUsd", "nodes.activeRoundsPercent"})
    @Order(3)
    final Stream<DynamicTest> inactiveNodeWithMultipleRegisteredBlockNodesGetsOnlyMinimumReward() {
        return blockNodeRewardScenario(false, 2);
    }

    // -------------------------------------------------------------------------
    // Scenarios 4–6: active node — block node associations must add the per-period bonus
    // -------------------------------------------------------------------------

    /**
     * Scenario 4: Active node, no registered block node.
     * An active node without an associated block node receives the consensus reward only.
     */
    @LeakyRepeatableHapiTest(
            value = {NEEDS_VIRTUAL_TIME_FOR_FAST_EXECUTION},
            overrides = {"nodes.activeRoundsPercent"})
    @Order(4)
    final Stream<DynamicTest> activeNodeWithNoRegisteredBlockNodeGetsOnlyConsensusReward() {
        return blockNodeRewardScenario(true, 0);
    }

    /**
     * Scenario 5: Active node, one registered block node.
     * An active node with one associated block node receives the consensus reward plus the per-period block node bonus.
     */
    @LeakyRepeatableHapiTest(
            value = {NEEDS_VIRTUAL_TIME_FOR_FAST_EXECUTION},
            overrides = {"nodes.activeRoundsPercent"})
    @Order(5)
    final Stream<DynamicTest> activeNodeWithOneRegisteredBlockNodeGetsConsensusAndBlockReward() {
        return blockNodeRewardScenario(true, 1);
    }

    /**
     * Scenario 6: Active node, multiple registered block nodes.
     * Having more than one associated block node still yields only a single per-period bonus (one per consensus node).
     */
    @LeakyRepeatableHapiTest(
            value = {NEEDS_VIRTUAL_TIME_FOR_FAST_EXECUTION},
            overrides = {"nodes.activeRoundsPercent"})
    @Order(6)
    final Stream<DynamicTest> activeNodeWithMultipleRegisteredBlockNodesGetsConsensusAndBlockReward() {
        return blockNodeRewardScenario(true, 2);
    }

    // -------------------------------------------------------------------------
    // Parameterized scenario builder
    // -------------------------------------------------------------------------

    /**
     * Builds a {@code hapiTest} stream covering one cell of the block node reward eligibility matrix.
     *
     * <ul>
     *   <li>When {@code isActive=false}, overrides {@code nodes.minPerPeriodNodeRewardUsd} to
     *       {@value MIN_INACTIVE_REWARD_USD} USD and asserts node 2 receives exactly that minimum
     *       (no block node bonus, regardless of how many registered block nodes are associated).</li>
     *   <li>When {@code isActive=true}, asserts node 2 receives the consensus reward plus a block
     *       node bonus when {@code numBlockNodes > 0}, or the consensus reward alone otherwise.</li>
     * </ul>
     *
     * @param isActive      whether node 2 should be active during the staking period
     * @param numBlockNodes number of registered block nodes to associate with node 2 (0, 1, or 2)
     */
    private static Stream<DynamicTest> blockNodeRewardScenario(final boolean isActive, final int numBlockNodes) {
        final AtomicReference<Instant> startConsensusTime = new AtomicReference<>();
        final AtomicLong expectedReward = new AtomicLong(0);
        final AtomicLong nodeRewardBalance = new AtomicLong(0);
        // One AtomicLong per registered block node to hold its auto-assigned ID.
        final List<AtomicLong> blockNodeIds = new ArrayList<>();
        for (int i = 0; i < numBlockNodes; i++) {
            blockNodeIds.add(new AtomicLong(-1));
        }

        final List<SpecOperation> ops = new ArrayList<>();

        // Record the consensus time so the record-stream predicate ignores older reward payments.
        ops.add(doingContextual(spec -> startConsensusTime.set(spec.consensusTime())));

        // Register the record-stream assertion listener before any state mutations.
        ops.add(recordStreamMustIncludePassWithoutBackgroundTrafficFrom(
                selectedItems(
                        singleNodeRewardValidator(expectedReward::get, nodeRewardBalance::get),
                        1,
                        (_, item) -> item.getRecord().getTransferList().getAccountAmountsList().stream()
                                        .anyMatch(aa -> aa.getAccountID().getAccountNum() == NODE_REWARD_ACCOUNT_NUM
                                                && aa.getAmount() < 0L)
                                && asInstant(toPbj(item.getRecord().getConsensusTimestamp()))
                                        .isAfter(startConsensusTime.get())),
                Duration.ofSeconds(1)));

        // Ensure the node reward account has sufficient funds.
        ops.add(cryptoTransfer(TokenMovement.movingHbar(ONE_MILLION_HBARS).between(GENESIS, NODE_REWARD)));

        // Inactive scenarios need a non-zero minimum, so there is an actual reward payment to assert.
        if (!isActive) {
            ops.add(overriding("nodes.minPerPeriodNodeRewardUsd", String.valueOf(MIN_INACTIVE_REWARD_USD)));
            ops.add(overriding("nodes.activeRoundsPercent", "100"));
        } else {
            ops.add(overriding("nodes.activeRoundsPercent", "0"));
        }

        // Create registered block nodes and capture their auto-assigned IDs.
        for (int i = 0; i < numBlockNodes; i++) {
            ops.add(registeredNodeCreate("blockNode" + i)
                    .exposingCreatedIdTo(blockNodeIds.get(i)::set));
        }

        // Associate node 2 with the registered block nodes (evaluated lazily so IDs are known).
        if (numBlockNodes > 0) {
            ops.add(withOpContext((spec, _) -> {
                final List<Long> ids = blockNodeIds.stream().map(AtomicLong::get).toList();
                allRunFor(spec, nodeUpdate("2").associatedRegisteredNode(ids));
            }));
        }

        // Advance into a clean staking period, so the next period's activity is what matters.
        ops.add(waitUntilStartOfNextStakingPeriod(1));

        // Ensure any open block is closed and NodeRewardManager's cache is flushed/reset
        // before we mutate the state.
        ops.add(doingContextual(TxnUtils::triggerAndCloseAtLeastOneFileIfNotInterrupted));

        // ZERO-TRANSACTION MUTATION:
        // We mutate the state while no block is open. This ensures that the next
        // block opening will load our mutated state into NRM's memory.
        // We use a large number of rounds so that the additional missed rounds tracked by
        // NodeRewardManager during block processing between now and the next period cross
        // remain negligible relative to the 50% activeRoundsPercent threshold.
        ops.add(mutateSingleton(TokenService.NAME, NODE_REWARDS_STATE_ID, (NodeRewards nodeRewards) -> {
            final int rounds = 10_000;
            final long missed = isActive ? 0 : rounds;
            return nodeRewards.copyBuilder()
                    .numRoundsInStakingPeriod(rounds)
                    .nodeActivities(List.of(
                            NodeActivity.newBuilder()
                                    .nodeId(2)
                                    .numMissedJudgeRounds(missed)
                                    .build()))
                    .build();
        }));

        // Compute the expected reward amount from live config and exchange rate.
        ops.add(doingContextual(spec -> {
            if (!isActive) {
                // Inactive nodes receive only the per-period minimum, regardless of block node associations.
                // convert first to cents and, then, tinycents.
                final long minReward = MIN_INACTIVE_REWARD_USD * 100 * TINY_PARTS_PER_WHOLE;
                expectedReward.set(spec.ratesProvider().toTbWithActiveRates(minReward));
            } else {
                final long numPeriods = spec.startupProperties().getLong("nodes.numPeriodsToTargetUsd");
                final long targetYearly = spec.startupProperties().getLong("nodes.targetYearlyNodeRewardsUsd");
                final long consensusTinybars = spec.ratesProvider()
                        .toTbWithActiveRates((targetYearly * 100 * TINY_PARTS_PER_WHOLE) / numPeriods);

                if (numBlockNodes > 0) {
                    final long blockTinybars = spec.ratesProvider()
                            .toTbWithActiveRates(
                                    (BLOCK_NODE_YEARLY_REWARD_USD * 100 * TINY_PARTS_PER_WHOLE) / numPeriods);
                    expectedReward.set(consensusTinybars + blockTinybars);
                } else {
                    expectedReward.set(consensusTinybars);
                }
            }
        }));

        // Snapshot the reward account balance (used by the validator to cap expected amounts).
        ops.add(getAccountBalance(NODE_REWARD).exposingBalanceTo(nodeRewardBalance::set));

        // Advance to the next staking period — this triggers the reward payment for the period above.
        ops.add(waitUntilStartOfNextStakingPeriod(1));

        ops.add(cryptoCreate("nobody").payingWith(GENESIS));
        ops.add(doingContextual(TxnUtils::triggerAndCloseAtLeastOneFileIfNotInterrupted));
        // Clean up the persistent state so subsequent tests start with a clean slate.

        if (numBlockNodes > 0) {
            ops.add(withOpContext((spec, _) ->
                    allRunFor(spec, nodeUpdate("2").associatedRegisteredNode(List.of()))));
            for (int i = 0; i < numBlockNodes; i++) {
                ops.add(registeredNodeDelete("blockNode" + i));
            }
        }

        return hapiTest(ops.toArray(SpecOperation[]::new));
    }

    // -------------------------------------------------------------------------
    // Assertion helper
    // -------------------------------------------------------------------------

    /**
     * Validates a single-node reward payment where only node 2 is eligible.
     *
     * <p>Asserts that the reward transfer contains exactly two adjustments:
     * a debit from the node reward account (0.0.801) and a credit to node 2 (0.0.5).
     *
     * @param expectedReward   supplier of the expected reward amount in tinybars
     * @param nodeRewardBalance supplier of the node reward account balance (used to cap the expected amount)
     */
    private static VisibleItemsValidator singleNodeRewardValidator(
            @NonNull final LongSupplier expectedReward, @NonNull final LongSupplier nodeRewardBalance) {
        return (_, records) -> {
            final var items = records.get(SELECTED_ITEMS_KEY);
            assertNotNull(items, "No reward payment found in the record stream");
            assertEquals(1, items.size(), "Expected exactly one reward payment");
            final var payment = items.getFirst();
            assertEquals(CryptoTransfer, payment.function());

            final var op = payment.body().getCryptoTransfer();
            long expected = expectedReward.getAsLong();
            // If the reward account balance is not enough, the network caps the payment.
            if (expected > nodeRewardBalance.getAsLong()) {
                expected = nodeRewardBalance.getAsLong();
            }

            final Map<Long, Long> adjustments = op.getTransfers().getAccountAmountsList().stream()
                    .collect(toMap(aa -> aa.getAccountID().getAccountNum(), AccountAmount::getAmount));

            assertEquals(
                    2,
                    adjustments.size(),
                    "Expected exactly 2 adjustments (debit from 0.0.801, credit to node 2 at 0.0.5)");
            assertEquals(
                    -expected,
                    adjustments.get(NODE_REWARD_ACCOUNT_NUM),
                    "Unexpected debit from node reward account 0.0.801; expectedReward=" + expected);
            assertEquals(
                    expected,
                    adjustments.get(NODE_2_ACCOUNT_NUM),
                    "Unexpected credit to node 2 account 0.0.5; expectedReward=" + expected);
        };
    }
}
