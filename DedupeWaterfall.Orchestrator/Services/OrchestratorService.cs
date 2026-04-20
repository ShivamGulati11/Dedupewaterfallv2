using Confluent.Kafka;
using DedupeWaterfall.Core.Contracts;
using DedupeWaterfall.Core.Enums;
using DedupeWaterfall.Core.Interfaces;
using DedupeWaterfall.Core.Models;
using DedupeWaterfall.Orchestrator.Options;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Text.Json;

namespace DedupeWaterfall.Orchestrator.Services;

/// <summary>
/// Core routing logic that:
/// <list type="bullet">
///   <item>Resolves the first eligible lender from the snapshot and publishes a trigger message.</item>
///   <item>On a lender result, either declares a winner or advances to the next step.</item>
/// </list>
/// This service never calls a lender API directly — it only produces Kafka messages.
/// </summary>
public sealed class OrchestratorService : IOrchestratorService
{
    private readonly IWaterfallSnapshotRepository _snapshots;
    private readonly ILeadRunStateRepository _states;
    private readonly IProducer<string, string> _producer;
    private readonly KafkaOptions _kafkaOptions;
    private readonly ILogger<OrchestratorService> _logger;

    public OrchestratorService(
        IWaterfallSnapshotRepository snapshots,
        ILeadRunStateRepository states,
        IProducer<string, string> producer,
        IOptions<KafkaOptions> kafkaOptions,
        ILogger<OrchestratorService> logger)
    {
        _snapshots = snapshots;
        _states = states;
        _producer = producer;
        _kafkaOptions = kafkaOptions.Value;
        _logger = logger;
    }

    /// <inheritdoc />
    public async Task HandleLeadQueuedAsync(LeadQueuedMessage message, CancellationToken ct = default)
    {
        _logger.LogInformation(
            "Lead queued: RunId={RunId} LeadId={LeadId} SnapshotId={SnapshotId}",
            message.RunId, message.LeadId, message.SnapshotId);

        var snapshot = await _snapshots.GetByIdAsync(message.SnapshotId, ct);
        if (snapshot is null)
        {
            _logger.LogError(
                "Snapshot {SnapshotId} not found for RunId={RunId}. Exhausting lead.",
                message.SnapshotId, message.RunId);
            await PublishExhaustedAsync(message.RunId, message.LeadId, 0, ct);
            return;
        }

        // Persist initial run state
        var runState = new LeadRunState
        {
            RunId = message.RunId,
            LeadId = message.LeadId,
            BaseId = message.BaseId,
            SnapshotId = message.SnapshotId,
            Status = LeadStatus.InProgress,
            CurrentStepOrder = 0,
            CreatedAt = DateTime.UtcNow,
            UpdatedAt = DateTime.UtcNow
        };
        await _states.UpsertAsync(runState, ct);

        var eligibleStep = FindNextEligibleStep(snapshot.Steps, stepOrderAfter: -1, message.SkipLenders, message.CachedStatuses);
        if (eligibleStep is null)
        {
            _logger.LogInformation("RunId={RunId}: no eligible steps — exhausting immediately.", message.RunId);
            await ExhaustLeadAsync(message.RunId, message.LeadId, snapshot.Steps.Count, ct);
            return;
        }

        await TriggerLenderAsync(message, eligibleStep, ct);
    }

    /// <inheritdoc />
    public async Task HandleLenderResultAsync(LenderResultMessage message, CancellationToken ct = default)
    {
        _logger.LogInformation(
            "Lender result: RunId={RunId} LenderId={LenderId} Status={Status}",
            message.RunId, message.LenderId, message.Status);

        var state = await _states.GetByRunIdAsync(message.RunId, ct);
        if (state is null)
        {
            _logger.LogWarning("RunId={RunId} not found in state store — ignoring result.", message.RunId);
            return;
        }

        if (state.Status is LeadStatus.Won or LeadStatus.Exhausted)
        {
            _logger.LogInformation(
                "RunId={RunId} already in terminal state {Status} — ignoring duplicate result.",
                message.RunId, state.Status);
            return;
        }

        if (message.Status == LenderStatus.Accepted)
        {
            _logger.LogInformation(
                "RunId={RunId} WON by LenderId={LenderId}.", message.RunId, message.LenderId);

            await _states.UpdateStatusAsync(message.RunId, LeadStatus.Won, message.LenderId, ct);
            await PublishWonAsync(message.RunId, message.LeadId, message.LenderId, state.CurrentStepOrder, ct);
            return;
        }

        // Lender rejected / errored — advance to next step
        var snapshot = await _snapshots.GetByIdAsync(state.SnapshotId, ct);
        if (snapshot is null)
        {
            _logger.LogError(
                "Snapshot {SnapshotId} missing for RunId={RunId} during result handling.",
                state.SnapshotId, message.RunId);
            await ExhaustLeadAsync(message.RunId, message.LeadId, state.CurrentStepOrder, ct);
            return;
        }

        var nextStep = FindNextEligibleStep(
            snapshot.Steps,
            stepOrderAfter: state.CurrentStepOrder,
            skipLenders: [],
            cachedStatuses: new Dictionary<long, LenderStatus>());

        if (nextStep is null)
        {
            _logger.LogInformation("RunId={RunId}: waterfall exhausted after step {Step}.", message.RunId, state.CurrentStepOrder);
            await ExhaustLeadAsync(message.RunId, message.LeadId, snapshot.Steps.Count, ct);
            return;
        }

        // Update step pointer
        state.CurrentStepOrder = nextStep.StepOrder;
        state.UpdatedAt = DateTime.UtcNow;
        await _states.UpsertAsync(state, ct);

        // Build a minimal trigger from saved state — original PII is in the LeadQueuedMessage
        // which was already persisted by the caller/upstream service.
        var trigger = new LenderTriggerMessage
        {
            MessageId = Guid.NewGuid(),
            RunId = message.RunId,
            LeadId = message.LeadId,
            LenderId = nextStep.LenderId,
            BaseId = state.BaseId,
            GuserId = string.Empty,   // PII not re-propagated; lender retrieves from Lead store
            TriggeredAt = DateTime.UtcNow
        };

        await PublishAsync(nextStep.TriggerTopic, trigger.RunId.ToString(), trigger, ct);
    }

    // -----------------------------------------------------------------------
    //  Private helpers
    // -----------------------------------------------------------------------

    private static WaterfallStep? FindNextEligibleStep(
        IEnumerable<WaterfallStep> steps,
        int stepOrderAfter,
        IEnumerable<long> skipLenders,
        IReadOnlyDictionary<long, LenderStatus> cachedStatuses)
    {
        var skipSet = new HashSet<long>(skipLenders);

        return steps
            .Where(s => s.StepOrder > stepOrderAfter)
            .OrderBy(s => s.StepOrder)
            .FirstOrDefault(s =>
                !skipSet.Contains(s.LenderId) &&
                (!cachedStatuses.TryGetValue(s.LenderId, out var cached) ||
                 cached is not (LenderStatus.Rejected or LenderStatus.Skipped or LenderStatus.Accepted)));
    }

    private async Task TriggerLenderAsync(LeadQueuedMessage lead, WaterfallStep step, CancellationToken ct)
    {
        var trigger = new LenderTriggerMessage
        {
            MessageId = Guid.NewGuid(),
            RunId = lead.RunId,
            LeadId = lead.LeadId,
            LenderId = step.LenderId,
            BaseId = lead.BaseId,
            GuserId = lead.GuserId,
            Mobile = lead.Mobile,
            Pan = lead.Pan,
            FullName = lead.FullName,
            TriggeredAt = DateTime.UtcNow
        };

        await _states.UpdateStatusAsync(lead.RunId, LeadStatus.InProgress, ct: ct);
        await PublishAsync(step.TriggerTopic, trigger.RunId.ToString(), trigger, ct);

        _logger.LogInformation(
            "Triggered LenderId={LenderId} (step {Step}) for RunId={RunId} on topic '{Topic}'.",
            step.LenderId, step.StepOrder, lead.RunId, step.TriggerTopic);
    }

    private async Task ExhaustLeadAsync(long runId, long leadId, int stepsAttempted, CancellationToken ct)
    {
        await _states.UpdateStatusAsync(runId, LeadStatus.Exhausted, ct: ct);
        await PublishExhaustedAsync(runId, leadId, stepsAttempted, ct);
    }

    private Task PublishWonAsync(long runId, long leadId, long winnerLenderId, int stepOrder, CancellationToken ct)
    {
        var msg = new LeadWonMessage
        {
            MessageId = Guid.NewGuid(),
            RunId = runId,
            LeadId = leadId,
            WinnerLenderId = winnerLenderId,
            WinningStepOrder = stepOrder,
            WonAt = DateTime.UtcNow
        };
        return PublishAsync(_kafkaOptions.LeadsWonTopic, runId.ToString(), msg, ct);
    }

    private Task PublishExhaustedAsync(long runId, long leadId, int stepsAttempted, CancellationToken ct)
    {
        var msg = new LeadExhaustedMessage
        {
            MessageId = Guid.NewGuid(),
            RunId = runId,
            LeadId = leadId,
            TotalStepsAttempted = stepsAttempted,
            ExhaustedAt = DateTime.UtcNow
        };
        return PublishAsync(_kafkaOptions.LeadsExhaustedTopic, runId.ToString(), msg, ct);
    }

    private async Task PublishAsync<T>(string topic, string key, T payload, CancellationToken ct)
    {
        var json = JsonSerializer.Serialize(payload);
        var kafkaMessage = new Message<string, string> { Key = key, Value = json };
        await _producer.ProduceAsync(topic, kafkaMessage, ct);
    }
}
