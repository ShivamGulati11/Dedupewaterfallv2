using DedupeWaterfall.Core.Enums;
using DedupeWaterfall.Core.Interfaces;
using DedupeWaterfall.Core.Kafka;
using DedupeWaterfall.Core.Models;
using Microsoft.Extensions.Logging;

namespace DedupeWaterfall.Orchestrator.Services;

public class OrchestratorService
{
    private readonly IWaterfallConfigRepository _configRepo;
    private readonly IWaterfallRunRepository    _runRepo;
    private readonly IDedupeHitRepository       _hitRepo;
    private readonly IEventLogRepository        _eventRepo;
    private readonly IKafkaProducer             _kafkaProducer;
    private readonly ILogger<OrchestratorService> _logger;

    public OrchestratorService(
        IWaterfallConfigRepository configRepo,
        IWaterfallRunRepository    runRepo,
        IDedupeHitRepository       hitRepo,
        IEventLogRepository        eventRepo,
        IKafkaProducer             kafkaProducer,
        ILogger<OrchestratorService> logger)
    {
        _configRepo    = configRepo;
        _runRepo       = runRepo;
        _hitRepo       = hitRepo;
        _eventRepo     = eventRepo;
        _kafkaProducer = kafkaProducer;
        _logger        = logger;
    }

    // -----------------------------------------------------------------------
    // Handles a new lead coming off dedupe.leads.queued
    // -----------------------------------------------------------------------
    public async Task ProcessLeadQueuedAsync(
        LeadQueuedMessage msg, CancellationToken ct)
    {
        _logger.LogInformation(
            "[LeadQueued] RunId={RunId} LeadId={LeadId} SnapshotId={SnapshotId}",
            msg.RunId, msg.LeadId, msg.SnapshotId);

        var snapshot = await _configRepo.GetSnapshotAsync(msg.SnapshotId, ct);

        // Find first lender not in the skip list
        var nextLender = snapshot
            .OrderBy(s => s.SequenceOrder)
            .FirstOrDefault(s => !msg.SkipLenders.Contains(
                s.LenderCode, StringComparer.OrdinalIgnoreCase));

        if (nextLender is null)
        {
            // All lenders are in the skip list — derive result from cached statuses
            long? winningLenderId = null;
            WaterfallRunStatus finalStatus = WaterfallRunStatus.RejectedAll;

            foreach (var (lenderCode, cachedStatus) in msg.CachedStatuses)
            {
                if (string.Equals(cachedStatus, "Approved",
                        StringComparison.OrdinalIgnoreCase))
                {
                    var matchedLender = snapshot.FirstOrDefault(s =>
                        string.Equals(s.LenderCode, lenderCode,
                            StringComparison.OrdinalIgnoreCase));

                    finalStatus   = WaterfallRunStatus.Approved;
                    winningLenderId = matchedLender?.LenderId;
                    break;
                }
            }

            _logger.LogInformation(
                "[LeadQueued] AllSkipped RunId={RunId} LeadId={LeadId} " +
                "FinalStatus={Status} WinningLenderId={WinningLenderId}",
                msg.RunId, msg.LeadId, finalStatus, winningLenderId);

            await _runRepo.UpdateRunStatusAsync(
                msg.RunId, finalStatus, snapshot.Count, winningLenderId, ct);

            await _eventRepo.BufferEventAsync(
                msg.RunId, msg.LeadId, null,
                WaterfallEventType.WaterfallComplete,
                new { reason = "AllSkipped", cached = msg.CachedStatuses }, ct);

            return;
        }

        await TriggerLenderAsync(msg, nextLender, ct);
    }

    // -----------------------------------------------------------------------
    // Handles a lender result coming off dedupe.lender.result
    // -----------------------------------------------------------------------
    public async Task ProcessLenderResultAsync(
        LenderResultMessage msg, CancellationToken ct)
    {
        _logger.LogInformation(
            "[LenderResult] RunId={RunId} LeadId={LeadId} " +
            "LenderCode={LenderCode} Status={Status}",
            msg.RunId, msg.LeadId, msg.LenderCode, msg.Status);

        if (string.Equals(msg.Status, "Approved", StringComparison.OrdinalIgnoreCase))
        {
            await _runRepo.UpdateRunStatusAsync(
                msg.RunId, WaterfallRunStatus.Approved,
                msg.SequenceOrder, msg.LenderId, ct);

            await _eventRepo.BufferEventAsync(
                msg.RunId, msg.LeadId, msg.LenderId,
                WaterfallEventType.DedupeApproved,
                new { lenderCode = msg.LenderCode }, ct);

            await _eventRepo.BufferEventAsync(
                msg.RunId, msg.LeadId, msg.LenderId,
                WaterfallEventType.WaterfallComplete,
                new { winningLender = msg.LenderCode }, ct);

            _logger.LogInformation(
                "[LenderResult] Approved RunId={RunId} LeadId={LeadId} " +
                "WinningLender={LenderCode}",
                msg.RunId, msg.LeadId, msg.LenderCode);

            return;
        }

        // Rejected — advance to next lender in sequence
        if (string.Equals(msg.Status, "Rejected", StringComparison.OrdinalIgnoreCase))
        {
            await _eventRepo.BufferEventAsync(
                msg.RunId, msg.LeadId, msg.LenderId,
                WaterfallEventType.DedupeRejected,
                new { lenderCode = msg.LenderCode }, ct);

            var snapshot = await _configRepo.GetSnapshotAsync(msg.SnapshotId, ct);

            var nextLender = snapshot
                .Where(s => s.SequenceOrder > msg.SequenceOrder)
                .OrderBy(s => s.SequenceOrder)
                .FirstOrDefault(s => !msg.SkipLenders.Contains(
                    s.LenderCode, StringComparer.OrdinalIgnoreCase));

            if (nextLender is null)
            {
                // Exhausted all lenders
                await _runRepo.UpdateRunStatusAsync(
                    msg.RunId, WaterfallRunStatus.RejectedAll,
                    msg.SequenceOrder, null, ct);

                await _eventRepo.BufferEventAsync(
                    msg.RunId, msg.LeadId, null,
                    WaterfallEventType.WaterfallComplete,
                    new { reason = "AllRejected" }, ct);

                _logger.LogInformation(
                    "[LenderResult] AllRejected RunId={RunId} LeadId={LeadId}",
                    msg.RunId, msg.LeadId);

                return;
            }

            await TriggerLenderAsync(msg, nextLender, ct);
        }
        else
        {
            _logger.LogWarning(
                "[LenderResult] Unknown status '{Status}' for RunId={RunId} " +
                "LeadId={LeadId}. Ignoring.",
                msg.Status, msg.RunId, msg.LeadId);
        }
    }

    // -----------------------------------------------------------------------
    // Shared: insert a pending hit, update run status, fire event, produce
    // -----------------------------------------------------------------------
    private async Task TriggerLenderAsync(
        ILeadMessage msg,
        WaterfallConfigSnapshot lender,
        CancellationToken ct)
    {
        await _hitRepo.InsertHitAsync(
            msg.RunId, msg.LeadId, lender.LenderId,
            lender.SequenceOrder, ct);

        await _runRepo.UpdateRunStatusAsync(
            msg.RunId, WaterfallRunStatus.InProgress,
            lender.SequenceOrder, null, ct);

        await _eventRepo.BufferEventAsync(
            msg.RunId, msg.LeadId, lender.LenderId,
            WaterfallEventType.DedupeHitInitiated,
            new { lenderCode = lender.LenderCode, sequence = lender.SequenceOrder }, ct);

        var request = new LenderRequestMessage
        {
            MessageId     = Guid.NewGuid(),
            BaseId        = msg.BaseId,
            RunId         = msg.RunId,
            LeadId        = msg.LeadId,
            GuserId       = msg.GuserId,
            Mobile        = msg.Mobile,
            Pan           = msg.Pan,
            FullName      = msg.FullName,
            SnapshotId    = msg.SnapshotId,
            LenderId      = lender.LenderId,
            LenderCode    = lender.LenderCode,
            SequenceOrder = lender.SequenceOrder,
            SkipLenders   = msg.SkipLenders,
            CachedStatuses = msg.CachedStatuses,
            Timestamp     = DateTime.UtcNow,
            CorrelationId = msg.CorrelationId
        };

        await _kafkaProducer.ProduceAsync(
            KafkaTopics.LenderRequest(lender.LenderCode),
            msg.LeadId.ToString(),
            request, ct);

        _logger.LogInformation(
            "[TriggerLender] RunId={RunId} LeadId={LeadId} " +
            "LenderCode={LenderCode} Sequence={Sequence} Status=InProgress",
            msg.RunId, msg.LeadId, lender.LenderCode, lender.SequenceOrder);
    }
}
