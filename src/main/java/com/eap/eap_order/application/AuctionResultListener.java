package com.eap.eap_order.application;

import com.eap.common.dto.AuctionResultDto;
import com.eap.common.dto.AuctionStatusDto;
import com.eap.common.event.AuctionClearedEvent;
import com.eap.common.event.AuctionCreatedEvent;
import com.eap.eap_order.configuration.repository.AuctionBidRepository;
import com.eap.eap_order.configuration.repository.AuctionResultRepository;
import com.eap.eap_order.configuration.repository.AuctionSessionRepository;
import com.eap.eap_order.domain.entity.AuctionBidEntity;
import com.eap.eap_order.domain.entity.AuctionResultEntity;
import com.eap.eap_order.domain.entity.AuctionSessionEntity;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.eap.common.constants.RabbitMQConstants.*;

@Component
@Slf4j
public class AuctionResultListener {

    @Autowired
    private AuctionSessionRepository auctionSessionRepository;

    @Autowired
    private AuctionBidRepository auctionBidRepository;

    @Autowired
    private AuctionResultRepository auctionResultRepository;

    @Autowired
    private SimpMessagingTemplate messagingTemplate;

    @Autowired
    private AuditService auditService;

    @RabbitListener(queues = ORDER_AUCTION_CLEARED_QUEUE)
    @Transactional
    public void handleAuctionCleared(AuctionClearedEvent event) {
        log.info("Received AuctionClearedEvent: auctionId={}, status={}, clearingPrice={}",
            event.getAuctionId(), event.getStatus(), event.getClearingPrice());

        try {
            // 1. Update auction session
            AuctionSessionEntity session = auctionSessionRepository.findByAuctionId(event.getAuctionId())
                .orElse(null);
            if (session == null) {
                log.error("Auction session not found: auctionId={}", event.getAuctionId());
                return;
            }

            session.setClearingPrice(event.getClearingPrice());
            session.setClearingVolume(event.getClearingVolume());
            session.setStatus(event.getStatus());
            if (event.getResults() != null) {
                session.setParticipantCount(event.getResults().size());
            }
            auctionSessionRepository.save(session);

            // 2. Save individual results and update bid statuses (batch)
            if (event.getResults() != null && !event.getResults().isEmpty()) {
                // Batch load all bids for this auction into a map
                Map<UUID, List<AuctionBidEntity>> bidsByUser = auctionBidRepository
                    .findByAuctionId(event.getAuctionId()).stream()
                    .collect(Collectors.groupingBy(AuctionBidEntity::getUserId));

                List<AuctionResultEntity> resultEntities = new ArrayList<>();
                List<AuctionBidEntity> updatedBids = new ArrayList<>();

                for (AuctionClearedEvent.AuctionBidResult result : event.getResults()) {
                    resultEntities.add(AuctionResultEntity.builder()
                        .auctionId(event.getAuctionId())
                        .userId(result.getUserId())
                        .side(result.getSide())
                        .clearingPrice(event.getClearingPrice())
                        .bidAmount(result.getBidAmount())
                        .clearedAmount(result.getClearedAmount())
                        .settlementAmount(result.getSettlementAmount())
                        .createdAt(LocalDateTime.now())
                        .build());

                    // Update bid statuses
                    List<AuctionBidEntity> userBids = bidsByUser.getOrDefault(result.getUserId(), List.of());
                    for (AuctionBidEntity bid : userBids) {
                        if (result.getClearedAmount() > 0 && result.getClearedAmount() >= result.getBidAmount()) {
                            bid.setStatus("CLEARED");
                        } else if (result.getClearedAmount() > 0) {
                            bid.setStatus("PARTIAL");
                        } else {
                            bid.setStatus("NOT_CLEARED");
                        }
                        updatedBids.add(bid);
                    }
                }

                auctionResultRepository.saveAll(resultEntities);
                auctionBidRepository.saveAll(updatedBids);
            }

            // 3. Audit trail
            auditService.record("AUCTION_CLEARED", event.getAuctionId(), null, event);

            // 4. Push to WebSocket
            AuctionResultDto resultDto = buildResultDto(event, session);
            messagingTemplate.convertAndSend("/topic/auction/result", resultDto);
            log.info("Auction cleared event processed: auctionId={}, MCP={}, MCV={}",
                event.getAuctionId(), event.getClearingPrice(), event.getClearingVolume());

        } catch (Exception e) {
            log.error("Failed to process AuctionClearedEvent: auctionId={}, error={}",
                event.getAuctionId(), e.getMessage(), e);
        }
    }

    @RabbitListener(queues = ORDER_AUCTION_CREATED_QUEUE)
    public void handleAuctionCreated(AuctionCreatedEvent event) {
        log.info("Received AuctionCreatedEvent: auctionId={}", event.getAuctionId());

        try {
            // 1. Create and save auction session
            AuctionSessionEntity session = AuctionSessionEntity.builder()
                .auctionId(event.getAuctionId())
                .status("OPEN")
                .deliveryHour(event.getDeliveryHour())
                .openTime(event.getOpenTime())
                .closeTime(event.getCloseTime())
                .priceFloor(event.getPriceFloor())
                .priceCeiling(event.getPriceCeiling())
                .participantCount(0)
                .createdAt(event.getCreatedAt() != null ? event.getCreatedAt() : LocalDateTime.now())
                .build();
            auctionSessionRepository.save(session);

            // 2. Push to WebSocket
            AuctionStatusDto statusDto = AuctionStatusDto.builder()
                .auctionId(session.getAuctionId())
                .status(session.getStatus())
                .deliveryHour(session.getDeliveryHour())
                .openTime(session.getOpenTime())
                .closeTime(session.getCloseTime())
                .participantCount(session.getParticipantCount())
                .priceFloor(session.getPriceFloor())
                .priceCeiling(session.getPriceCeiling())
                .build();
            messagingTemplate.convertAndSend("/topic/auction/status", statusDto);
            log.info("Auction session created and WebSocket notified: auctionId={}", event.getAuctionId());

            auditService.record("AUCTION_CREATED", event.getAuctionId(), null, event);

        } catch (Exception e) {
            log.error("Failed to process AuctionCreatedEvent: auctionId={}, error={}",
                event.getAuctionId(), e.getMessage(), e);
        }
    }

    private AuctionResultDto buildResultDto(AuctionClearedEvent event, AuctionSessionEntity session) {
        List<AuctionResultDto.UserResult> userResults = event.getResults() != null
            ? event.getResults().stream()
                .map(r -> AuctionResultDto.UserResult.builder()
                    .userId(r.getUserId().toString())
                    .side(r.getSide())
                    .bidAmount(r.getBidAmount())
                    .clearedAmount(r.getClearedAmount())
                    .settlementAmount(r.getSettlementAmount())
                    .status(r.getClearedAmount() > 0 ? "CLEARED" : "NOT_CLEARED")
                    .build())
                .collect(Collectors.toList())
            : List.of();

        return AuctionResultDto.builder()
            .auctionId(event.getAuctionId())
            .clearingPrice(event.getClearingPrice())
            .clearingVolume(event.getClearingVolume())
            .participantCount(session.getParticipantCount())
            .results(userResults)
            .clearedAt(event.getClearedAt())
            .build();
    }
}
