package com.eap.eap_order.application;

import com.eap.common.dto.AuctionBidRequest;
import com.eap.common.dto.AuctionBidResponse;
import com.eap.common.event.AuctionBidSubmittedEvent;
import com.eap.eap_order.configuration.repository.AuctionBidRepository;
import com.eap.eap_order.domain.entity.AuctionBidEntity;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.eap.common.constants.RabbitMQConstants.*;

/**
 * Auction bid submission service.
 *
 * Follows the wallet-first + outbox pattern (same as CDA order flow):
 * 1. Validate request
 * 2. Persist bid to local DB (status=SUBMITTED)
 * 3. Publish AuctionBidSubmittedEvent → wallet locks funds + outbox → matchEngine
 *
 * The bid does NOT go directly to matchEngine. Only wallet-confirmed bids
 * reach matchEngine via the outbox pattern, ensuring auction fairness.
 */
@Service
@Slf4j
public class PlaceAuctionBidService {

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private AuctionBidRepository auctionBidRepository;

    @Autowired
    private ObjectMapper objectMapper;

    public AuctionBidResponse submitBid(AuctionBidRequest request) {
        // 1. Validate
        if (!request.isValid()) {
            return AuctionBidResponse.failure(request.getValidationError());
        }

        // 1b. Duplicate bid check — one bid per user per auction (regardless of side)
        if (auctionBidRepository.existsByAuctionIdAndUserId(
                request.getAuctionId(), UUID.fromString(request.getUserId()))) {
            log.warn("Duplicate bid rejected: auctionId={}, userId={}",
                request.getAuctionId(), request.getUserId());
            return AuctionBidResponse.failure("User has already submitted a bid for this auction");
        }

        // 2. Calculate totalLocked
        int totalLocked;
        if (request.isBuy()) {
            totalLocked = request.getSteps().stream()
                .mapToInt(step -> step.getPrice() * step.getAmount())
                .sum();
        } else {
            totalLocked = request.getSteps().stream()
                .mapToInt(AuctionBidRequest.BidStep::getAmount)
                .sum();
        }

        // 3. Persist to local DB
        try {
            String stepsJson = objectMapper.writeValueAsString(request.getSteps());
            AuctionBidEntity entity = AuctionBidEntity.builder()
                .auctionId(request.getAuctionId())
                .userId(UUID.fromString(request.getUserId()))
                .side(request.getSide().toUpperCase())
                .steps(stepsJson)
                .totalLocked(totalLocked)
                .status("SUBMITTED")
                .createdAt(LocalDateTime.now())
                .build();
            auctionBidRepository.save(entity);
            log.info("Auction bid persisted: auctionId={}, userId={}, side={}",
                request.getAuctionId(), request.getUserId(), request.getSide());
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize bid steps: {}", e.getMessage(), e);
            return AuctionBidResponse.failure("Internal error: failed to serialize bid steps");
        }

        // 4. Publish event to wallet for fund locking (wallet-first pattern)
        List<AuctionBidSubmittedEvent.BidStep> eventSteps = request.getSteps().stream()
            .map(step -> new AuctionBidSubmittedEvent.BidStep(step.getPrice(), step.getAmount()))
            .collect(Collectors.toList());

        AuctionBidSubmittedEvent event = AuctionBidSubmittedEvent.builder()
            .auctionId(request.getAuctionId())
            .userId(UUID.fromString(request.getUserId()))
            .side(request.getSide().toUpperCase())
            .steps(eventSteps)
            .totalLocked(totalLocked)
            .createdAt(LocalDateTime.now())
            .build();

        rabbitTemplate.convertAndSend(AUCTION_EXCHANGE, AUCTION_BID_SUBMITTED_KEY, event);
        log.info("AuctionBidSubmittedEvent published: auctionId={}, userId={}, totalLocked={}",
            request.getAuctionId(), request.getUserId(), totalLocked);

        return AuctionBidResponse.success(
            request.getAuctionId(), request.getUserId(), request.getSide().toUpperCase(), totalLocked);
    }
}
