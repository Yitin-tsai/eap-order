package com.eap.eap_order.application;

import com.eap.common.dto.AuctionBidRequest;
import com.eap.common.dto.AuctionBidResponse;
import com.eap.common.event.AuctionBidSubmittedEvent;
import com.eap.eap_order.application.OutBound.EapMatchEngine;
import com.eap.eap_order.configuration.repository.AuctionBidRepository;
import com.eap.eap_order.domain.entity.AuctionBidEntity;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.eap.common.constants.RabbitMQConstants.*;

@Service
@Slf4j
public class PlaceAuctionBidService {

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private AuctionBidRepository auctionBidRepository;

    @Autowired
    private EapMatchEngine eapMatchEngine;

    @Autowired
    private ObjectMapper objectMapper;

    public AuctionBidResponse submitBid(AuctionBidRequest request) {
        // 1. Validate
        if (!request.isValid()) {
            return AuctionBidResponse.failure(request.getValidationError());
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

        // 3. Forward to matchEngine for Redis collection
        try {
            ResponseEntity<AuctionBidResponse> matchResponse = eapMatchEngine.submitAuctionBid(request);
            if (matchResponse.getBody() == null || !matchResponse.getBody().isSuccess()) {
                String errorMsg = matchResponse.getBody() != null
                    ? matchResponse.getBody().getMessage()
                    : "MatchEngine returned empty response";
                log.warn("MatchEngine rejected auction bid: {}", errorMsg);
                return AuctionBidResponse.failure(errorMsg);
            }
        } catch (Exception e) {
            log.error("Failed to forward bid to matchEngine: {}", e.getMessage(), e);
            return AuctionBidResponse.failure("Failed to submit bid to matching engine: " + e.getMessage());
        }

        // 4. Persist to local DB
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

        // 5. Publish event for wallet locking
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
