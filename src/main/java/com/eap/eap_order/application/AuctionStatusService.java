package com.eap.eap_order.application;

import com.eap.common.dto.AuctionResultDto;
import com.eap.common.dto.AuctionStatusDto;
import com.eap.eap_order.configuration.repository.AuctionBidRepository;
import com.eap.eap_order.configuration.repository.AuctionResultRepository;
import com.eap.eap_order.configuration.repository.AuctionSessionRepository;
import com.eap.eap_order.domain.entity.AuctionResultEntity;
import com.eap.eap_order.domain.entity.AuctionSessionEntity;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
@Slf4j
public class AuctionStatusService {

    @Autowired
    private AuctionSessionRepository auctionSessionRepository;

    @Autowired
    private AuctionResultRepository auctionResultRepository;

    @Autowired
    private AuctionBidRepository auctionBidRepository;

    /**
     * Get the current active auction status (latest OPEN or CLEARING session)
     */
    public AuctionStatusDto getCurrentAuctionStatus() {
        // Try OPEN first, then CLEARING
        AuctionSessionEntity session = auctionSessionRepository
            .findFirstByStatusOrderByCreatedAtDesc("OPEN")
            .orElse(null);

        if (session == null) {
            session = auctionSessionRepository
                .findFirstByStatusOrderByCreatedAtDesc("CLEARING")
                .orElse(null);
        }

        if (session == null) {
            return AuctionStatusDto.builder()
                .status("NO_ACTIVE_AUCTION")
                .build();
        }

        return buildStatusDto(session);
    }

    /**
     * Get auction results by auctionId
     */
    public AuctionResultDto getAuctionResults(String auctionId) {
        AuctionSessionEntity session = auctionSessionRepository.findByAuctionId(auctionId)
            .orElse(null);

        if (session == null) {
            return AuctionResultDto.builder()
                .auctionId(auctionId)
                .results(List.of())
                .build();
        }

        List<AuctionResultEntity> results = auctionResultRepository.findByAuctionId(auctionId);
        List<AuctionResultDto.UserResult> userResults = results.stream()
            .map(r -> AuctionResultDto.UserResult.builder()
                .userId(r.getUserId().toString())
                .side(r.getSide())
                .bidAmount(r.getBidAmount())
                .clearedAmount(r.getClearedAmount())
                .settlementAmount(r.getSettlementAmount())
                .status(r.getClearedAmount() > 0 ? "CLEARED" : "NOT_CLEARED")
                .build())
            .collect(Collectors.toList());

        return AuctionResultDto.builder()
            .auctionId(auctionId)
            .clearingPrice(session.getClearingPrice())
            .clearingVolume(session.getClearingVolume())
            .participantCount(session.getParticipantCount())
            .results(userResults)
            .clearedAt(session.getCreatedAt())
            .build();
    }

    /**
     * Get auction history (recent cleared auctions)
     */
    public List<AuctionStatusDto> getAuctionHistory(int limit) {
        List<AuctionSessionEntity> sessions = auctionSessionRepository
            .findByStatusOrderByCreatedAtDesc("CLEARED");

        return sessions.stream()
            .limit(limit)
            .map(this::buildStatusDto)
            .collect(Collectors.toList());
    }

    /**
     * Get user-specific results for an auction
     */
    public List<AuctionResultEntity> getUserAuctionResults(String auctionId, UUID userId) {
        return auctionResultRepository.findByAuctionIdAndUserId(auctionId, userId);
    }

    private AuctionStatusDto buildStatusDto(AuctionSessionEntity session) {
        return AuctionStatusDto.builder()
            .auctionId(session.getAuctionId())
            .status(session.getStatus())
            .deliveryHour(session.getDeliveryHour())
            .openTime(session.getOpenTime())
            .closeTime(session.getCloseTime())
            .participantCount(session.getParticipantCount())
            .clearingPrice(session.getClearingPrice())
            .clearingVolume(session.getClearingVolume())
            .priceFloor(session.getPriceFloor())
            .priceCeiling(session.getPriceCeiling())
            .build();
    }
}
