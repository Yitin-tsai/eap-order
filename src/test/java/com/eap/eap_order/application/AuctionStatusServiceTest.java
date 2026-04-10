package com.eap.eap_order.application;

import com.eap.common.dto.AuctionResultDto;
import com.eap.common.dto.AuctionStatusDto;
import com.eap.eap_order.configuration.repository.AuctionBidRepository;
import com.eap.eap_order.configuration.repository.AuctionResultRepository;
import com.eap.eap_order.configuration.repository.AuctionSessionRepository;
import com.eap.eap_order.domain.entity.AuctionResultEntity;
import com.eap.eap_order.domain.entity.AuctionSessionEntity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.data.domain.PageRequest;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AuctionStatusServiceTest {

    @Mock
    private AuctionSessionRepository auctionSessionRepository;

    @Mock
    private AuctionResultRepository auctionResultRepository;

    @Mock
    private AuctionBidRepository auctionBidRepository;

    @InjectMocks
    private AuctionStatusService auctionStatusService;

    private static final String AUCTION_ID = "AUCTION-2026-001";
    private static final UUID USER_ID = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
    private static final LocalDateTime NOW = LocalDateTime.now();

    private AuctionSessionEntity openSession;
    private AuctionSessionEntity clearedSession;

    @BeforeEach
    void setUp() {
        openSession = AuctionSessionEntity.builder()
            .id(1L)
            .auctionId(AUCTION_ID)
            .status("OPEN")
            .deliveryHour(NOW.plusHours(2))
            .openTime(NOW.minusMinutes(5))
            .closeTime(NOW.plusMinutes(25))
            .priceFloor(50)
            .priceCeiling(200)
            .participantCount(3)
            .createdAt(NOW.minusMinutes(5))
            .build();

        clearedSession = AuctionSessionEntity.builder()
            .id(2L)
            .auctionId("AUCTION-2026-000")
            .status("CLEARED")
            .deliveryHour(NOW.minusHours(1))
            .openTime(NOW.minusHours(2))
            .closeTime(NOW.minusHours(1).minusMinutes(30))
            .priceFloor(50)
            .priceCeiling(200)
            .clearingPrice(95)
            .clearingVolume(200)
            .participantCount(5)
            .createdAt(NOW.minusHours(2))
            .build();
    }

    // ─── getCurrentAuctionStatus ────────────────────────────────────────────

    @Test
    @DisplayName("getCurrentAuctionStatus: returns OPEN session when one exists")
    void getCurrentAuctionStatus_openSessionExists_returnsOpenStatus() {
        when(auctionSessionRepository.findFirstByStatusOrderByCreatedAtDesc("OPEN"))
            .thenReturn(Optional.of(openSession));

        AuctionStatusDto result = auctionStatusService.getCurrentAuctionStatus();

        assertThat(result.getStatus()).isEqualTo("OPEN");
        assertThat(result.getAuctionId()).isEqualTo(AUCTION_ID);
        assertThat(result.getParticipantCount()).isEqualTo(3);
        assertThat(result.getPriceFloor()).isEqualTo(50);
        assertThat(result.getPriceCeiling()).isEqualTo(200);
        verify(auctionSessionRepository, never()).findFirstByStatusOrderByCreatedAtDesc("CLEARING");
    }

    @Test
    @DisplayName("getCurrentAuctionStatus: falls back to CLEARING session when no OPEN session")
    void getCurrentAuctionStatus_noOpenSession_fallsBackToClearing() {
        AuctionSessionEntity clearingSession = AuctionSessionEntity.builder()
            .id(3L)
            .auctionId(AUCTION_ID)
            .status("CLEARING")
            .deliveryHour(NOW.plusHours(1))
            .openTime(NOW.minusMinutes(30))
            .closeTime(NOW)
            .priceFloor(50)
            .priceCeiling(200)
            .participantCount(4)
            .createdAt(NOW.minusMinutes(30))
            .build();

        when(auctionSessionRepository.findFirstByStatusOrderByCreatedAtDesc("OPEN"))
            .thenReturn(Optional.empty());
        when(auctionSessionRepository.findFirstByStatusOrderByCreatedAtDesc("CLEARING"))
            .thenReturn(Optional.of(clearingSession));

        AuctionStatusDto result = auctionStatusService.getCurrentAuctionStatus();

        assertThat(result.getStatus()).isEqualTo("CLEARING");
        assertThat(result.getAuctionId()).isEqualTo(AUCTION_ID);
    }

    @Test
    @DisplayName("getCurrentAuctionStatus: returns NO_ACTIVE_AUCTION when no OPEN or CLEARING session")
    void getCurrentAuctionStatus_noActiveSession_returnsNoActiveAuction() {
        when(auctionSessionRepository.findFirstByStatusOrderByCreatedAtDesc("OPEN"))
            .thenReturn(Optional.empty());
        when(auctionSessionRepository.findFirstByStatusOrderByCreatedAtDesc("CLEARING"))
            .thenReturn(Optional.empty());

        AuctionStatusDto result = auctionStatusService.getCurrentAuctionStatus();

        assertThat(result.getStatus()).isEqualTo("NO_ACTIVE_AUCTION");
        assertThat(result.getAuctionId()).isNull();
    }

    // ─── getAuctionResults ──────────────────────────────────────────────────

    @Test
    @DisplayName("getAuctionResults: returns results with clearing info when auction exists")
    void getAuctionResults_auctionExists_returnsResultsWithClearingInfo() {
        AuctionSessionEntity clearedSessionForId = AuctionSessionEntity.builder()
            .auctionId(AUCTION_ID)
            .status("CLEARED")
            .clearingPrice(95)
            .clearingVolume(100)
            .participantCount(2)
            .createdAt(NOW.minusMinutes(30))
            .build();

        AuctionResultEntity userResult = AuctionResultEntity.builder()
            .auctionId(AUCTION_ID)
            .userId(USER_ID)
            .side("BUY")
            .clearingPrice(95)
            .bidAmount(100)
            .clearedAmount(100)
            .settlementAmount(9500)
            .createdAt(NOW)
            .build();

        when(auctionSessionRepository.findByAuctionId(AUCTION_ID))
            .thenReturn(Optional.of(clearedSessionForId));
        when(auctionResultRepository.findByAuctionId(AUCTION_ID))
            .thenReturn(List.of(userResult));

        AuctionResultDto result = auctionStatusService.getAuctionResults(AUCTION_ID);

        assertThat(result.getAuctionId()).isEqualTo(AUCTION_ID);
        assertThat(result.getClearingPrice()).isEqualTo(95);
        assertThat(result.getClearingVolume()).isEqualTo(100);
        assertThat(result.getParticipantCount()).isEqualTo(2);
        assertThat(result.getResults()).hasSize(1);

        AuctionResultDto.UserResult ur = result.getResults().get(0);
        assertThat(ur.getUserId()).isEqualTo(USER_ID.toString());
        assertThat(ur.getSide()).isEqualTo("BUY");
        assertThat(ur.getBidAmount()).isEqualTo(100);
        assertThat(ur.getClearedAmount()).isEqualTo(100);
        assertThat(ur.getStatus()).isEqualTo("CLEARED");
    }

    @Test
    @DisplayName("getAuctionResults: user result with clearedAmount=0 has status NOT_CLEARED")
    void getAuctionResults_userNotCleared_statusIsNotCleared() {
        AuctionSessionEntity session = AuctionSessionEntity.builder()
            .auctionId(AUCTION_ID)
            .status("CLEARED")
            .clearingPrice(95)
            .clearingVolume(0)
            .participantCount(1)
            .createdAt(NOW)
            .build();

        AuctionResultEntity userResult = AuctionResultEntity.builder()
            .auctionId(AUCTION_ID)
            .userId(USER_ID)
            .side("BUY")
            .clearingPrice(95)
            .bidAmount(100)
            .clearedAmount(0)
            .settlementAmount(0)
            .createdAt(NOW)
            .build();

        when(auctionSessionRepository.findByAuctionId(AUCTION_ID))
            .thenReturn(Optional.of(session));
        when(auctionResultRepository.findByAuctionId(AUCTION_ID))
            .thenReturn(List.of(userResult));

        AuctionResultDto result = auctionStatusService.getAuctionResults(AUCTION_ID);

        assertThat(result.getResults().get(0).getStatus()).isEqualTo("NOT_CLEARED");
    }

    @Test
    @DisplayName("getAuctionResults: auction not found returns empty result with auctionId")
    void getAuctionResults_auctionNotFound_returnsEmptyResult() {
        when(auctionSessionRepository.findByAuctionId("UNKNOWN")).thenReturn(Optional.empty());

        AuctionResultDto result = auctionStatusService.getAuctionResults("UNKNOWN");

        assertThat(result.getAuctionId()).isEqualTo("UNKNOWN");
        assertThat(result.getResults()).isEmpty();
        verifyNoInteractions(auctionResultRepository);
    }

    // ─── getAuctionHistory ──────────────────────────────────────────────────

    @Test
    @DisplayName("getAuctionHistory: returns cleared sessions up to limit")
    void getAuctionHistory_multipleClearedSessions_respectsLimit() {
        List<AuctionSessionEntity> clearedSessions = List.of(
            buildClearedSession("A-003", 3),
            buildClearedSession("A-002", 2)
        );

        when(auctionSessionRepository.findByStatusOrderByCreatedAtDesc(eq("CLEARED"), eq(PageRequest.of(0, 2))))
            .thenReturn(clearedSessions);

        List<AuctionStatusDto> history = auctionStatusService.getAuctionHistory(2);

        assertThat(history).hasSize(2);
        assertThat(history.get(0).getAuctionId()).isEqualTo("A-003");
        assertThat(history.get(1).getAuctionId()).isEqualTo("A-002");
    }

    @Test
    @DisplayName("getAuctionHistory: returns all when limit exceeds available sessions")
    void getAuctionHistory_limitExceedsAvailable_returnsAll() {
        when(auctionSessionRepository.findByStatusOrderByCreatedAtDesc(eq("CLEARED"), eq(PageRequest.of(0, 10))))
            .thenReturn(List.of(clearedSession));

        List<AuctionStatusDto> history = auctionStatusService.getAuctionHistory(10);

        assertThat(history).hasSize(1);
        assertThat(history.get(0).getStatus()).isEqualTo("CLEARED");
        assertThat(history.get(0).getClearingPrice()).isEqualTo(95);
    }

    @Test
    @DisplayName("getAuctionHistory: returns empty list when no cleared sessions")
    void getAuctionHistory_noClearedSessions_returnsEmpty() {
        when(auctionSessionRepository.findByStatusOrderByCreatedAtDesc(eq("CLEARED"), eq(PageRequest.of(0, 5))))
            .thenReturn(List.of());

        List<AuctionStatusDto> history = auctionStatusService.getAuctionHistory(5);

        assertThat(history).isEmpty();
    }

    // ─── helper ────────────────────────────────────────────────────────────

    private AuctionSessionEntity buildClearedSession(String auctionId, int hourOffset) {
        return AuctionSessionEntity.builder()
            .auctionId(auctionId)
            .status("CLEARED")
            .deliveryHour(NOW.minusHours(hourOffset))
            .openTime(NOW.minusHours(hourOffset + 1))
            .closeTime(NOW.minusHours(hourOffset))
            .priceFloor(50)
            .priceCeiling(200)
            .clearingPrice(90 + hourOffset)
            .clearingVolume(100)
            .participantCount(2)
            .createdAt(NOW.minusHours(hourOffset + 1))
            .build();
    }
}
