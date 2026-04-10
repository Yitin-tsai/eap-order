package com.eap.eap_order.application;

import com.eap.common.event.AuctionClearedEvent;
import com.eap.common.event.AuctionCreatedEvent;
import com.eap.eap_order.configuration.repository.AuctionBidRepository;
import com.eap.eap_order.configuration.repository.AuctionResultRepository;
import com.eap.eap_order.configuration.repository.AuctionSessionRepository;
import com.eap.eap_order.domain.entity.AuctionBidEntity;
import com.eap.eap_order.domain.entity.AuctionResultEntity;
import com.eap.eap_order.domain.entity.AuctionSessionEntity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.messaging.simp.SimpMessagingTemplate;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AuctionResultListenerTest {

    @Mock
    private AuctionSessionRepository auctionSessionRepository;

    @Mock
    private AuctionBidRepository auctionBidRepository;

    @Mock
    private AuctionResultRepository auctionResultRepository;

    @Mock
    private SimpMessagingTemplate messagingTemplate;

    @InjectMocks
    private AuctionResultListener auctionResultListener;

    private static final String AUCTION_ID = "AUCTION-2026-001";
    private static final UUID USER_ID = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");

    private AuctionSessionEntity existingSession;

    @BeforeEach
    void setUp() {
        existingSession = AuctionSessionEntity.builder()
            .id(1L)
            .auctionId(AUCTION_ID)
            .status("OPEN")
            .deliveryHour(LocalDateTime.now().plusHours(2))
            .openTime(LocalDateTime.now().minusMinutes(10))
            .closeTime(LocalDateTime.now().plusMinutes(20))
            .priceFloor(50)
            .priceCeiling(200)
            .participantCount(0)
            .createdAt(LocalDateTime.now())
            .build();
    }

    // ─── handleAuctionCleared ───────────────────────────────────────────────

    @Test
    @DisplayName("handleAuctionCleared: updates session status, saves results, updates bid statuses, and pushes WebSocket")
    void handleAuctionCleared_fullyClearedBid_updatesSessionAndResults() {
        AuctionClearedEvent.AuctionBidResult result = new AuctionClearedEvent.AuctionBidResult(
            USER_ID, "BUY", 100, 100, 9500, 10000);

        AuctionClearedEvent event = AuctionClearedEvent.builder()
            .auctionId(AUCTION_ID)
            .clearingPrice(95)
            .clearingVolume(100)
            .status("CLEARED")
            .results(List.of(result))
            .clearedAt(LocalDateTime.now())
            .build();

        AuctionBidEntity bid = AuctionBidEntity.builder()
            .auctionId(AUCTION_ID)
            .userId(USER_ID)
            .side("BUY")
            .status("SUBMITTED")
            .build();

        when(auctionSessionRepository.findByAuctionId(AUCTION_ID)).thenReturn(Optional.of(existingSession));
        when(auctionSessionRepository.save(any())).thenReturn(existingSession);
        when(auctionBidRepository.findByAuctionId(AUCTION_ID)).thenReturn(List.of(bid));
        when(auctionBidRepository.saveAll(anyList())).thenReturn(List.of(bid));
        when(auctionResultRepository.saveAll(anyList())).thenReturn(List.of(new AuctionResultEntity()));

        auctionResultListener.handleAuctionCleared(event);

        // Session updated
        ArgumentCaptor<AuctionSessionEntity> sessionCaptor = ArgumentCaptor.forClass(AuctionSessionEntity.class);
        verify(auctionSessionRepository).save(sessionCaptor.capture());
        AuctionSessionEntity savedSession = sessionCaptor.getValue();
        assertThat(savedSession.getClearingPrice()).isEqualTo(95);
        assertThat(savedSession.getClearingVolume()).isEqualTo(100);
        assertThat(savedSession.getStatus()).isEqualTo("CLEARED");
        assertThat(savedSession.getParticipantCount()).isEqualTo(1);

        // Results saved via saveAll
        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<AuctionResultEntity>> resultCaptor = ArgumentCaptor.forClass(List.class);
        verify(auctionResultRepository).saveAll(resultCaptor.capture());
        List<AuctionResultEntity> savedResults = resultCaptor.getValue();
        assertThat(savedResults).hasSize(1);
        assertThat(savedResults.get(0).getAuctionId()).isEqualTo(AUCTION_ID);
        assertThat(savedResults.get(0).getUserId()).isEqualTo(USER_ID);
        assertThat(savedResults.get(0).getSide()).isEqualTo("BUY");
        assertThat(savedResults.get(0).getClearingPrice()).isEqualTo(95);
        assertThat(savedResults.get(0).getBidAmount()).isEqualTo(100);
        assertThat(savedResults.get(0).getClearedAmount()).isEqualTo(100);

        // Bid status updated to CLEARED (clearedAmount >= bidAmount)
        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<AuctionBidEntity>> bidCaptor = ArgumentCaptor.forClass(List.class);
        verify(auctionBidRepository).saveAll(bidCaptor.capture());
        assertThat(bidCaptor.getValue().get(0).getStatus()).isEqualTo("CLEARED");

        // WebSocket push
        verify(messagingTemplate).convertAndSend(eq("/topic/auction/result"), any(Object.class));
    }

    @Test
    @DisplayName("handleAuctionCleared: partially cleared bid gets status PARTIAL")
    void handleAuctionCleared_partiallyClearedBid_statusPartial() {
        AuctionClearedEvent.AuctionBidResult result = new AuctionClearedEvent.AuctionBidResult(
            USER_ID, "BUY", 100, 50, 4750, 10000);

        AuctionClearedEvent event = AuctionClearedEvent.builder()
            .auctionId(AUCTION_ID)
            .clearingPrice(95)
            .clearingVolume(50)
            .status("CLEARED")
            .results(List.of(result))
            .clearedAt(LocalDateTime.now())
            .build();

        AuctionBidEntity bid = AuctionBidEntity.builder()
            .auctionId(AUCTION_ID)
            .userId(USER_ID)
            .side("BUY")
            .status("SUBMITTED")
            .build();

        when(auctionSessionRepository.findByAuctionId(AUCTION_ID)).thenReturn(Optional.of(existingSession));
        when(auctionSessionRepository.save(any())).thenReturn(existingSession);
        when(auctionBidRepository.findByAuctionId(AUCTION_ID)).thenReturn(List.of(bid));
        when(auctionBidRepository.saveAll(anyList())).thenReturn(List.of(bid));
        when(auctionResultRepository.saveAll(anyList())).thenReturn(List.of(new AuctionResultEntity()));

        auctionResultListener.handleAuctionCleared(event);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<AuctionBidEntity>> bidCaptor = ArgumentCaptor.forClass(List.class);
        verify(auctionBidRepository).saveAll(bidCaptor.capture());
        assertThat(bidCaptor.getValue().get(0).getStatus()).isEqualTo("PARTIAL");
    }

    @Test
    @DisplayName("handleAuctionCleared: not-cleared bid gets status NOT_CLEARED")
    void handleAuctionCleared_notClearedBid_statusNotCleared() {
        AuctionClearedEvent.AuctionBidResult result = new AuctionClearedEvent.AuctionBidResult(
            USER_ID, "BUY", 100, 0, 0, 10000);

        AuctionClearedEvent event = AuctionClearedEvent.builder()
            .auctionId(AUCTION_ID)
            .clearingPrice(95)
            .clearingVolume(0)
            .status("CLEARED")
            .results(List.of(result))
            .clearedAt(LocalDateTime.now())
            .build();

        AuctionBidEntity bid = AuctionBidEntity.builder()
            .auctionId(AUCTION_ID)
            .userId(USER_ID)
            .side("BUY")
            .status("SUBMITTED")
            .build();

        when(auctionSessionRepository.findByAuctionId(AUCTION_ID)).thenReturn(Optional.of(existingSession));
        when(auctionSessionRepository.save(any())).thenReturn(existingSession);
        when(auctionBidRepository.findByAuctionId(AUCTION_ID)).thenReturn(List.of(bid));
        when(auctionBidRepository.saveAll(anyList())).thenReturn(List.of(bid));
        when(auctionResultRepository.saveAll(anyList())).thenReturn(List.of(new AuctionResultEntity()));

        auctionResultListener.handleAuctionCleared(event);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<AuctionBidEntity>> bidCaptor = ArgumentCaptor.forClass(List.class);
        verify(auctionBidRepository).saveAll(bidCaptor.capture());
        assertThat(bidCaptor.getValue().get(0).getStatus()).isEqualTo("NOT_CLEARED");
    }

    @Test
    @DisplayName("handleAuctionCleared: session not found should log error and return without saving")
    void handleAuctionCleared_sessionNotFound_doesNothing() {
        AuctionClearedEvent event = AuctionClearedEvent.builder()
            .auctionId("UNKNOWN-AUCTION")
            .clearingPrice(95)
            .clearingVolume(100)
            .status("CLEARED")
            .results(List.of())
            .clearedAt(LocalDateTime.now())
            .build();

        when(auctionSessionRepository.findByAuctionId("UNKNOWN-AUCTION")).thenReturn(Optional.empty());

        auctionResultListener.handleAuctionCleared(event);

        verify(auctionSessionRepository, never()).save(any());
        verifyNoInteractions(auctionResultRepository);
        verifyNoInteractions(auctionBidRepository);
        verifyNoInteractions(messagingTemplate);
    }

    @Test
    @DisplayName("handleAuctionCleared: null results list should update session and push WebSocket without saving result records")
    void handleAuctionCleared_nullResults_updatesSessionOnly() {
        AuctionClearedEvent event = AuctionClearedEvent.builder()
            .auctionId(AUCTION_ID)
            .clearingPrice(95)
            .clearingVolume(0)
            .status("FAILED")
            .results(null)
            .clearedAt(LocalDateTime.now())
            .build();

        when(auctionSessionRepository.findByAuctionId(AUCTION_ID)).thenReturn(Optional.of(existingSession));
        when(auctionSessionRepository.save(any())).thenReturn(existingSession);

        auctionResultListener.handleAuctionCleared(event);

        verify(auctionSessionRepository).save(any());
        verifyNoInteractions(auctionResultRepository);
        verifyNoInteractions(auctionBidRepository);
        verify(messagingTemplate).convertAndSend(eq("/topic/auction/result"), any(Object.class));
    }

    // ─── handleAuctionCreated ───────────────────────────────────────────────

    @Test
    @DisplayName("handleAuctionCreated: creates session with OPEN status and pushes WebSocket")
    void handleAuctionCreated_validEvent_createsSessionAndNotifiesWebSocket() {
        LocalDateTime now = LocalDateTime.now();
        AuctionCreatedEvent event = AuctionCreatedEvent.builder()
            .auctionId(AUCTION_ID)
            .deliveryHour(now.plusHours(3))
            .openTime(now)
            .closeTime(now.plusMinutes(30))
            .priceFloor(50)
            .priceCeiling(200)
            .createdAt(now)
            .build();

        when(auctionSessionRepository.save(any())).thenReturn(existingSession);

        auctionResultListener.handleAuctionCreated(event);

        ArgumentCaptor<AuctionSessionEntity> captor = ArgumentCaptor.forClass(AuctionSessionEntity.class);
        verify(auctionSessionRepository).save(captor.capture());

        AuctionSessionEntity saved = captor.getValue();
        assertThat(saved.getAuctionId()).isEqualTo(AUCTION_ID);
        assertThat(saved.getStatus()).isEqualTo("OPEN");
        assertThat(saved.getPriceFloor()).isEqualTo(50);
        assertThat(saved.getPriceCeiling()).isEqualTo(200);
        assertThat(saved.getParticipantCount()).isEqualTo(0);
        assertThat(saved.getCreatedAt()).isEqualTo(now);

        verify(messagingTemplate).convertAndSend(eq("/topic/auction/status"), any(Object.class));
    }

    @Test
    @DisplayName("handleAuctionCreated: event with null createdAt uses current time")
    void handleAuctionCreated_nullCreatedAt_usesCurrentTime() {
        LocalDateTime now = LocalDateTime.now();
        AuctionCreatedEvent event = AuctionCreatedEvent.builder()
            .auctionId(AUCTION_ID)
            .deliveryHour(now.plusHours(3))
            .openTime(now)
            .closeTime(now.plusMinutes(30))
            .priceFloor(50)
            .priceCeiling(200)
            .createdAt(null)
            .build();

        when(auctionSessionRepository.save(any())).thenReturn(existingSession);

        auctionResultListener.handleAuctionCreated(event);

        ArgumentCaptor<AuctionSessionEntity> captor = ArgumentCaptor.forClass(AuctionSessionEntity.class);
        verify(auctionSessionRepository).save(captor.capture());
        // createdAt should not be null when event.createdAt is null
        assertThat(captor.getValue().getCreatedAt()).isNotNull();
    }
}
