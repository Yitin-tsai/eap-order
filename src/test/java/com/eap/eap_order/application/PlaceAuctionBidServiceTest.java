package com.eap.eap_order.application;

import com.eap.common.dto.AuctionBidRequest;
import com.eap.common.dto.AuctionBidResponse;
import com.eap.eap_order.configuration.repository.AuctionBidRepository;
import com.eap.eap_order.domain.entity.AuctionBidEntity;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class PlaceAuctionBidServiceTest {

    @Mock
    private RabbitTemplate rabbitTemplate;

    @Mock
    private AuctionBidRepository auctionBidRepository;

    @Spy
    private ObjectMapper objectMapper = new ObjectMapper();

    @InjectMocks
    private PlaceAuctionBidService placeAuctionBidService;

    private static final String AUCTION_ID = "AUCTION-2026-001";
    private static final String USER_ID = "550e8400-e29b-41d4-a716-446655440000";

    private AuctionBidRequest validBuyRequest;
    private AuctionBidRequest validSellRequest;

    @BeforeEach
    void setUp() {
        validBuyRequest = AuctionBidRequest.builder()
            .userId(USER_ID)
            .auctionId(AUCTION_ID)
            .side("BUY")
            .steps(List.of(
                new AuctionBidRequest.BidStep(100, 10),
                new AuctionBidRequest.BidStep(90, 20)
            ))
            .build();

        validSellRequest = AuctionBidRequest.builder()
            .userId(USER_ID)
            .auctionId(AUCTION_ID)
            .side("SELL")
            .steps(List.of(
                new AuctionBidRequest.BidStep(80, 15),
                new AuctionBidRequest.BidStep(70, 25)
            ))
            .build();
    }

    @Test
    @DisplayName("BUY bid: successful flow should validate, persist, and publish event (no Feign to matchEngine)")
    void submitBid_buyBid_successFlow() {
        when(auctionBidRepository.existsByAuctionIdAndUserId(anyString(), any(UUID.class))).thenReturn(false);
        when(auctionBidRepository.save(any())).thenReturn(new AuctionBidEntity());

        AuctionBidResponse result = placeAuctionBidService.submitBid(validBuyRequest);

        assertThat(result.isSuccess()).isTrue();
        verify(auctionBidRepository).save(any(AuctionBidEntity.class));
        verify(rabbitTemplate).convertAndSend(anyString(), anyString(), any(Object.class));
    }

    @Test
    @DisplayName("BUY totalLocked = sum(price * amount) for each step")
    void submitBid_buyBid_totalLockedCalculation() {
        // step1: 100*10 = 1000, step2: 90*20 = 1800 → total = 2800
        when(auctionBidRepository.existsByAuctionIdAndUserId(anyString(), any(UUID.class))).thenReturn(false);
        when(auctionBidRepository.save(any())).thenReturn(new AuctionBidEntity());

        AuctionBidResponse result = placeAuctionBidService.submitBid(validBuyRequest);

        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getTotalLocked()).isEqualTo(2800);

        ArgumentCaptor<AuctionBidEntity> entityCaptor = ArgumentCaptor.forClass(AuctionBidEntity.class);
        verify(auctionBidRepository).save(entityCaptor.capture());
        assertThat(entityCaptor.getValue().getTotalLocked()).isEqualTo(2800);
    }

    @Test
    @DisplayName("SELL totalLocked = sum(amount) for each step")
    void submitBid_sellBid_totalLockedCalculation() {
        // step1: 15, step2: 25 → total = 40
        when(auctionBidRepository.existsByAuctionIdAndUserId(anyString(), any(UUID.class))).thenReturn(false);
        when(auctionBidRepository.save(any())).thenReturn(new AuctionBidEntity());

        AuctionBidResponse result = placeAuctionBidService.submitBid(validSellRequest);

        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getTotalLocked()).isEqualTo(40);

        ArgumentCaptor<AuctionBidEntity> entityCaptor = ArgumentCaptor.forClass(AuctionBidEntity.class);
        verify(auctionBidRepository).save(entityCaptor.capture());
        assertThat(entityCaptor.getValue().getTotalLocked()).isEqualTo(40);
    }

    // ─── Duplicate bid prevention (RISK-5) ─────────────────────────────────

    @Test
    @DisplayName("RISK-5: duplicate bid from same user in same auction should be rejected regardless of side")
    void submitBid_duplicateBid_sameUser_sameAuction_returnsFailure() {
        when(auctionBidRepository.existsByAuctionIdAndUserId(AUCTION_ID, UUID.fromString(USER_ID)))
            .thenReturn(true);

        AuctionBidResponse result = placeAuctionBidService.submitBid(validBuyRequest);

        assertThat(result.isSuccess()).isFalse();
        assertThat(result.getMessage()).contains("already submitted a bid");
        verifyNoMoreInteractions(rabbitTemplate);
        verify(auctionBidRepository, never()).save(any());
    }

    @Test
    @DisplayName("RISK-5: user who submitted BUY cannot submit SELL for the same auction")
    void submitBid_duplicateBid_buyThenSell_returnsFailure() {
        // User already has a BUY bid → now tries to submit SELL
        when(auctionBidRepository.existsByAuctionIdAndUserId(AUCTION_ID, UUID.fromString(USER_ID)))
            .thenReturn(true);

        AuctionBidResponse result = placeAuctionBidService.submitBid(validSellRequest);

        assertThat(result.isSuccess()).isFalse();
        assertThat(result.getMessage()).contains("already submitted a bid");
        verifyNoMoreInteractions(rabbitTemplate);
        verify(auctionBidRepository, never()).save(any());
    }

    @Test
    @DisplayName("RISK-5: different user in same auction is allowed to submit bid")
    void submitBid_differentUser_sameAuction_isAllowed() {
        String otherUserId = "660e8400-e29b-41d4-a716-446655440001";
        AuctionBidRequest otherUserRequest = AuctionBidRequest.builder()
            .userId(otherUserId)
            .auctionId(AUCTION_ID)
            .side("BUY")
            .steps(List.of(new AuctionBidRequest.BidStep(100, 5)))
            .build();

        when(auctionBidRepository.existsByAuctionIdAndUserId(AUCTION_ID, UUID.fromString(otherUserId)))
            .thenReturn(false);
        when(auctionBidRepository.save(any())).thenReturn(new AuctionBidEntity());

        AuctionBidResponse result = placeAuctionBidService.submitBid(otherUserRequest);

        assertThat(result.isSuccess()).isTrue();
        verify(auctionBidRepository).save(any(AuctionBidEntity.class));
    }

    @Test
    @DisplayName("RISK-5: same user in different auction is allowed to submit bid")
    void submitBid_sameUser_differentAuction_isAllowed() {
        String otherAuctionId = "AUCTION-2026-002";
        AuctionBidRequest otherAuctionRequest = AuctionBidRequest.builder()
            .userId(USER_ID)
            .auctionId(otherAuctionId)
            .side("BUY")
            .steps(List.of(new AuctionBidRequest.BidStep(100, 5)))
            .build();

        when(auctionBidRepository.existsByAuctionIdAndUserId(otherAuctionId, UUID.fromString(USER_ID)))
            .thenReturn(false);
        when(auctionBidRepository.save(any())).thenReturn(new AuctionBidEntity());

        AuctionBidResponse result = placeAuctionBidService.submitBid(otherAuctionRequest);

        assertThat(result.isSuccess()).isTrue();
        verify(auctionBidRepository).save(any(AuctionBidEntity.class));
    }

    @Test
    @DisplayName("RISK-5: duplicate check uses correct auctionId and userId")
    void submitBid_duplicateCheck_usesCorrectParameters() {
        when(auctionBidRepository.existsByAuctionIdAndUserId(anyString(), any(UUID.class))).thenReturn(false);
        when(auctionBidRepository.save(any())).thenReturn(new AuctionBidEntity());

        placeAuctionBidService.submitBid(validBuyRequest);

        verify(auctionBidRepository).existsByAuctionIdAndUserId(AUCTION_ID, UUID.fromString(USER_ID));
    }

    @Test
    @DisplayName("Invalid request (missing userId) should return failure without persisting or publishing")
    void submitBid_invalidRequest_missingUserId_returnsFailure() {
        AuctionBidRequest invalidRequest = AuctionBidRequest.builder()
            .userId("")
            .auctionId(AUCTION_ID)
            .side("BUY")
            .steps(List.of(new AuctionBidRequest.BidStep(100, 10)))
            .build();

        AuctionBidResponse result = placeAuctionBidService.submitBid(invalidRequest);

        assertThat(result.isSuccess()).isFalse();
        assertThat(result.getMessage()).isNotBlank();
        verifyNoInteractions(auctionBidRepository);
        verifyNoInteractions(rabbitTemplate);
    }

    @Test
    @DisplayName("Invalid request (invalid side) should return failure")
    void submitBid_invalidRequest_invalidSide_returnsFailure() {
        AuctionBidRequest invalidRequest = AuctionBidRequest.builder()
            .userId(USER_ID)
            .auctionId(AUCTION_ID)
            .side("INVALID")
            .steps(List.of(new AuctionBidRequest.BidStep(100, 10)))
            .build();

        AuctionBidResponse result = placeAuctionBidService.submitBid(invalidRequest);

        assertThat(result.isSuccess()).isFalse();
        verifyNoInteractions(auctionBidRepository);
    }

    @Test
    @DisplayName("Invalid request (empty steps) should return failure")
    void submitBid_invalidRequest_emptySteps_returnsFailure() {
        AuctionBidRequest invalidRequest = AuctionBidRequest.builder()
            .userId(USER_ID)
            .auctionId(AUCTION_ID)
            .side("BUY")
            .steps(List.of())
            .build();

        AuctionBidResponse result = placeAuctionBidService.submitBid(invalidRequest);

        assertThat(result.isSuccess()).isFalse();
        verifyNoInteractions(auctionBidRepository);
    }

    @Test
    @DisplayName("Persisted entity should have correct fields")
    void submitBid_persistedEntity_hasCorrectFields() {
        when(auctionBidRepository.existsByAuctionIdAndUserId(anyString(), any(UUID.class))).thenReturn(false);
        when(auctionBidRepository.save(any())).thenReturn(new AuctionBidEntity());

        placeAuctionBidService.submitBid(validBuyRequest);

        ArgumentCaptor<AuctionBidEntity> captor = ArgumentCaptor.forClass(AuctionBidEntity.class);
        verify(auctionBidRepository).save(captor.capture());
        AuctionBidEntity saved = captor.getValue();

        assertThat(saved.getAuctionId()).isEqualTo(AUCTION_ID);
        assertThat(saved.getUserId()).isEqualTo(UUID.fromString(USER_ID));
        assertThat(saved.getSide()).isEqualTo("BUY");
        assertThat(saved.getStatus()).isEqualTo("SUBMITTED");
        assertThat(saved.getSteps()).isNotBlank();
        assertThat(saved.getCreatedAt()).isNotNull();
    }

    @Test
    @DisplayName("Side value in persisted entity is uppercased")
    void submitBid_sideStoredUppercase() {
        AuctionBidRequest lowerCaseRequest = AuctionBidRequest.builder()
            .userId(USER_ID)
            .auctionId(AUCTION_ID)
            .side("buy")
            .steps(List.of(new AuctionBidRequest.BidStep(100, 10)))
            .build();

        when(auctionBidRepository.existsByAuctionIdAndUserId(anyString(), any(UUID.class))).thenReturn(false);
        when(auctionBidRepository.save(any())).thenReturn(new AuctionBidEntity());

        AuctionBidResponse result = placeAuctionBidService.submitBid(lowerCaseRequest);

        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getSide()).isEqualTo("BUY");

        ArgumentCaptor<AuctionBidEntity> captor = ArgumentCaptor.forClass(AuctionBidEntity.class);
        verify(auctionBidRepository).save(captor.capture());
        assertThat(captor.getValue().getSide()).isEqualTo("BUY");
    }
}
