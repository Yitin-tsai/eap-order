package com.eap.eap_order.application;

import com.eap.common.dto.AuctionBidRequest;
import com.eap.common.dto.AuctionBidResponse;
import com.eap.eap_order.application.OutBound.EapMatchEngine;
import com.eap.eap_order.configuration.repository.AuctionBidRepository;
import com.eap.eap_order.domain.entity.AuctionBidEntity;
import com.fasterxml.jackson.core.JsonProcessingException;
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
import org.springframework.http.ResponseEntity;

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

    @Mock
    private EapMatchEngine eapMatchEngine;

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
    @DisplayName("BUY bid: successful flow should validate, forward to matchEngine, persist, and publish event")
    void submitBid_buyBid_successFlow() {
        AuctionBidResponse matchEngineSuccess = AuctionBidResponse.success(AUCTION_ID, USER_ID, "BUY", 0);
        when(eapMatchEngine.submitAuctionBid(any())).thenReturn(ResponseEntity.ok(matchEngineSuccess));
        when(auctionBidRepository.save(any())).thenReturn(new AuctionBidEntity());

        AuctionBidResponse result = placeAuctionBidService.submitBid(validBuyRequest);

        assertThat(result.isSuccess()).isTrue();
        verify(eapMatchEngine).submitAuctionBid(validBuyRequest);
        verify(auctionBidRepository).save(any(AuctionBidEntity.class));
        verify(rabbitTemplate).convertAndSend(anyString(), anyString(), any(Object.class));
    }

    @Test
    @DisplayName("BUY totalLocked = sum(price * amount) for each step")
    void submitBid_buyBid_totalLockedCalculation() {
        // step1: 100*10 = 1000, step2: 90*20 = 1800 → total = 2800
        AuctionBidResponse matchEngineSuccess = AuctionBidResponse.success(AUCTION_ID, USER_ID, "BUY", 2800);
        when(eapMatchEngine.submitAuctionBid(any())).thenReturn(ResponseEntity.ok(matchEngineSuccess));
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
        AuctionBidResponse matchEngineSuccess = AuctionBidResponse.success(AUCTION_ID, USER_ID, "SELL", 40);
        when(eapMatchEngine.submitAuctionBid(any())).thenReturn(ResponseEntity.ok(matchEngineSuccess));
        when(auctionBidRepository.save(any())).thenReturn(new AuctionBidEntity());

        AuctionBidResponse result = placeAuctionBidService.submitBid(validSellRequest);

        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getTotalLocked()).isEqualTo(40);

        ArgumentCaptor<AuctionBidEntity> entityCaptor = ArgumentCaptor.forClass(AuctionBidEntity.class);
        verify(auctionBidRepository).save(entityCaptor.capture());
        assertThat(entityCaptor.getValue().getTotalLocked()).isEqualTo(40);
    }

    @Test
    @DisplayName("Invalid request (missing userId) should return failure without calling matchEngine")
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
        verifyNoInteractions(eapMatchEngine);
        verifyNoInteractions(auctionBidRepository);
        verifyNoInteractions(rabbitTemplate);
    }

    @Test
    @DisplayName("Invalid request (invalid side) should return failure without calling matchEngine")
    void submitBid_invalidRequest_invalidSide_returnsFailure() {
        AuctionBidRequest invalidRequest = AuctionBidRequest.builder()
            .userId(USER_ID)
            .auctionId(AUCTION_ID)
            .side("INVALID")
            .steps(List.of(new AuctionBidRequest.BidStep(100, 10)))
            .build();

        AuctionBidResponse result = placeAuctionBidService.submitBid(invalidRequest);

        assertThat(result.isSuccess()).isFalse();
        verifyNoInteractions(eapMatchEngine);
    }

    @Test
    @DisplayName("Invalid request (empty steps) should return failure without calling matchEngine")
    void submitBid_invalidRequest_emptySteps_returnsFailure() {
        AuctionBidRequest invalidRequest = AuctionBidRequest.builder()
            .userId(USER_ID)
            .auctionId(AUCTION_ID)
            .side("BUY")
            .steps(List.of())
            .build();

        AuctionBidResponse result = placeAuctionBidService.submitBid(invalidRequest);

        assertThat(result.isSuccess()).isFalse();
        verifyNoInteractions(eapMatchEngine);
    }

    @Test
    @DisplayName("MatchEngine returns failure should return failure without persisting or publishing event")
    void submitBid_matchEngineReturnsFailure_returnsFailure() {
        AuctionBidResponse matchEngineFailure = AuctionBidResponse.failure("Auction not open");
        when(eapMatchEngine.submitAuctionBid(any())).thenReturn(ResponseEntity.ok(matchEngineFailure));

        AuctionBidResponse result = placeAuctionBidService.submitBid(validBuyRequest);

        assertThat(result.isSuccess()).isFalse();
        assertThat(result.getMessage()).isEqualTo("Auction not open");
        verifyNoInteractions(auctionBidRepository);
        verifyNoInteractions(rabbitTemplate);
    }

    @Test
    @DisplayName("MatchEngine returns null body should return failure")
    void submitBid_matchEngineReturnsNullBody_returnsFailure() {
        when(eapMatchEngine.submitAuctionBid(any())).thenReturn(ResponseEntity.ok(null));

        AuctionBidResponse result = placeAuctionBidService.submitBid(validBuyRequest);

        assertThat(result.isSuccess()).isFalse();
        assertThat(result.getMessage()).contains("MatchEngine returned empty response");
        verifyNoInteractions(auctionBidRepository);
        verifyNoInteractions(rabbitTemplate);
    }

    @Test
    @DisplayName("MatchEngine throws exception should return failure")
    void submitBid_matchEngineThrowsException_returnsFailure() {
        when(eapMatchEngine.submitAuctionBid(any())).thenThrow(new RuntimeException("Connection refused"));

        AuctionBidResponse result = placeAuctionBidService.submitBid(validBuyRequest);

        assertThat(result.isSuccess()).isFalse();
        assertThat(result.getMessage()).contains("Failed to submit bid to matching engine");
        verifyNoInteractions(auctionBidRepository);
        verifyNoInteractions(rabbitTemplate);
    }

    @Test
    @DisplayName("Persisted entity should have correct fields")
    void submitBid_persistedEntity_hasCorrectFields() {
        AuctionBidResponse matchEngineSuccess = AuctionBidResponse.success(AUCTION_ID, USER_ID, "BUY", 2800);
        when(eapMatchEngine.submitAuctionBid(any())).thenReturn(ResponseEntity.ok(matchEngineSuccess));
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

        AuctionBidResponse matchEngineSuccess = AuctionBidResponse.success(AUCTION_ID, USER_ID, "BUY", 1000);
        when(eapMatchEngine.submitAuctionBid(any())).thenReturn(ResponseEntity.ok(matchEngineSuccess));
        when(auctionBidRepository.save(any())).thenReturn(new AuctionBidEntity());

        AuctionBidResponse result = placeAuctionBidService.submitBid(lowerCaseRequest);

        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getSide()).isEqualTo("BUY");

        ArgumentCaptor<AuctionBidEntity> captor = ArgumentCaptor.forClass(AuctionBidEntity.class);
        verify(auctionBidRepository).save(captor.capture());
        assertThat(captor.getValue().getSide()).isEqualTo("BUY");
    }
}
