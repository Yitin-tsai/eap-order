package com.eap.eap_order.controller;

import com.eap.common.dto.AuctionBidResponse;
import com.eap.common.dto.AuctionResultDto;
import com.eap.common.dto.AuctionStatusDto;
import com.eap.eap_order.application.AuctionStatusService;
import com.eap.eap_order.application.PlaceAuctionBidService;
import com.eap.eap_order.controller.dto.req.PlaceAuctionBidReq;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AuctionControllerTest {

    @Mock
    private PlaceAuctionBidService placeAuctionBidService;

    @Mock
    private AuctionStatusService auctionStatusService;

    @InjectMocks
    private AuctionController auctionController;

    private static final String AUCTION_ID = "AUCTION-2026-001";
    private static final UUID USER_ID = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");

    private PlaceAuctionBidReq validBidReq;

    @BeforeEach
    void setUp() {
        validBidReq = PlaceAuctionBidReq.builder()
            .userId(USER_ID)
            .auctionId(AUCTION_ID)
            .side("BUY")
            .steps(List.of(
                new PlaceAuctionBidReq.Step(100, 10),
                new PlaceAuctionBidReq.Step(90, 20)
            ))
            .build();
    }

    // ─── POST /bid/auction ──────────────────────────────────────────────────

    @Test
    @DisplayName("submitBid: successful bid returns 200 OK with success response")
    void submitBid_success_returns200() {
        AuctionBidResponse successResponse = AuctionBidResponse.success(AUCTION_ID, USER_ID.toString(), "BUY", 2800);
        when(placeAuctionBidService.submitBid(any())).thenReturn(successResponse);

        ResponseEntity<AuctionBidResponse> response = auctionController.submitBid(validBidReq);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().isSuccess()).isTrue();
        assertThat(response.getBody().getAuctionId()).isEqualTo(AUCTION_ID);
        assertThat(response.getBody().getTotalLocked()).isEqualTo(2800);
    }

    @Test
    @DisplayName("submitBid: failed bid returns 400 Bad Request with failure response")
    void submitBid_failure_returns400() {
        AuctionBidResponse failureResponse = AuctionBidResponse.failure("Auction not open");
        when(placeAuctionBidService.submitBid(any())).thenReturn(failureResponse);

        ResponseEntity<AuctionBidResponse> response = auctionController.submitBid(validBidReq);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().isSuccess()).isFalse();
        assertThat(response.getBody().getMessage()).isEqualTo("Auction not open");
    }

    @Test
    @DisplayName("submitBid: controller converts PlaceAuctionBidReq to AuctionBidRequest correctly")
    void submitBid_convertsRequestCorrectly() {
        AuctionBidResponse successResponse = AuctionBidResponse.success(AUCTION_ID, USER_ID.toString(), "BUY", 2800);
        when(placeAuctionBidService.submitBid(any())).thenReturn(successResponse);

        auctionController.submitBid(validBidReq);

        // Verify the service was called with converted request
        verify(placeAuctionBidService).submitBid(argThat(req ->
            req.getUserId().equals(USER_ID.toString()) &&
            req.getAuctionId().equals(AUCTION_ID) &&
            req.getSide().equals("BUY") &&
            req.getSteps().size() == 2 &&
            req.getSteps().get(0).getPrice() == 100 &&
            req.getSteps().get(0).getAmount() == 10
        ));
    }

    // ─── GET /bid/auction/status ────────────────────────────────────────────

    @Test
    @DisplayName("getAuctionStatus: returns 200 OK with current auction status")
    void getAuctionStatus_returns200WithStatus() {
        AuctionStatusDto statusDto = AuctionStatusDto.builder()
            .auctionId(AUCTION_ID)
            .status("OPEN")
            .participantCount(5)
            .priceFloor(50)
            .priceCeiling(200)
            .openTime(LocalDateTime.now().minusMinutes(10))
            .closeTime(LocalDateTime.now().plusMinutes(20))
            .build();
        when(auctionStatusService.getCurrentAuctionStatus()).thenReturn(statusDto);

        ResponseEntity<AuctionStatusDto> response = auctionController.getAuctionStatus();

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().getStatus()).isEqualTo("OPEN");
        assertThat(response.getBody().getAuctionId()).isEqualTo(AUCTION_ID);
        assertThat(response.getBody().getParticipantCount()).isEqualTo(5);
    }

    @Test
    @DisplayName("getAuctionStatus: returns NO_ACTIVE_AUCTION status when no active auction")
    void getAuctionStatus_noActiveAuction_returnsNoActiveStatus() {
        AuctionStatusDto noAuctionDto = AuctionStatusDto.builder()
            .status("NO_ACTIVE_AUCTION")
            .build();
        when(auctionStatusService.getCurrentAuctionStatus()).thenReturn(noAuctionDto);

        ResponseEntity<AuctionStatusDto> response = auctionController.getAuctionStatus();

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody().getStatus()).isEqualTo("NO_ACTIVE_AUCTION");
    }

    // ─── GET /bid/auction/results ───────────────────────────────────────────

    @Test
    @DisplayName("getAuctionResults: returns 200 OK with auction results for given auctionId")
    void getAuctionResults_returns200WithResults() {
        AuctionResultDto resultDto = AuctionResultDto.builder()
            .auctionId(AUCTION_ID)
            .clearingPrice(95)
            .clearingVolume(100)
            .participantCount(2)
            .results(List.of(
                AuctionResultDto.UserResult.builder()
                    .userId(USER_ID.toString())
                    .side("BUY")
                    .bidAmount(100)
                    .clearedAmount(100)
                    .settlementAmount(9500)
                    .status("CLEARED")
                    .build()
            ))
            .clearedAt(LocalDateTime.now())
            .build();
        when(auctionStatusService.getAuctionResults(AUCTION_ID)).thenReturn(resultDto);

        ResponseEntity<AuctionResultDto> response = auctionController.getAuctionResults(AUCTION_ID);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().getAuctionId()).isEqualTo(AUCTION_ID);
        assertThat(response.getBody().getClearingPrice()).isEqualTo(95);
        assertThat(response.getBody().getResults()).hasSize(1);
        verify(auctionStatusService).getAuctionResults(AUCTION_ID);
    }

    @Test
    @DisplayName("getAuctionResults: returns empty results when auction not found")
    void getAuctionResults_auctionNotFound_returnsEmptyResults() {
        AuctionResultDto emptyResult = AuctionResultDto.builder()
            .auctionId("UNKNOWN")
            .results(List.of())
            .build();
        when(auctionStatusService.getAuctionResults("UNKNOWN")).thenReturn(emptyResult);

        ResponseEntity<AuctionResultDto> response = auctionController.getAuctionResults("UNKNOWN");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody().getResults()).isEmpty();
    }

    // ─── GET /bid/auction/history ───────────────────────────────────────────

    @Test
    @DisplayName("getAuctionHistory: returns 200 OK with list of cleared auctions")
    void getAuctionHistory_returns200WithHistory() {
        List<AuctionStatusDto> history = List.of(
            AuctionStatusDto.builder().auctionId("A-002").status("CLEARED").clearingPrice(95).build(),
            AuctionStatusDto.builder().auctionId("A-001").status("CLEARED").clearingPrice(90).build()
        );
        when(auctionStatusService.getAuctionHistory(10)).thenReturn(history);

        ResponseEntity<List<AuctionStatusDto>> response = auctionController.getAuctionHistory(10);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).hasSize(2);
        assertThat(response.getBody().get(0).getAuctionId()).isEqualTo("A-002");
        verify(auctionStatusService).getAuctionHistory(10);
    }

    @Test
    @DisplayName("getAuctionHistory: uses provided limit parameter")
    void getAuctionHistory_usesProvidedLimit() {
        when(auctionStatusService.getAuctionHistory(5)).thenReturn(List.of());

        auctionController.getAuctionHistory(5);

        verify(auctionStatusService).getAuctionHistory(5);
    }

    @Test
    @DisplayName("getAuctionHistory: returns empty list when no history")
    void getAuctionHistory_noHistory_returnsEmptyList() {
        when(auctionStatusService.getAuctionHistory(10)).thenReturn(List.of());

        ResponseEntity<List<AuctionStatusDto>> response = auctionController.getAuctionHistory(10);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isEmpty();
    }

    // ─── helper for verify with argThat ────────────────────────────────────

    private static <T> T argThat(java.util.function.Predicate<T> predicate) {
        return org.mockito.ArgumentMatchers.argThat(predicate::test);
    }
}
