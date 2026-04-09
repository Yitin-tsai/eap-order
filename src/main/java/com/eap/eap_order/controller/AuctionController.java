package com.eap.eap_order.controller;

import com.eap.common.dto.AuctionBidRequest;
import com.eap.common.dto.AuctionBidResponse;
import com.eap.common.dto.AuctionResultDto;
import com.eap.common.dto.AuctionStatusDto;
import com.eap.eap_order.application.AuctionStatusService;
import com.eap.eap_order.application.PlaceAuctionBidService;
import com.eap.eap_order.controller.dto.req.PlaceAuctionBidReq;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/bid/auction")
@Validated
@Tag(name = "auction", description = "Timed Double Auction API")
@Slf4j
public class AuctionController {

    @Autowired
    private PlaceAuctionBidService placeAuctionBidService;

    @Autowired
    private AuctionStatusService auctionStatusService;

    @Operation(operationId = "post-auction-bid", summary = "Submit auction bid", description = "Submit a sealed bid for the current auction")
    @ApiResponse(responseCode = "200", description = "Bid submitted")
    @ApiResponse(responseCode = "400", description = "Validation error")
    @PostMapping
    public ResponseEntity<AuctionBidResponse> submitBid(
            @Parameter(description = "Auction bid request") @Valid @RequestBody PlaceAuctionBidReq request) {

        log.info("Received auction bid: auctionId={}, userId={}, side={}",
            request.getAuctionId(), request.getUserId(), request.getSide());

        // Convert controller DTO to common DTO
        AuctionBidRequest bidRequest = AuctionBidRequest.builder()
            .userId(request.getUserId().toString())
            .auctionId(request.getAuctionId())
            .side(request.getSide())
            .steps(request.getSteps().stream()
                .map(step -> new AuctionBidRequest.BidStep(step.getPrice(), step.getAmount()))
                .collect(Collectors.toList()))
            .build();

        AuctionBidResponse response = placeAuctionBidService.submitBid(bidRequest);

        if (response.isSuccess()) {
            return ResponseEntity.ok(response);
        } else {
            return ResponseEntity.badRequest().body(response);
        }
    }

    @Operation(operationId = "get-auction-status", summary = "Get current auction status", description = "Get the status of the current active auction")
    @ApiResponse(responseCode = "200", description = "Success")
    @GetMapping("/status")
    public ResponseEntity<AuctionStatusDto> getAuctionStatus() {
        log.info("Query current auction status");
        AuctionStatusDto status = auctionStatusService.getCurrentAuctionStatus();
        return ResponseEntity.ok(status);
    }

    @Operation(operationId = "get-auction-results", summary = "Get auction results", description = "Get clearing results for a specific auction")
    @ApiResponse(responseCode = "200", description = "Success")
    @GetMapping("/results")
    public ResponseEntity<AuctionResultDto> getAuctionResults(
            @Parameter(description = "Auction ID") @RequestParam String auctionId) {
        log.info("Query auction results: auctionId={}", auctionId);
        AuctionResultDto results = auctionStatusService.getAuctionResults(auctionId);
        return ResponseEntity.ok(results);
    }

    @Operation(operationId = "get-auction-history", summary = "Get auction history", description = "Get recent cleared auction sessions")
    @ApiResponse(responseCode = "200", description = "Success")
    @GetMapping("/history")
    public ResponseEntity<List<AuctionStatusDto>> getAuctionHistory(
            @Parameter(description = "Limit") @RequestParam(defaultValue = "10") int limit) {
        log.info("Query auction history: limit={}", limit);
        List<AuctionStatusDto> history = auctionStatusService.getAuctionHistory(limit);
        return ResponseEntity.ok(history);
    }
}
