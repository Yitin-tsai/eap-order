package com.eap.eap_order.application;

import java.util.List;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.eap.eap_order.controller.dto.res.ListUserOrderRes;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class OrderQueryService {

    private static final String USER_ORDER_SELECT = """
            SELECT current_order.order_id,
                   current_order.price,
                   CASE WHEN COALESCE(matching.status, current_order.status) = 'MATCHED'
                        THEN COALESCE(matching.matched_amount, current_order.matched_amount)
                        ELSE COALESCE(matching.remaining_amount, current_order.remaining_amount)
                   END AS display_amount,
                   current_order.side,
                   to_char(GREATEST(current_order.updated_at,
                                    COALESCE(matching.updated_at, current_order.updated_at)),
                           'YYYY-MM-DD HH24:MI:SS') AS update_time,
                   COALESCE(matching.status, current_order.status) AS status,
                   COALESCE(matching.asset_reservation_status,
                            current_order.asset_reservation_status) AS asset_reservation_status
            FROM order_service.orders_current current_order
            LEFT JOIN order_service.order_matching_state matching
                   ON matching.order_id = current_order.order_id
            WHERE current_order.user_id = ?
            """;

    @Autowired
    JdbcTemplate jdbc;

    /**
     * 查詢用戶的所有訂單（包括待處理和已成交）。
     *
     * User order status is owned by Order. Do not synchronously query MatchEngine Redis here;
     * Redis orderbook indexes are matching state, not the user-order read model.
     */
    public ListUserOrderRes getUserOrderList(String userId) {
        return safeQuery(userId, "ORDER BY update_time DESC", "user orders");
    }

    /**
     * 只查詢用戶的待處理訂單。
     */
    public ListUserOrderRes getUserPendingOrders(String userId) {
        return safeQuery(userId, """
                AND COALESCE(matching.status, current_order.status)
                    IN ('PENDING_ASSET_CHECK', 'OPEN', 'PARTIALLY_MATCHED')
                ORDER BY update_time DESC
                """, "user pending orders");
    }

    /**
     * 只查詢用戶的已成交訂單。
     */
    public ListUserOrderRes getUserMatchedOrders(String userId) {
        return safeQuery(userId, """
                AND COALESCE(matching.status, current_order.status) = 'MATCHED'
                ORDER BY update_time DESC
                """, "user matched orders");
    }

    private ListUserOrderRes safeQuery(String userId, String sqlSuffix, String label) {
        try {
            return ListUserOrderRes.builder()
                    .userOrders(queryOrders(userId, sqlSuffix))
                    .build();
        } catch (Exception e) {
            log.warn("Failed to query {}: userId={}", label, userId, e);
            return ListUserOrderRes.builder()
                    .userOrders(List.of())
                    .build();
        }
    }

    private List<ListUserOrderRes.UserOrder> queryOrders(String userId, String sqlSuffix) {
        UUID userUuid = UUID.fromString(userId);
        return jdbc.query(USER_ORDER_SELECT + sqlSuffix,
                (rs, rowNum) -> ListUserOrderRes.UserOrder.builder()
                        .orderId(rs.getString("order_id"))
                        .price(rs.getInt("price"))
                        .amount(rs.getInt("display_amount"))
                        .type(rs.getString("side"))
                        .updateTime(rs.getString("update_time"))
                        .status(rs.getString("status"))
                        .assetReservationStatus(rs.getString("asset_reservation_status"))
                        .build(),
                userUuid);
    }
}
