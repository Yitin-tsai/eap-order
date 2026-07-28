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
            SELECT order_id,
                   price,
                   CASE WHEN status = 'MATCHED' THEN matched_amount ELSE remaining_amount END AS display_amount,
                   side,
                   to_char(updated_at, 'YYYY-MM-DD HH24:MI:SS') AS update_time,
                   status
            FROM order_service.orders_current
            WHERE user_id = ?
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
        return safeQuery(userId, "ORDER BY updated_at DESC", "user orders");
    }

    /**
     * 只查詢用戶的待處理訂單。
     */
    public ListUserOrderRes getUserPendingOrders(String userId) {
        return safeQuery(userId, """
                AND status IN ('PENDING_ASSET_CHECK', 'OPEN', 'PARTIALLY_MATCHED')
                ORDER BY updated_at DESC
                """, "user pending orders");
    }

    /**
     * 只查詢用戶的已成交訂單。
     */
    public ListUserOrderRes getUserMatchedOrders(String userId) {
        return safeQuery(userId, """
                AND status = 'MATCHED'
                ORDER BY updated_at DESC
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
                        .build(),
                userUuid);
    }
}
