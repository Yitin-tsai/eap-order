package com.eap.eap_order.configuration.config;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import static com.eap.common.constants.RabbitMQConstants.*;

/**
 * Order Module RabbitMQ Configuration
 * 
 * This module consumes:
 * - order.confirmed events (for status updates after wallet validation)
 * - trade.executed events (for matched order-state application)
 * - order.failed events (for failure handling)
 * 
 * This module publishes:
 * - order.submitted events (initial order placement)
 * 
 * Topology: Each module gets its own queues bound to shared routing keys
 */
@Configuration
public class RabbitMQConfig {

  @Bean
  public MessageConverter jsonMessageConverter() {
    return new Jackson2JsonMessageConverter();
  }

  @Bean
  public TopicExchange orderExchange() {
    return new TopicExchange(ORDER_EXCHANGE);
  }

  @Bean
  public TopicExchange tradeExchange() {
    return new TopicExchange(TRADE_EXCHANGE);
  }

  // --- Dead Letter Exchange / Queue (ADR-001) ---

  @Bean
  public FanoutExchange deadLetterExchange() {
    return new FanoutExchange(DEAD_LETTER_EXCHANGE);
  }

  @Bean
  public Queue deadLetterQueue() {
    return QueueBuilder.durable(DEAD_LETTER_QUEUE).build();
  }

  @Bean
  public Binding dlqBinding(Queue deadLetterQueue, FanoutExchange deadLetterExchange) {
    return BindingBuilder.bind(deadLetterQueue).to(deadLetterExchange);
  }

  // --- Order module queues (with DLX binding) ---

  @Bean
  public Queue orderOrderConfirmedQueue() {
    return QueueBuilder.durable(ORDER_ORDER_CONFIRMED_QUEUE)
        .withArgument("x-dead-letter-exchange", DEAD_LETTER_EXCHANGE)
        .build();
  }

  @Bean
  public Queue orderTradeExecutedQueue() {
    return QueueBuilder.durable(ORDER_TRADE_EXECUTED_QUEUE)
        .withArgument("x-dead-letter-exchange", DEAD_LETTER_EXCHANGE)
        .build();
  }

  @Bean
  public Queue orderOrderFailedQueue() {
    return QueueBuilder.durable(ORDER_ORDER_FAILED_QUEUE)
        .withArgument("x-dead-letter-exchange", DEAD_LETTER_EXCHANGE)
        .build();
  }

  @Bean
  public Binding orderOrderConfirmedBinding(@Qualifier("orderOrderConfirmedQueue") Queue orderOrderConfirmedQueue,
      @Qualifier("orderExchange") TopicExchange orderExchange) {
    return BindingBuilder.bind(orderOrderConfirmedQueue).to(orderExchange).with(ORDER_CONFIRMED_KEY);
  }

  @Bean
  public Binding orderTradeExecutedBinding(
      @Qualifier("orderTradeExecutedQueue") Queue orderTradeExecutedQueue,
      @Qualifier("tradeExchange") TopicExchange tradeExchange) {
    return BindingBuilder.bind(orderTradeExecutedQueue).to(tradeExchange).with(TRADE_EXECUTED_KEY);
  }

  @Bean
  public Binding orderOrderFailedBinding(@Qualifier("orderOrderFailedQueue") Queue orderOrderFailedQueue,
      @Qualifier("orderExchange") TopicExchange orderExchange) {
    return BindingBuilder.bind(orderOrderFailedQueue).to(orderExchange).with(ORDER_FAILED_KEY);
  }

  // --- Auction Exchange and Queues ---

  @Bean
  public TopicExchange auctionExchange() {
    return new TopicExchange(AUCTION_EXCHANGE);
  }

  @Bean
  public Queue orderAuctionClearedQueue() {
    return QueueBuilder.durable(ORDER_AUCTION_CLEARED_QUEUE)
        .withArgument("x-dead-letter-exchange", DEAD_LETTER_EXCHANGE)
        .build();
  }

  @Bean
  public Queue orderAuctionCreatedQueue() {
    return QueueBuilder.durable(ORDER_AUCTION_CREATED_QUEUE)
        .withArgument("x-dead-letter-exchange", DEAD_LETTER_EXCHANGE)
        .build();
  }

  @Bean
  public Binding orderAuctionClearedBinding(@Qualifier("orderAuctionClearedQueue") Queue orderAuctionClearedQueue,
      @Qualifier("auctionExchange") TopicExchange auctionExchange) {
    return BindingBuilder.bind(orderAuctionClearedQueue).to(auctionExchange).with(AUCTION_CLEARED_KEY);
  }

  @Bean
  public Binding orderAuctionCreatedBinding(@Qualifier("orderAuctionCreatedQueue") Queue orderAuctionCreatedQueue,
      @Qualifier("auctionExchange") TopicExchange auctionExchange) {
    return BindingBuilder.bind(orderAuctionCreatedQueue).to(auctionExchange).with(AUCTION_CREATED_KEY);
  }
}
