package com.eap.eap_order.configuration.config;

import com.zaxxer.hikari.HikariDataSource;
import jakarta.persistence.EntityManagerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
public class OrderWorkloadDataSourceConfig {

    @Primary
    @Bean(name = "dataSource")
    public HikariDataSource dataSource(
            @Value("${spring.datasource.url}") String jdbcUrl,
            @Value("${spring.datasource.username}") String username,
            @Value("${spring.datasource.password}") String password,
            @Value("${spring.datasource.hikari.maximum-pool-size:10}") int maximumPoolSize,
            @Value("${spring.datasource.hikari.minimum-idle:2}") int minimumIdle) {
        return hikariDataSource("OrderCommandPool", jdbcUrl, username, password, maximumPoolSize, minimumIdle);
    }

    @Primary
    @Bean(name = "transactionManager")
    public PlatformTransactionManager transactionManager(EntityManagerFactory entityManagerFactory) {
        return new JpaTransactionManager(entityManagerFactory);
    }

    @Bean(name = "orderCommandTransactionManager")
    public PlatformTransactionManager orderCommandTransactionManager(
            @Qualifier("dataSource") DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }

    @Primary
    @Bean(name = "jdbcTemplate")
    public JdbcTemplate jdbcTemplate(@Qualifier("dataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    @Primary
    @Bean(name = "namedParameterJdbcTemplate")
    public NamedParameterJdbcTemplate namedParameterJdbcTemplate(@Qualifier("dataSource") DataSource dataSource) {
        return new NamedParameterJdbcTemplate(dataSource);
    }

    @Bean(name = "orderConsumerDataSource")
    public HikariDataSource orderConsumerDataSource(
            @Value("${eap.order.datasource.consumer.jdbc-url:${spring.datasource.url}}") String jdbcUrl,
            @Value("${eap.order.datasource.consumer.username:${spring.datasource.username}}") String username,
            @Value("${eap.order.datasource.consumer.password:${spring.datasource.password}}") String password,
            @Value("${eap.order.datasource.consumer.maximum-pool-size:15}") int maximumPoolSize,
            @Value("${eap.order.datasource.consumer.minimum-idle:4}") int minimumIdle) {
        return hikariDataSource("OrderConsumerPool", jdbcUrl, username, password, maximumPoolSize, minimumIdle);
    }

    @Bean(name = "orderConsumerTransactionManager")
    public PlatformTransactionManager orderConsumerTransactionManager(
            @Qualifier("orderConsumerDataSource") DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }

    @Bean(name = "orderConsumerJdbcTemplate")
    public JdbcTemplate orderConsumerJdbcTemplate(
            @Qualifier("orderConsumerDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    @Bean(name = "orderConsumerNamedParameterJdbcTemplate")
    public NamedParameterJdbcTemplate orderConsumerNamedParameterJdbcTemplate(
            @Qualifier("orderConsumerDataSource") DataSource dataSource) {
        return new NamedParameterJdbcTemplate(dataSource);
    }

    @Bean(name = "orderProjectionDataSource")
    public HikariDataSource orderProjectionDataSource(
            @Value("${eap.order.datasource.projection.jdbc-url:${spring.datasource.url}}") String jdbcUrl,
            @Value("${eap.order.datasource.projection.username:${spring.datasource.username}}") String username,
            @Value("${eap.order.datasource.projection.password:${spring.datasource.password}}") String password,
            @Value("${eap.order.datasource.projection.maximum-pool-size:5}") int maximumPoolSize,
            @Value("${eap.order.datasource.projection.minimum-idle:1}") int minimumIdle) {
        return hikariDataSource("OrderProjectionPool", jdbcUrl, username, password, maximumPoolSize, minimumIdle);
    }

    @Bean(name = "orderProjectionTransactionManager")
    public PlatformTransactionManager orderProjectionTransactionManager(
            @Qualifier("orderProjectionDataSource") DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }

    @Bean(name = "orderProjectionJdbcTemplate")
    public JdbcTemplate orderProjectionJdbcTemplate(
            @Qualifier("orderProjectionDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    private HikariDataSource hikariDataSource(
            String poolName,
            String jdbcUrl,
            String username,
            String password,
            int maximumPoolSize,
            int minimumIdle) {
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setPoolName(poolName);
        dataSource.setJdbcUrl(jdbcUrl);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        dataSource.addDataSourceProperty("ApplicationName", poolName);
        dataSource.setMaximumPoolSize(maximumPoolSize);
        dataSource.setMinimumIdle(minimumIdle);
        return dataSource;
    }
}
