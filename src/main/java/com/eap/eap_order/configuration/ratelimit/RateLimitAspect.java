package com.eap.eap_order.configuration.ratelimit;

import com.eap.eap_order.application.OrderSubmissionMetrics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.core.DefaultParameterNameDiscoverer;
import org.springframework.core.ParameterNameDiscoverer;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Aspect
@Component
@RequiredArgsConstructor
@Slf4j
public class RateLimitAspect {

    private final RateLimitService rateLimitService;
    private final OrderSubmissionMetrics metrics;

    private final ExpressionParser parser = new SpelExpressionParser();
    private final ParameterNameDiscoverer nameDiscoverer = new DefaultParameterNameDiscoverer();
    private final ConcurrentMap<KeyExpression, KeyExtractor> keyExtractors = new ConcurrentHashMap<>();
    private static final Pattern SIMPLE_PROPERTY_EXPRESSION =
            Pattern.compile("^#([A-Za-z_$][A-Za-z\\d_$]*)\\.([A-Za-z_$][A-Za-z\\d_$]*)$");

    @Before("@annotation(rateLimit)")
    public void checkRateLimit(JoinPoint joinPoint, RateLimit rateLimit) {
        long aspectStartedNanos = System.nanoTime();
        try {
            long keyExtractionStartedNanos = System.nanoTime();
            String userId = extractKey(joinPoint, rateLimit.key());
            metrics.recordRateLimitKeyExtraction(
                    Duration.ofNanos(System.nanoTime() - keyExtractionStartedNanos));

            if (rateLimitService.isRateLimited(userId, rateLimit.limit(), rateLimit.window())) {
                throw new RateLimitExceededException(userId);
            }
        } finally {
            metrics.recordRateLimitAspect(Duration.ofNanos(System.nanoTime() - aspectStartedNanos));
        }
    }

    private String extractKey(JoinPoint joinPoint, String keyExpression) {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Method method = signature.getMethod();
        KeyExtractor extractor = keyExtractors.computeIfAbsent(
                new KeyExpression(method, keyExpression),
                ignored -> buildKeyExtractor(method, keyExpression));
        return extractor.extract(joinPoint.getArgs());
    }

    private KeyExtractor buildKeyExtractor(Method method, String keyExpression) {
        String[] paramNames = nameDiscoverer.getParameterNames(method);
        KeyExtractor simpleExtractor = tryBuildSimplePropertyExtractor(method, paramNames, keyExpression);
        if (simpleExtractor != null) {
            return simpleExtractor;
        }
        Expression expression = parser.parseExpression(keyExpression);
        return args -> {
            EvaluationContext context = new StandardEvaluationContext();
            if (paramNames != null) {
                for (int i = 0; i < paramNames.length && i < args.length; i++) {
                    context.setVariable(paramNames[i], args[i]);
                }
            }
            Object value = expression.getValue(context);
            return requireKeyValue(value, keyExpression);
        };
    }

    private KeyExtractor tryBuildSimplePropertyExtractor(Method method, String[] paramNames, String keyExpression) {
        Matcher matcher = SIMPLE_PROPERTY_EXPRESSION.matcher(keyExpression);
        if (!matcher.matches() || paramNames == null) {
            return null;
        }
        String parameterName = matcher.group(1);
        String propertyName = matcher.group(2);
        int parameterIndex = -1;
        for (int i = 0; i < paramNames.length; i++) {
            if (parameterName.equals(paramNames[i])) {
                parameterIndex = i;
                break;
            }
        }
        if (parameterIndex < 0 || parameterIndex >= method.getParameterTypes().length) {
            return null;
        }
        Method getter = findGetter(method.getParameterTypes()[parameterIndex], propertyName);
        if (getter == null) {
            return null;
        }
        int resolvedParameterIndex = parameterIndex;
        return args -> {
            if (resolvedParameterIndex >= args.length) {
                throw new IllegalArgumentException("Rate limit key parameter missing for expression: " + keyExpression);
            }
            try {
                Object value = getter.invoke(args[resolvedParameterIndex]);
                return requireKeyValue(value, keyExpression);
            } catch (IllegalAccessException e) {
                throw new IllegalArgumentException("Rate limit key getter is not accessible for expression: "
                        + keyExpression, e);
            } catch (InvocationTargetException e) {
                throw new IllegalArgumentException("Rate limit key getter failed for expression: "
                        + keyExpression, e.getCause());
            }
        };
    }

    private Method findGetter(Class<?> parameterType, String propertyName) {
        String suffix = Character.toUpperCase(propertyName.charAt(0)) + propertyName.substring(1);
        try {
            return parameterType.getMethod("get" + suffix);
        } catch (NoSuchMethodException ignored) {
            try {
                return parameterType.getMethod("is" + suffix);
            } catch (NoSuchMethodException ignoredAgain) {
                return null;
            }
        }
    }

    private String requireKeyValue(Object value, String keyExpression) {
        if (value == null) {
            throw new IllegalArgumentException("Rate limit key resolved to null for expression: " + keyExpression);
        }
        return value.toString();
    }

    private record KeyExpression(Method method, String expression) {
    }

    @FunctionalInterface
    private interface KeyExtractor {
        String extract(Object[] args);
    }
}
