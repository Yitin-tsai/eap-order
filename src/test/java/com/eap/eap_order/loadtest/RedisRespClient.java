package com.eap.eap_order.loadtest;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

final class RedisRespClient {

    private final String host;
    private final int port;

    RedisRespClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    long integer(String command, String... args) throws IOException {
        Object value = command(command, args);
        if (value instanceof Number number) {
            return number.longValue();
        }
        throw new IOException("expected Redis integer response, got " + value);
    }

    Object command(String command, String... args) throws IOException {
        try (Socket socket = new Socket(host, port)) {
            socket.setSoTimeout(5_000);
            OutputStream output = socket.getOutputStream();
            output.write(commandBytes(command, args));
            output.flush();
            return readResponse(socket.getInputStream());
        }
    }

    long countKeys(String pattern) throws IOException {
        long count = 0;
        String cursor = "0";
        do {
            Object response = command("SCAN", cursor, "MATCH", pattern, "COUNT", "1000");
            if (!(response instanceof List<?> values) || values.size() != 2) {
                throw new IOException("unexpected Redis SCAN response: " + response);
            }
            cursor = String.valueOf(values.get(0));
            if (!(values.get(1) instanceof List<?> keys)) {
                throw new IOException("unexpected Redis SCAN key response: " + values.get(1));
            }
            count += keys.size();
        } while (!"0".equals(cursor));
        return count;
    }

    void deleteKeys(String pattern) throws IOException {
        String cursor = "0";
        do {
            Object response = command("SCAN", cursor, "MATCH", pattern, "COUNT", "1000");
            if (!(response instanceof List<?> values) || values.size() != 2) {
                throw new IOException("unexpected Redis SCAN response: " + response);
            }
            cursor = String.valueOf(values.get(0));
            if (values.get(1) instanceof List<?> keys && !keys.isEmpty()) {
                command("DEL", keys.stream().map(String::valueOf).toArray(String[]::new));
            }
        } while (!"0".equals(cursor));
    }

    static byte[] commandBytes(String command, String... args) {
        StringBuilder builder = new StringBuilder();
        builder.append("*").append(args.length + 1).append("\r\n");
        appendBulk(builder, command);
        for (String arg : args) {
            appendBulk(builder, arg);
        }
        return builder.toString().getBytes(StandardCharsets.UTF_8);
    }

    static Object parseResponse(String response) throws IOException {
        return readResponse(new ByteArrayInputStream(response.getBytes(StandardCharsets.UTF_8)));
    }

    private static void appendBulk(StringBuilder builder, String value) {
        builder.append("$").append(value.getBytes(StandardCharsets.UTF_8).length).append("\r\n");
        builder.append(value).append("\r\n");
    }

    private static Object readResponse(InputStream input) throws IOException {
        int prefix = input.read();
        if (prefix == -1) {
            throw new IOException("empty Redis response");
        }
        String line = readLine(input);
        return switch (prefix) {
            case '+' -> line;
            case ':' -> Long.parseLong(line);
            case '-' -> throw new IOException("Redis error: " + line);
            case '$' -> readBulk(input, Integer.parseInt(line));
            case '*' -> {
                int size = Integer.parseInt(line);
                List<Object> values = new ArrayList<>(Math.max(size, 0));
                for (int i = 0; i < size; i++) {
                    values.add(readResponse(input));
                }
                yield values;
            }
            default -> throw new IOException("unsupported Redis response prefix: " + (char) prefix);
        };
    }

    private static String readBulk(InputStream input, int length) throws IOException {
        if (length < 0) {
            return null;
        }
        byte[] bytes = input.readNBytes(length);
        if (bytes.length != length || input.read() != '\r' || input.read() != '\n') {
            throw new IOException("truncated Redis bulk response");
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static String readLine(InputStream input) throws IOException {
        StringBuilder builder = new StringBuilder();
        int previous = -1;
        int current;
        while ((current = input.read()) != -1) {
            if (previous == '\r' && current == '\n') {
                builder.setLength(builder.length() - 1);
                return builder.toString();
            }
            builder.append((char) current);
            previous = current;
        }
        throw new IOException("unterminated Redis response");
    }
}
