package com.sukesh.inventory;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Service
public class InventoryKafkaConsumer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    // Current available stock
    private final Map<String, Integer> stock = new HashMap<>();

    // Tracks orders for which stock was actually reserved
    private final Map<String, Reservation> reservations = new HashMap<>();

    // Tracks payment-failed events already processed
    private final Set<String> releasedOrders = new HashSet<>();

    public InventoryKafkaConsumer(
            KafkaTemplate<String, String> kafkaTemplate) {

        this.kafkaTemplate = kafkaTemplate;

        // Initial stock
        stock.put("Phone", 10);
        stock.put("Laptop", 5);
        stock.put("Keyboard", 20);
    }

    // =========================================================
    // ORDER CREATED
    // =========================================================

    @KafkaListener(
            topics = "order-created",
            groupId = "inventory-group"
    )
    public void consumeOrder(String message) {

        System.out.println("🔥 INVENTORY RECEIVED:");
        System.out.println(message);

        try {

            String orderId = extractValue(message, "orderId");
            String product = extractValue(message, "product");

            int quantity = Integer.parseInt(
                    extractValue(message, "quantity")
            );

            int availableStock =
                    stock.getOrDefault(product, 0);

            System.out.println("📦 Product: " + product);
            System.out.println("📦 Requested quantity: " + quantity);
            System.out.println("📦 Available stock: " + availableStock);

            // Check if this order was already processed
            if (reservations.containsKey(orderId)) {

                System.out.println(
                        "⚠️ Order already reserved: " + orderId
                );

                return;
            }

            // =================================================
            // STOCK AVAILABLE
            // =================================================

            if (availableStock >= quantity) {

                int remainingStock =
                        availableStock - quantity;

                stock.put(product, remainingStock);

                // Remember the reservation
                reservations.put(
                        orderId,
                        new Reservation(product, quantity)
                );

                kafkaTemplate.send(
                        "inventory-reserved",
                        message
                );

                System.out.println(
                        "✅ STOCK RESERVED FOR: " + orderId
                );

                System.out.println(
                        "📦 Remaining " + product +
                        " stock: " + remainingStock
                );

                System.out.println(
                        "📤 inventory-reserved event sent to Kafka"
                );

            }

            // =================================================
            // STOCK NOT AVAILABLE
            // =================================================

            else {

                kafkaTemplate.send(
                        "inventory-failed",
                        message
                );

                System.out.println(
                        "❌ INSUFFICIENT STOCK FOR: " + product
                );

                System.out.println(
                        "❌ Requested: " + quantity
                );

                System.out.println(
                        "❌ Available: " + availableStock
                );

                System.out.println(
                        "📤 inventory-failed event sent to Kafka"
                );
            }

        } catch (Exception e) {

            System.out.println(
                    "❌ Failed to process inventory event: "
                            + e.getMessage()
            );
        }
    }

    // =========================================================
    // PAYMENT FAILED → RELEASE RESERVED STOCK
    // =========================================================

    @KafkaListener(
            topics = "payment-failed",
            groupId = "inventory-compensation-group"
    )
    public void paymentFailed(String message) {

        System.out.println(
                "💸 PAYMENT FAILED - RELEASE INVENTORY:"
        );

        System.out.println(message);

        try {

            String orderId = extractValue(
                    message,
                    "orderId"
            );

            // Prevent duplicate release
            if (releasedOrders.contains(orderId)) {

                System.out.println(
                        "⚠️ Inventory already released for: "
                                + orderId
                );

                return;
            }

            Reservation reservation =
                    reservations.get(orderId);

            // No reservation found
            if (reservation == null) {

                System.out.println(
                        "⚠️ No inventory reservation found for: "
                                + orderId
                );

                return;
            }

            int currentStock =
                    stock.getOrDefault(
                            reservation.product(),
                            0
                    );

            int newStock =
                    currentStock + reservation.quantity();

            stock.put(
                    reservation.product(),
                    newStock
            );

            releasedOrders.add(orderId);
            reservations.remove(orderId);

            System.out.println(
                    "🔄 INVENTORY RELEASED FOR: "
                            + orderId
            );

            System.out.println(
                    "📦 Returned quantity: "
                            + reservation.quantity()
            );

            System.out.println(
                    "📦 New "
                            + reservation.product()
                            + " stock: "
                            + newStock
            );

            kafkaTemplate.send(
                    "inventory-released",
                    message
            );

            System.out.println(
                    "📤 inventory-released event sent to Kafka"
            );

        } catch (Exception e) {

            System.out.println(
                    "❌ Failed to release inventory: "
                            + e.getMessage()
            );
        }
    }

    // =========================================================
    // SIMPLE JSON VALUE EXTRACTION
    // =========================================================

    private String extractValue(
            String message,
            String field) {

        String search = "\"" + field + "\":";

        int start = message.indexOf(search);

        if (start == -1) {

            throw new IllegalArgumentException(
                    "Field not found: " + field
            );
        }

        start += search.length();

        // String field
        if (message.charAt(start) == '"') {

            start++;

            int end = message.indexOf(
                    '"',
                    start
            );

            if (end == -1) {

                throw new IllegalArgumentException(
                        "Invalid JSON for field: "
                                + field
                );
            }

            return message.substring(
                    start,
                    end
            );
        }

        // Numeric field
        int end = message.indexOf(
                ',',
                start
        );

        if (end == -1) {

            end = message.indexOf(
                    '}',
                    start
            );
        }

        if (end == -1) {

            throw new IllegalArgumentException(
                    "Invalid JSON for field: "
                            + field
            );
        }

        return message
                .substring(start, end)
                .trim();
    }

    // =========================================================
    // RESERVATION RECORD
    // =========================================================

    private record Reservation(
            String product,
            int quantity
    ) {
    }
}