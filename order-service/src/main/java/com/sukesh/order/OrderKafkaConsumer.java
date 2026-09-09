package com.sukesh.order;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class OrderKafkaConsumer {

    private final OrderRepository orderRepository;

    public OrderKafkaConsumer(OrderRepository orderRepository) {
        this.orderRepository = orderRepository;
    }

    // ==============================
    // PAYMENT COMPLETED
    // ==============================

    @KafkaListener(
            topics = "payment-completed",
            groupId = "order-group"
    )
    public void paymentCompleted(String message) {

        System.out.println("💰 ORDER SERVICE RECEIVED PAYMENT SUCCESS:");
        System.out.println(message);

        try {

            String orderId = message
                    .split("\"orderId\":\"")[1]
                    .split("\"")[0];

            Order order = orderRepository
                    .findById(orderId)
                    .orElse(null);

            if (order != null) {

                order.setStatus("CONFIRMED");

                orderRepository.save(order);

                System.out.println("🎉 ORDER CONFIRMED: " + orderId);

            } else {

                System.out.println("❌ Order not found: " + orderId);
            }

        } catch (Exception e) {

            System.out.println(
                    "❌ Failed to process payment success: "
                            + e.getMessage()
            );
        }
    }


    // ==============================
    // PAYMENT FAILED
    // ==============================

    @KafkaListener(
            topics = "payment-failed",
            groupId = "order-group"
    )
    public void paymentFailed(String message) {

        System.out.println("❌ ORDER SERVICE RECEIVED PAYMENT FAILURE:");
        System.out.println(message);

        cancelOrder(message);
    }


    // ==============================
    // INVENTORY FAILED
    // ==============================

    @KafkaListener(
            topics = "inventory-failed",
            groupId = "order-group"
    )
    public void inventoryFailed(String message) {

        System.out.println("❌ ORDER SERVICE RECEIVED INVENTORY FAILURE:");
        System.out.println(message);

        cancelOrder(message);
    }


    // ==============================
    // COMMON CANCEL METHOD
    // ==============================

    private void cancelOrder(String message) {

        try {

            String orderId = message
                    .split("\"orderId\":\"")[1]
                    .split("\"")[0];

            Order order = orderRepository
                    .findById(orderId)
                    .orElse(null);

            if (order != null) {

                order.setStatus("CANCELLED");

                orderRepository.save(order);

                System.out.println(
                        "🚫 ORDER CANCELLED: " + orderId
                );

            } else {

                System.out.println(
                        "❌ Order not found: " + orderId
                );
            }

        } catch (Exception e) {

            System.out.println(
                    "❌ Failed to process cancellation: "
                            + e.getMessage()
            );
        }
    }
}