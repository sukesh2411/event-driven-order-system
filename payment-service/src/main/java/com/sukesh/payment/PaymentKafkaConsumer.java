package com.sukesh.payment;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class PaymentKafkaConsumer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public PaymentKafkaConsumer(
            KafkaTemplate<String, String> kafkaTemplate) {

        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(
            topics = "inventory-reserved",
            groupId = "payment-group"
    )
    public void processPayment(String message) {

        System.out.println("💳 PAYMENT RECEIVED:");
        System.out.println(message);

        try {

            // Extract orderId from the event
            String orderId = message
                    .split("\"orderId\":\"")[1]
                    .split("\"")[0];

            /*
             * Mock payment rule:
             *
             * Orders whose ID contains "PAY-FAIL"
             * will intentionally fail.
             *
             * All other orders will succeed.
             */
            if (orderId.contains("PAY-FAIL")) {

                System.out.println(
                        "❌ PAYMENT FAILED FOR: " + orderId
                );

                kafkaTemplate.send(
                        "payment-failed",
                        message
                );

                System.out.println(
                        "📨 Payment failed event sent to Kafka"
                );

            } else {

                System.out.println(
                        "✅ Payment processed successfully!"
                );

                kafkaTemplate.send(
                        "payment-completed",
                        message
                );

                System.out.println(
                        "📨 Payment completed event sent to Kafka"
                );
            }

        } catch (Exception e) {

            System.out.println(
                    "❌ Failed to process payment: "
                            + e.getMessage()
            );
        }
    }
}