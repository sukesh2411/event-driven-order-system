package com.sukesh.order;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducer {

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    public KafkaProducer(KafkaTemplate<String, OrderEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendOrderCreated(OrderEvent event) {
        kafkaTemplate.send("order-created", event.getOrderId(), event);
    }
}