package com.sukesh.order;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
public class OrderController {

    private final KafkaProducer kafkaProducer;
    private final OrderRepository orderRepository;

    public OrderController(
            KafkaProducer kafkaProducer,
            OrderRepository orderRepository) {

        this.kafkaProducer = kafkaProducer;
        this.orderRepository = orderRepository;
    }

    @PostMapping("/orders")
    public String createOrder(@RequestBody OrderEvent event) {

        Order order = new Order(
                event.getOrderId(),
                event.getProduct(),
                event.getQuantity(),
                "PENDING"
        );

        orderRepository.save(order);

        kafkaProducer.sendOrderCreated(event);

        return "Order created and event sent to Kafka!";
    }

    @GetMapping("/orders/{orderId}")
    public ResponseEntity<Order> getOrder(
            @PathVariable String orderId) {

        return orderRepository
                .findById(orderId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }
}