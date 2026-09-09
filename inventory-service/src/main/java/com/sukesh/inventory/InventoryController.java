package com.sukesh.inventory;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class InventoryController {

    @GetMapping("/inventory")
    public String inventory() {
        return "Inventory Service is working!";
    }
}