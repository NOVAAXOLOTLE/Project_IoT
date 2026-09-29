idf_component_register(SRCS "main.cpp"
                    INCLUDE_DIRS "."
                    REQUIRES esp_adc esp_driver_gpio esp_wifi esp_netif nvs_flash dht)