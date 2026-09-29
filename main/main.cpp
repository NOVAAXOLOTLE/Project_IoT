#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <math.h>
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/event_groups.h"
#include "esp_system.h"
#include "esp_wifi.h"
#include "esp_event.h"
#include "esp_log.h"
#include "nvs_flash.h"
#include "mqtt_client.h"
#include "esp_adc/adc_oneshot.h"
#include "driver/gpio.h"
#include "cJSON.h"
#include "esp_sntp.h"
#include <sys/time.h>
#include "dht.h"

// ================= CREDENCIALES Y RED =================
#define WIFI_SSID       "MEGACABLE-2.4G-AFA5"
#define WIFI_PASS       "xXdpScJ6Us"

// MQTTS: Puerto 8883 para TLS
#define MQTT_BROKER_URI "mqtts://192.168.100.12:8883"

// ================= TOPICOS MQTT =======================
#define TOPIC_TELEMETRY "greenhouse/telemetry"
#define TOPIC_CONTROL   "greenhouse/control"

// ================= HARDWARE ===========================
#define DHT_PIN            GPIO_NUM_4
#define SENSOR_ADC_UNIT    ADC_UNIT_1
#define SOIL_ADC_CHANNEL   ADC_CHANNEL_6 // GPIO34 
#define LDR_ADC_CHANNEL    ADC_CHANNEL_7 // GPIO35 

#define ADC_DRY            3000 // Valor ADC para suelo seco
#define ADC_WET            0 // Valor ADC para suelo húmedo

#define ESP32_ADC_MAX_VAL  4095
#define ESP32_VCC          3.3f
#define FIXED_RESISTOR     10000.0f // 10k Ohm
#define GAMMA              0.7f
#define RL10               50.0f

// Actuadores de ejemplo
#define RED_LED_PIN       GPIO_NUM_5  // Representa Bomba de Agua
#define BLUE_LED_PIN       GPIO_NUM_2  // Representa Extractores

static const char *TAG = "EDGE_GREENHOUSE";
static bool led_rojo_state = false;
static bool led_azul_state = false;

// ================= CERTIFICADO TLS (X.509) =================
const char *server_cert_pem_start = 
"-----BEGIN CERTIFICATE-----\n"
"MIIDizCCAnOgAwIBAgIUS9jtYbFAri5YcrMBzsNf6b6SfCwwDQYJKoZIhvcNAQEL\n"
"BQAwVTELMAkGA1UEBhMCTVgxEDAOBgNVBAgMB0phbGlzY28xFDASBgNVBAcMC0d1\n"
"YWRhbGFqYXJhMQ0wCwYDVQQKDARDRVRJMQ8wDQYDVQQDDAZFZGdlQ0EwHhcNMjYw\n"
"OTI1MDc1NDIyWhcNMzYwOTIyMDc1NDIyWjBVMQswCQYDVQQGEwJNWDEQMA4GA1UE\n"
"CAwHSmFsaXNjbzEUMBIGA1UEBwwLR3VhZGFsYWphcmExDTALBgNVBAoMBENFVEkx\n"
"DzANBgNVBAMMBkVkZ2VDQTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEB\n"
"AKyd++sEwyPJ/1hMewwsXVb6U0jdVE2Iyr1KKYeJV4V6gXX8AdouNXJ+CotyWoGr\n"
"icEzvs2rhe2HIYhyW0C75kSsUMV8mYFSwCxX36dEHEghFRINAf0QhYhinBRmpyPH\n"
"CexCa6OsrevmGDu0OKCY9WJQ1AcFZ5BsoRTNcjNtRL6wqz+dueWLW2PBB9C41Kjx\n"
"J3syHzmqe30jbZCaYyZVv74JQv2/QFgs4ZDtcRN1yoETGt23JuFZJVdD482qusGt\n"
"YY4y/CcDGYwd5hXbSl2jqJx1CkvnUW3WxNjUsGI+/9DYTNgmQVXIKja9ldD30fNz\n"
"a7LXfa750iGsfNu5xOikkJUCAwEAAaNTMFEwHQYDVR0OBBYEFO1bcJ8PreieaRZA\n"
"cjyFg/OhbWq3MB8GA1UdIwQYMBaAFO1bcJ8PreieaRZAcjyFg/OhbWq3MA8GA1Ud\n"
"EwEB/wQFMAMBAf8wDQYJKoZIhvcNAQELBQADggEBAFhqrklldg7GS8VwieMyfS6P\n"
"9kvlfb8rTnxjtbCfi1oQigXYb4anZ4yI0pb6C1227/+5T3e6LrqLD8RLFXni6L6u\n"
"RAMLdIYvNNTQWy10Va+ukYqHd0iQgvmibnQIhHgKiL4AJE7pJ2orn13uD6fKV7z+\n"
"W9bQUdKmUFGSnPfcSYxghORYbKmf32yt1kIlhYdI3wdJRGBBuWqL3F0UENb1ZA6l\n"
"c9BqrYWhqt5M8AZt3l0Qz871Fl8DZN9Yk3SAtLQymF9f/UX27Y9raN1hnCPb6i/I\n"
"MEvnhCkRuh7zywcULlTArye867uI5zHpMjJs+UeTiCBB0gkEXiepmQQzbr57lQI=\n"
"-----END CERTIFICATE-----\n";

// Variables Globales
static EventGroupHandle_t s_wifi_event_group;
#define WIFI_CONNECTED_BIT BIT0
static esp_mqtt_client_handle_t mqtt_client = NULL;

// ================= FILTRO PROMEDIO MÓVIL =================
#define FILTER_SAMPLES 5

struct MovingAverageFilter {
    float buffer[FILTER_SAMPLES] = {0};
    uint8_t index = 0;
    bool filled = false;

    float add_and_get_average(float new_value) {
        buffer[index] = new_value;
        index++;
        if (index >= FILTER_SAMPLES) {
            index = 0;
            filled = true;
        }
        
        int count = filled ? FILTER_SAMPLES : index;
        float sum = 0;
        for(int i = 0; i < count; i++) {
            sum += buffer[i];
        }
        return sum / count;
    }
};

MovingAverageFilter filter_temp;
MovingAverageFilter filter_hum;
MovingAverageFilter filter_soil;
MovingAverageFilter filter_ldr;

float map_adc_to_percentage(float adc_value) {
    float percentage = ((ADC_DRY - adc_value) / (ADC_DRY - ADC_WET)) * 100.0f;
    if (percentage < 0.0f) percentage = 0.0f;
    if (percentage > 100.0f) percentage = 100.0f;
    return percentage;
}

float convert_adc_to_lux(int raw_adc) {
    // 1. evitar división por cero en total oscuridad
    if (raw_adc >= ESP32_ADC_MAX_VAL) raw_adc = ESP32_ADC_MAX_VAL - 1;
    if (raw_adc <= 0) raw_adc = 1;

    // 2. Convertir el valor crudo a voltaje real
    float voltage = ((float)raw_adc / ESP32_ADC_MAX_VAL) * ESP32_VCC;

    // 3. Calcular la resistencia actual del LDR
    // (cambia el orden si tu LDR está conectado a GND en vez de VCC)
    float resistance_ldr = FIXED_RESISTOR * (ESP32_VCC - voltage) / voltage;

    // 4. Aplicar la fórmula logarítmica para obtener los Lux reales
    float lux = pow((RL10 * 1e3 * pow(10, GAMMA)) / resistance_ldr, (1.0 / GAMMA));

    return lux;
}

// ================= EVENTOS MQTT Y CONTROL =================
static void mqtt_event_handler(void *handler_args, esp_event_base_t base, int32_t event_id, void *event_data) {
    esp_mqtt_event_handle_t event = (esp_mqtt_event_handle_t)event_data;
    esp_mqtt_client_handle_t client = event->client;
    
    if (event_id == MQTT_EVENT_CONNECTED) {
        ESP_LOGI(TAG, "MQTT Seguro Conectado. Suscribiendo a control...");
        esp_mqtt_client_subscribe(client, TOPIC_CONTROL, 1);
    } 
    else if (event_id == MQTT_EVENT_DATA) {
        if (strncmp(event->topic, TOPIC_CONTROL, event->topic_len) == 0) {
            cJSON *root = cJSON_ParseWithLength(event->data, event->data_len);
            if (root) {
                cJSON *led = cJSON_GetObjectItem(root, "led");
                cJSON *status = cJSON_GetObjectItem(root, "status");
                
                if (led && status && strcmp(status->valuestring, "TOGGLE") == 0) {
                    if (strcmp(led->valuestring, "red") == 0) {
                        led_rojo_state = !led_rojo_state;
                        gpio_set_level(RED_LED_PIN, led_rojo_state);
                        ESP_LOGI(TAG, "Actuador (Bomba): %d", led_rojo_state);
                    } else if (strcmp(led->valuestring, "blue") == 0) {
                        led_azul_state = !led_azul_state;
                        gpio_set_level(BLUE_LED_PIN, led_azul_state);
                        ESP_LOGI(TAG, "Actuador (Extractor): %d", led_azul_state);
                    }
                }
                cJSON_Delete(root);
            }
        }
    }
}

// ================= RED Y TIEMPO =================
static void wifi_event_handler(void* arg, esp_event_base_t event_base, int32_t event_id, void* event_data) {
    if (event_base == WIFI_EVENT && event_id == WIFI_EVENT_STA_START) {
        esp_wifi_connect();
    } else if (event_base == WIFI_EVENT && event_id == WIFI_EVENT_STA_DISCONNECTED) {
        esp_wifi_connect();
        xEventGroupClearBits(s_wifi_event_group, WIFI_CONNECTED_BIT);
    } else if (event_base == IP_EVENT && event_id == IP_EVENT_STA_GOT_IP) {
        xEventGroupSetBits(s_wifi_event_group, WIFI_CONNECTED_BIT);
    }
}

void wifi_init_sta(void) {
    s_wifi_event_group = xEventGroupCreate();
    ESP_ERROR_CHECK(esp_netif_init());
    ESP_ERROR_CHECK(esp_event_loop_create_default());
    esp_netif_create_default_wifi_sta();
    wifi_init_config_t cfg = WIFI_INIT_CONFIG_DEFAULT();
    ESP_ERROR_CHECK(esp_wifi_init(&cfg));
    ESP_ERROR_CHECK(esp_event_handler_instance_register(WIFI_EVENT, ESP_EVENT_ANY_ID, &wifi_event_handler, NULL, NULL));
    ESP_ERROR_CHECK(esp_event_handler_instance_register(IP_EVENT, IP_EVENT_STA_GOT_IP, &wifi_event_handler, NULL, NULL));
    
    wifi_config_t wifi_config = {};
    strcpy((char*)wifi_config.sta.ssid, WIFI_SSID);
    strcpy((char*)wifi_config.sta.password, WIFI_PASS);
    ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_STA));
    ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_STA, &wifi_config));
    ESP_ERROR_CHECK(esp_wifi_start());
    xEventGroupWaitBits(s_wifi_event_group, WIFI_CONNECTED_BIT, pdFALSE, pdTRUE, portMAX_DELAY);
}

void time_sync_init(void) {
    ESP_LOGI(TAG, "Sincronizando hora por SNTP para el Timestamp...");
    esp_sntp_setoperatingmode(ESP_SNTP_OPMODE_POLL);
    esp_sntp_setservername(0, "pool.ntp.org");
    esp_sntp_init();
    int retry = 0;
    while (sntp_get_sync_status() == SNTP_SYNC_STATUS_RESET && ++retry < 15) {
        vTaskDelay(pdMS_TO_TICKS(2000));
    }
}

// ================= MQTT SEGURO =================
void mqtt_app_start(void) {
    esp_mqtt_client_config_t mqtt_cfg = {};
    mqtt_cfg.broker.address.uri = MQTT_BROKER_URI;
    mqtt_cfg.broker.verification.certificate = server_cert_pem_start; 

    mqtt_client = esp_mqtt_client_init(&mqtt_cfg);
    esp_mqtt_client_register_event(mqtt_client, MQTT_EVENT_ANY, mqtt_event_handler, NULL);
    esp_mqtt_client_start(mqtt_client);
}

// ================= APP MAIN =================
extern "C" void app_main(void) {
    esp_err_t ret = nvs_flash_init();
    if (ret == ESP_ERR_NVS_NO_FREE_PAGES || ret == ESP_ERR_NVS_NEW_VERSION_FOUND) {
        ESP_ERROR_CHECK(nvs_flash_erase());
        nvs_flash_init();
    }

    // Configurar GPIOs Actuadores
    gpio_set_direction(RED_LED_PIN, GPIO_MODE_OUTPUT);
    gpio_set_direction(BLUE_LED_PIN, GPIO_MODE_OUTPUT);
    gpio_set_level(RED_LED_PIN, 0);
    gpio_set_level(BLUE_LED_PIN, 0);

    wifi_init_sta();
    time_sync_init();
    mqtt_app_start();

    // Configurar ADC1 (Sensores Capacitivo y LDR)
    adc_oneshot_unit_handle_t adc1_handle;
    adc_oneshot_unit_init_cfg_t init_config1 = {};
    init_config1.unit_id = SENSOR_ADC_UNIT;
    ESP_ERROR_CHECK(adc_oneshot_new_unit(&init_config1, &adc1_handle));
    
    adc_oneshot_chan_cfg_t config = {};
    config.atten = ADC_ATTEN_DB_12;
    config.bitwidth = ADC_BITWIDTH_DEFAULT;
    ESP_ERROR_CHECK(adc_oneshot_config_channel(adc1_handle, SOIL_ADC_CHANNEL, &config));
    ESP_ERROR_CHECK(adc_oneshot_config_channel(adc1_handle, LDR_ADC_CHANNEL, &config));

    while (true) {
        float raw_temp = 0.0, raw_hum_env = 0.0;
        int raw_soil = 0, raw_ldr = 0;


        // Se corrigió al tipo DHT11 según la arquitectura
        dht_read_float_data(DHT_TYPE_DHT11, DHT_PIN, &raw_hum_env, &raw_temp); 
        adc_oneshot_read(adc1_handle, SOIL_ADC_CHANNEL, &raw_soil);
        adc_oneshot_read(adc1_handle, LDR_ADC_CHANNEL, &raw_ldr);

        float temp_avg = filter_temp.add_and_get_average(raw_temp);
        float hum_env_avg = filter_hum.add_and_get_average(raw_hum_env);
        float hum_soil_avg = filter_soil.add_and_get_average((float)raw_soil);
        float hum_soil_percentage = map_adc_to_percentage(hum_soil_avg);
        float ldr_avg = filter_ldr.add_and_get_average((float)raw_ldr);
        float lux_value = convert_adc_to_lux((int)ldr_avg);

        struct timeval tv_now;
        gettimeofday(&tv_now, NULL);
        int64_t timestamp = (int64_t)tv_now.tv_sec;

        cJSON *root = cJSON_CreateObject();
        cJSON_AddStringToObject(root, "device_id", "ESP32_GREENHOUSE_01");
        cJSON_AddNumberToObject(root, "timestamp", timestamp);

        cJSON *telemetry = cJSON_CreateObject();
        cJSON_AddNumberToObject(telemetry, "temperature", temp_avg);
        cJSON_AddNumberToObject(telemetry, "humidity_env", hum_env_avg);
        cJSON_AddNumberToObject(telemetry, "humidity_soil", hum_soil_percentage);
        cJSON_AddNumberToObject(telemetry, "luminosity", lux_value);

        cJSON_AddItemToObject(root, "telemetry", telemetry);

        char *json_string = cJSON_PrintUnformatted(root);

        if (mqtt_client != NULL && filter_temp.filled) {
            esp_mqtt_client_publish(mqtt_client, TOPIC_TELEMETRY, json_string, 0, 1, 0); 
        }

        free(json_string);
        cJSON_Delete(root);

        // Aumentado a 1500ms debido a las restricciones de hardware del DHT11
        vTaskDelay(pdMS_TO_TICKS(1500));
    }
}