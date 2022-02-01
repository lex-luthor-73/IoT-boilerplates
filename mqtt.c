/*
 * mqtt.cpp
 *
 *  Created on: 20-Jan-2022
 *      Author: HK
 */

#include "mqtt.h"

#include "cJSON.h"
#include "fileOperations.h"
#include "commondef.h"


char mqttendpoint[256];
ssize_t len_mqttep;
char mqttusername[300];
char mqttclientid[64];
char mqtturi[300];
char mqtt_key[2048];
ssize_t len_mqtt_key;
char mqtt_cert[2048];
ssize_t len_mqtt_cert;

char mqttTopicBuf[MAX_MQTT_TPC_SIZE];
char mqttData[MAX_MQTT_MSG_SIZE];

const char mqtt_broker_uri[]= "mqtts://domain_name.com:8883";


esp_mqtt_client_handle_t mqtt_client;
static esp_err_t mqtt_event_handler(esp_mqtt_event_handle_t event);

static void mqtt_on_connect();


void start_mqtt(void)
{
	ESP_LOGI("", "Starting MQTT STuffs");

	len_mqttep = 0;
	get_endpoint(mqttendpoint, sizeof(mqttendpoint), &len_mqttep);

	sprintf(mqttMsgPubChannel, "devices/%s/messages/events/", soft_ap_data.name_short);
	sprintf(mqttclientid, "%s", soft_ap_data.name_short);
	sprintf(mqtturi, "mqtts://%s:8883", mqttendpoint);
	sprintf(mqttusername, "%s/%s", mqttendpoint, soft_ap_data.name_short);

	ESP_LOGI("", "Pub channel:%s", mqttMsgPubChannel);
	ESP_LOGI("", "Client ID: %s", mqttclientid);
	ESP_LOGI("", "MQTT URI: %s", mqtturi);
	ESP_LOGI("", "Username: %s", mqttusername);

	len_mqtt_key = 0;
	len_mqtt_cert = 0;

	get_key(mqtt_key, sizeof(mqtt_key), &len_mqtt_key);
	get_cert(mqtt_cert, sizeof(mqtt_cert), &len_mqtt_cert);

	ESP_LOGI("", "%d\n%s", len_mqtt_key, mqtt_key);
	ESP_LOGI("", "%d\n %s", len_mqtt_cert, mqtt_cert);


	ESP_LOGI("", "Waiting for the wifi");
	xEventGroupWaitBits(common_events_group, 1 << enum_connected_to_internet, false, true, portMAX_DELAY);
	ESP_LOGI("", "Waiting for time");
	xEventGroupWaitBits(common_events_group, 1 << enum_time_synced, false, true, portMAX_DELAY);
	ESP_LOGI("", "Connecting to mqtt server");

	esp_mqtt_client_config_t mqtt_cfg = {
			.uri = mqtturi,
			.event_handle = mqtt_event_handler,
			.cert_pem = NULL,
			.skip_cert_common_name_check = 1,
			.client_id = mqttclientid,
			.username = mqttusername,
			//.cert_pem = NULL, //(const char *)server_cert_pem_start,
			.client_cert_pem = (const char *)mqtt_cert,
			.client_key_pem = (const char *)mqtt_key,
			.keepalive = 90,

	};


	mqtt_client = esp_mqtt_client_init(&mqtt_cfg);

	esp_mqtt_client_start(mqtt_client);
	ESP_LOGI("", "Free memory: %d bytes", esp_get_free_heap_size());
	ESP_LOGI("", "MQTT Client started");
}

static esp_err_t mqtt_event_handler(esp_mqtt_event_handle_t event)
{
	esp_mqtt_client_handle_t client;
	client = event->client;
	mqtt_msg_def rcvd_msg;
	switch(event->event_id)
	{
	case MQTT_EVENT_CONNECTED:


		xEventGroupSetBits(common_events_group, 1 << enum_connected_to_mqtt);
		ESP_LOGI("", "MQTT is connected \n");

		memset(mqttTopicBuf, 0, sizeof(mqttTopicBuf));
		sprintf(mqttTopicBuf, "devices/%s/messages/devicebound/#", soft_ap_data.name_short);
		ESP_LOGI("", "Subscribe to : [%s]", mqttTopicBuf);

		esp_mqtt_client_subscribe(client, mqttTopicBuf, 0);

		mqtt_on_connect();

		break;

	case MQTT_EVENT_DISCONNECTED:
		xEventGroupClearBits(common_events_group, 1 << enum_connected_to_mqtt);
		ESP_LOGI("", "MQTT is disconnected \n");

		break;

	case MQTT_EVENT_DATA:
		ESP_LOGI("", "MQTT_EVENT_DATA");
		ESP_LOGI("","TOPIC=%.*s\r\n", event->topic_len, event->topic);
		ESP_LOGI("","DATA=%.*s\r\n", event->data_len, event->data);
		if(event->data_len < 512)
		{
			memset(rcvd_msg.msg, 0 ,sizeof(rcvd_msg.msg));
			memcpy((void *)rcvd_msg.msg, (void *)event->data, event->data_len);
			rcvd_msg.type = enum_rcvd_msg;
			xQueueSend(queue_mqtt_msg_from_server, (void *)&rcvd_msg, 10);
		}

		break;

	default:
		break;
	}
	return ESP_OK;
}

static void mqtt_on_connect()
{



}

void mqtt_push_to_publish(mqtt_msg_def msg)
{
	xSemaphoreTake(mutex_mqtt_msg_queue_push, portMAX_DELAY);

	if(queue_mqtt_msg_to_server != 0)
	{
		if(xEventGroupGetBits(common_events_group) & (1 << enum_connected_to_mqtt))
		{
			xQueueSend(queue_mqtt_msg_to_server, (void *)&msg, 10);
		}
	}
	xSemaphoreGive(mutex_mqtt_msg_queue_push);
}

void task_mqtt_pub_msg_hanle(void *pV)
{
	mqtt_msg_def msg_to_publish;
	uint8_t retain = 0;
	while(1)
	{
		memset(&msg_to_publish, 0 , sizeof(mqtt_msg_def));
		if(xEventGroupGetBits(common_events_group) & (1 << enum_connected_to_mqtt))
		{
			if(pdTRUE == xQueueReceive(queue_mqtt_msg_to_server, &msg_to_publish, 0))
			{
				strcpy(mqttData, msg_to_publish.msg);
				switch(msg_to_publish.type)
				{
				case enum_ack_msg:
				case enum_info_msg:
				case enum_status_msg:
				default:
					break;
				}
				if(strlen(mqttData) > 0)
				{
					ESP_LOGE("", "publishing: [%s] on [%s] retain [%d]", mqttMsgPubChannel, mqttData, retain);
					esp_mqtt_client_publish(mqtt_client, mqttMsgPubChannel, mqttData, strlen(mqttData), 0 ,retain);
				}
			}
		}
		vTaskDelay(pdMS_TO_TICKS(100));
	}
}

void task_mqtt_rcvd_msg_handle(void *pV)
{
	mqtt_msg_def rcvd_msg;
	while(1)
	{
		memset((void *)&rcvd_msg, 0, sizeof(mqtt_msg_def));
		if(pdTRUE == xQueueReceive(queue_mqtt_msg_from_server, &rcvd_msg, portMAX_DELAY))
		{
			ESP_LOGI("", "Msg = [%s]", rcvd_msg.msg);
			process_cmd(rcvd_msg.msg);
		}
		vTaskDelay(pdMS_TO_TICKS(10));
	}

}