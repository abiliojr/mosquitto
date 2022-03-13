/*
Copyright (c) 2016-2021 Roger Light <roger@atchoo.org>

All rights reserved. This program and the accompanying materials
are made available under the terms of the Eclipse Public License 2.0
and Eclipse Distribution License v1.0 which accompany this distribution.

The Eclipse Public License is available at
   https://www.eclipse.org/legal/epl-2.0/
and the Eclipse Distribution License is available at
  http://www.eclipse.org/org/documents/edl-v10.php.

SPDX-License-Identifier: EPL-2.0 OR BSD-3-Clause

Contributors:
   Abilio Marques - initial implementation and documentation.
*/

#include "config.h"

#include "mosquitto_broker_internal.h"
#include "memory_mosq.h"
#include "utlist.h"


static int plugin__handle_subscribe_single(struct mosquitto__callback *callbacks, enum mosquitto_plugin_event ev_type, struct mosquitto *context, struct mosquitto_base_msg *sub)
{
	struct mosquitto_evt_subscribe event_data;
	struct mosquitto__callback *cb_base;
	int rc = MOSQ_ERR_SUCCESS;

	memset(&event_data, 0, sizeof(event_data));
	event_data.client = context;
	event_data.subscription = sub->topic;
	event_data.qos = sub->qos;

	DL_FOREACH(callbacks, cb_base){
		rc = cb_base->cb(ev_type, &event_data, cb_base->userdata);
		if(rc != MOSQ_ERR_SUCCESS){
			break;
		}

		if(sub->topic != event_data.subscription){
			mosquitto__FREE(sub->topic);
			sub->topic = event_data.subscription;
		}

		if (event_data.qos < sub->qos){
			sub->qos = event_data.qos;
		}
	}

	return rc;
}

int plugin__handle_subscribe(struct mosquitto *context, struct mosquitto_base_msg *original_sub)
{
	int rc = MOSQ_ERR_SUCCESS;

	/* Global plugins */
	rc = plugin__handle_subscribe_single(db.config->security_options.plugin_callbacks.subscribe,
			MOSQ_EVT_SUBSCRIBE, context, original_sub);
	if(rc) return rc;

	if(db.config->per_listener_settings && context->listener){
		rc = plugin__handle_subscribe_single(context->listener->security_options.plugin_callbacks.subscribe,
			MOSQ_EVT_SUBSCRIBE, context, original_sub);
	}

	return rc;
}

int plugin__handle_unsubscribe(struct mosquitto *context, struct mosquitto_base_msg *original_sub)
{
	int rc = MOSQ_ERR_SUCCESS;

	/* Global plugins */
	rc = plugin__handle_subscribe_single(db.config->security_options.plugin_callbacks.unsubscribe,
			MOSQ_EVT_UNSUBSCRIBE, context, original_sub);
	if(rc) return rc;

	if(db.config->per_listener_settings && context->listener){
		rc = plugin__handle_subscribe_single(context->listener->security_options.plugin_callbacks.unsubscribe,
			MOSQ_EVT_UNSUBSCRIBE, context, original_sub);
	}

	return rc;
}
