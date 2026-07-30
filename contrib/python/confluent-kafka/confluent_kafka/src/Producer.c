/**
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * ⚠️  WARNING: UPDATE TYPE STUBS WHEN MODIFYING INTERFACES ⚠️
 *
 * This file defines the Producer class and its methods.
 * When changing method signatures, parameters, or defaults, you MUST
 * also update the corresponding type definitions in:
 *   src/confluent_kafka/cimpl.pyi
 *
 * Failure to keep both in sync will result in incorrect type hints.
 */

#include "confluent_kafka.h"


/**
 * @brief KNOWN ISSUES
 *
 *  - Partitioners will cause a dead-lock with librdkafka, because:
 *     GIL + topic lock in topic_new  is different lock order than
 *     topic lock in msg_partitioner + GIL.
 *     This needs to be sorted out in librdkafka, preferably making the
 *     partitioner run without any locks taken.
 *     Until this is fixed the partitioner is ignored and librdkafka's
 *     default will be used.
 *
 */



/****************************************************************************
 *
 *
 * Producer
 *
 *
 *
 *
 ****************************************************************************/

/**
 * Per-message state.
 */
struct Producer_msgstate {
        Handle *self;
        PyObject *dr_cb;
};


/**
 * Create a new per-message state.
 * Returns NULL if neither dr_cb or partitioner_cb is set.
 */
static __inline struct Producer_msgstate *
Producer_msgstate_new(Handle *self, PyObject *dr_cb) {
        struct Producer_msgstate *msgstate;

        msgstate       = calloc(1, sizeof(*msgstate));
        msgstate->self = self;

        if (dr_cb) {
                msgstate->dr_cb = dr_cb;
                Py_INCREF(dr_cb);
        }
        return msgstate;
}

static __inline void
Producer_msgstate_destroy(struct Producer_msgstate *msgstate) {
        if (msgstate->dr_cb)
                Py_DECREF(msgstate->dr_cb);
        free(msgstate);
}


static void Producer_clear0(Handle *self) {
        if (self->u.Producer.default_dr_cb) {
                Py_DECREF(self->u.Producer.default_dr_cb);
                self->u.Producer.default_dr_cb = NULL;
        }
}

static int Producer_clear(Handle *self) {
        Producer_clear0(self);
        Handle_clear(self);
        return 0;
}

static void Producer_dealloc(Handle *self) {
        PyObject_GC_UnTrack(self);

        Producer_clear0(self);

        if (self->rk) {
                CallState cs;
                CallState_begin(self, &cs);

                rd_kafka_destroy(self->rk);

                CallState_end(self, &cs);
        }

        Handle_clear(self);

        Py_TYPE(self)->tp_free((PyObject *)self);
}

static int Producer_traverse(Handle *self, visitproc visit, void *arg) {
        if (self->u.Producer.default_dr_cb)
                Py_VISIT(self->u.Producer.default_dr_cb);

        Handle_traverse(self, visit, arg);

        return 0;
}


static void
dr_msg_cb(rd_kafka_t *rk, const rd_kafka_message_t *rkm, void *opaque) {
        struct Producer_msgstate *msgstate = rkm->_private;
        Handle *self                       = opaque;
        CallState *cs;
        PyObject *args;
        PyObject *result;
        PyObject *msgobj;

        if (!msgstate)
                return;

        cs = CallState_get(self);

        if (!msgstate->dr_cb) {
                /* No callback defined */
                goto done;
        }

        /* Skip callback if delivery.report.only.error=true */
        if (self->u.Producer.dr_only_error && !rkm->err)
                goto done;

        msgobj = Message_new0(self, rkm);

        args = Py_BuildValue("(OO)", ((Message *)msgobj)->error, msgobj);

        Py_DECREF(msgobj);

        if (!args) {
                cfl_PyErr_Format(RD_KAFKA_RESP_ERR__FAIL,
                                 "Unable to build callback args");
                CallState_crash(cs);
                goto done;
        }

        result = PyObject_CallObject(msgstate->dr_cb, args);
        Py_DECREF(args);

        if (result)
                Py_DECREF(result);
        else {
                CallState_fetch_exception(cs);
                CallState_crash(cs);
                rd_kafka_yield(rk);
        }

done:
        Producer_msgstate_destroy(msgstate);
        CallState_resume(cs);
}


#if HAVE_PRODUCEV
static rd_kafka_resp_err_t Producer_producev(Handle *self,
                                             const char *topic,
                                             int32_t partition,
                                             const void *value,
                                             size_t value_len,
                                             const void *key,
                                             size_t key_len,
                                             void *opaque,
                                             int64_t timestamp
#ifdef RD_KAFKA_V_HEADERS
                                             ,
                                             rd_kafka_headers_t *headers
#endif
) {

        return rd_kafka_producev(
            self->rk, RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
            RD_KAFKA_V_TOPIC(topic), RD_KAFKA_V_PARTITION(partition),
            RD_KAFKA_V_KEY(key, (size_t)key_len),
            RD_KAFKA_V_VALUE((void *)value, (size_t)value_len),
            RD_KAFKA_V_TIMESTAMP(timestamp),
#ifdef RD_KAFKA_V_HEADERS
            RD_KAFKA_V_HEADERS(headers),
#endif
            RD_KAFKA_V_OPAQUE(opaque), RD_KAFKA_V_END);
}
#else

static rd_kafka_resp_err_t Producer_produce0(Handle *self,
                                             const char *topic,
                                             int32_t partition,
                                             const void *value,
                                             size_t value_len,
                                             const void *key,
                                             size_t key_len,
                                             void *opaque) {
        rd_kafka_topic_t *rkt;
        rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR_NO_ERROR;

        if (!(rkt = rd_kafka_topic_new(self->rk, topic, NULL)))
                return RD_KAFKA_RESP_ERR__INVALID_ARG;

        if (rd_kafka_produce(rkt, partition, RD_KAFKA_MSG_F_COPY, (void *)value,
                             value_len, (void *)key, key_len, opaque) == -1)
                err = rd_kafka_last_error();

        rd_kafka_topic_destroy(rkt);

        return err;
}
#endif


static PyObject *
Producer_produce(Handle *self, PyObject *args, PyObject *kwargs) {
        const char *topic, *value = NULL, *key = NULL;
        Py_ssize_t value_len = 0, key_len = 0;
        int partition     = RD_KAFKA_PARTITION_UA;
        PyObject *headers = NULL, *dr_cb = NULL, *dr_cb2 = NULL;
        long long timestamp = 0;
        rd_kafka_resp_err_t err;
        struct Producer_msgstate *msgstate;
#ifdef RD_KAFKA_V_HEADERS
        rd_kafka_headers_t *rd_headers = NULL;
#endif

        static char *kws[] = {"topic",     "value",       "key", "partition",
                              "callback",  "on_delivery", /* Alias */
                              "timestamp", "headers",     NULL};

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s|z#z#iOOLO", kws,
                                         &topic, &value, &value_len, &key,
                                         &key_len, &partition, &dr_cb, &dr_cb2,
                                         &timestamp, &headers))
                return NULL;

#if !HAVE_PRODUCEV
        if (timestamp) {
                PyErr_Format(PyExc_NotImplementedError,
                             "Producer timestamps require "
                             "confluent-kafka-python built for librdkafka "
                             "version >=v0.9.4 (librdkafka runtime 0x%x, "
                             "buildtime 0x%x)",
                             rd_kafka_version(), RD_KAFKA_VERSION);
                return NULL;
        }
#endif

#ifndef RD_KAFKA_V_HEADERS
        if (headers) {
                PyErr_Format(PyExc_NotImplementedError,
                             "Producer message headers requires "
                             "confluent-kafka-python built for librdkafka "
                             "version >=v0.11.4 (librdkafka runtime 0x%x, "
                             "buildtime 0x%x)",
                             rd_kafka_version(), RD_KAFKA_VERSION);
                return NULL;
        }
#else
        if (headers && headers != Py_None) {
                if (!(rd_headers = py_headers_to_c(headers)))
                        return NULL;
        }
#endif


        if (dr_cb2 && !dr_cb) /* Alias */
                dr_cb = dr_cb2;

        if (!dr_cb || dr_cb == Py_None)
                dr_cb = self->u.Producer.default_dr_cb;

        if (!self->rk) {
#ifdef RD_KAFKA_V_HEADERS
                if (rd_headers)
                        rd_kafka_headers_destroy(rd_headers);
#endif
                PyErr_SetString(PyExc_RuntimeError, ERR_MSG_PRODUCER_CLOSED);
                return NULL;
        }

        /* Create msgstate if necessary, may return NULL if no callbacks
         * are wanted. */
        msgstate = Producer_msgstate_new(self, dr_cb);

        /* Produce message */
#if HAVE_PRODUCEV
        err = Producer_producev(self, topic, partition, value, value_len, key,
                                key_len, msgstate, timestamp
#ifdef RD_KAFKA_V_HEADERS
                                ,
                                rd_headers
#endif
        );
#else
        err = Producer_produce0(self, topic, partition, value, value_len, key,
                                key_len, msgstate);
#endif

        if (err) {
                if (msgstate)
                        Producer_msgstate_destroy(msgstate);
#ifdef RD_KAFKA_V_HEADERS
                if (rd_headers)
                        rd_kafka_headers_destroy(rd_headers);
#endif

                if (err == RD_KAFKA_RESP_ERR__QUEUE_FULL)
                        PyErr_Format(PyExc_BufferError, "%s",
                                     rd_kafka_err2str(err));
                else
                        cfl_PyErr_Format(err, "Unable to produce message: %s",
                                         rd_kafka_err2str(err));

                return NULL;
        }

        Py_RETURN_NONE;
}


/**
 * @brief Poll for producer events with wakeable pattern for interruptibility.
 *
 * This function:
 * 1. Splits the timeout into 200ms chunks
 * 2. Calls rd_kafka_poll() with chunk timeout
 * 3. Between chunks, re-acquires GIL and calls PyErr_CheckSignals()
 * 4. If signal detected, returns -1 (raises KeyboardInterrupt)
 * 5. Continues until events processed, timeout expired, or signal detected
 *
 * @param self Producer handle
 * @param tmout Timeout in milliseconds (-1 for infinite)
 * @returns -1 if callback crashed, signal detected, or poll() failed, else the
 * number of events served.
 */
static int Producer_poll0(Handle *self, int tmout) {
        int r = 0;
        CallState cs;
        const int CHUNK_TIMEOUT_MS = 200; /* 200ms chunks for signal checking */
        int total_timeout_ms       = tmout;
        int chunk_timeout_ms;
        int chunk_count = 0;

        CallState_begin(self, &cs);

        /* Skip wakeable poll pattern for non-blocking or very short timeouts.
         * This avoids unnecessary GIL re-acquisition that can interfere with
         * ThreadPool. Only use wakeable poll for
         * blocking calls that need to be interruptible. */
        if (total_timeout_ms >= 0 && total_timeout_ms < CHUNK_TIMEOUT_MS) {
                r = rd_kafka_poll(self->rk, total_timeout_ms);
        } else {
                while (1) {
                        /* Calculate timeout for this chunk */
                        chunk_timeout_ms = calculate_chunk_timeout(
                            total_timeout_ms, chunk_count, CHUNK_TIMEOUT_MS);
                        if (chunk_timeout_ms == 0) {
                                /* Timeout expired */
                                break;
                        }

                        /* Poll with chunk timeout */
                        int chunk_result =
                            rd_kafka_poll(self->rk, chunk_timeout_ms);
                        /* Error from poll */
                        if (chunk_result < 0) {
                                r = chunk_result;
                                break;
                        }
                        r += chunk_result; /* Accumulate events processed */

                        chunk_count++;

                        /* Check for signals between chunks */
                        if (check_signals_between_chunks(self, &cs)) {
                                return -1; /* Signal detected */
                        }
                }
        }

        if (!CallState_end(self, &cs)) {
                return -1;
        }

        return r;
}


static PyObject *Producer_poll(Handle *self, PyObject *args, PyObject *kwargs) {
        double tmout = -1.0;
        int r;
        static char *kws[] = {"timeout", NULL};

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|d", kws, &tmout))
                return NULL;

        if (!self->rk) {
                PyErr_SetString(PyExc_RuntimeError, ERR_MSG_PRODUCER_CLOSED);
                return NULL;
        }

        r = Producer_poll0(self, cfl_timeout_ms(tmout));
        if (r == -1)
                return NULL;

        return cfl_PyInt_FromInt(r);
}


/**
 * @brief Flush all messages in the producer queue with wakeable pattern for
 * interruptibility.
 *
 * Instead of a single blocking call to rd_kafka_flush() with the
 * full timeout, this function:
 * 1. Splits the timeout into 200ms chunks
 * 2. Calls rd_kafka_flush() with chunk timeout
 * 3. Between chunks, re-acquires GIL and calls PyErr_CheckSignals()
 * 4. If signal detected, returns NULL (raises KeyboardInterrupt)
 * 5. Continues until all messages flushed, timeout expired, or signal detected
 *
 * @param self Producer handle
 * @param args Positional arguments (unused)
 * @param kwargs Keyword arguments:
 *              - timeout (float, optional): Timeout in seconds.
 *                Default: -1.0 (infinite timeout)
 * @return PyObject* Number of messages remaining in queue, or NULL on error
 *         (raises KeyboardInterrupt if signal detected)
 */
static PyObject *
Producer_flush(Handle *self, PyObject *args, PyObject *kwargs) {
        double tmout       = -1;
        int qlen           = 0;
        static char *kws[] = {"timeout", NULL};
        rd_kafka_resp_err_t err;
        CallState cs;
        const int CHUNK_TIMEOUT_MS = 200; /* 200ms chunks for signal checking */
        int total_timeout_ms;
        int chunk_timeout_ms;
        int chunk_count = 0;

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|d", kws, &tmout))
                return NULL;

        if (!self->rk) {
                PyErr_SetString(PyExc_RuntimeError, ERR_MSG_PRODUCER_CLOSED);
                return NULL;
        }

        total_timeout_ms = cfl_timeout_ms(tmout);
        CallState_begin(self, &cs);

        /* Skip wakeable poll pattern for non-blocking or very short timeouts.
         * This avoids unnecessary GIL re-acquisition that can interfere with
         * ThreadPool. Only use wakeable poll for
         * blocking calls that need to be interruptible. */
        if (total_timeout_ms >= 0 && total_timeout_ms < CHUNK_TIMEOUT_MS) {
                err = rd_kafka_flush(self->rk, total_timeout_ms);
        } else {
                /* For infinite timeout, we need to keep looping and checking
                 * for signals. rd_kafka_flush() waits for messages that were in
                 * the queue when it's called. When flush() returns NO_ERROR, it
                 * means all messages that were queued at that point have been
                 * delivered. Note: Messages produced after flush() starts are
                 * not included in the current flush. */
                while (1) {
                        /* Calculate timeout for this chunk */
                        chunk_timeout_ms = calculate_chunk_timeout(
                            total_timeout_ms, chunk_count, CHUNK_TIMEOUT_MS);
                        if (chunk_timeout_ms == 0) {
                                /* Timeout expired */
                                err = RD_KAFKA_RESP_ERR__TIMED_OUT;
                                break;
                        }

                        /* Flush with chunk timeout */
                        err = rd_kafka_flush(self->rk, chunk_timeout_ms);

                        /* Always check for signals between chunks (critical for
                         * interruptibility) */
                        chunk_count++;
                        if (check_signals_between_chunks(self, &cs)) {
                                return NULL; /* Signal detected */
                        }

                        if (err == RD_KAFKA_RESP_ERR_NO_ERROR) {
                                break;
                        }

                        /* If timeout error, continue to next chunk */
                        if (err == RD_KAFKA_RESP_ERR__TIMED_OUT) {
                                continue;
                        }

                        /* Other error - break and return it */
                        break;
                }
        }

        if (!CallState_end(self, &cs))
                return NULL;

        if (err) /* Get the queue length on error (timeout) */
                qlen = rd_kafka_outq_len(self->rk);

        return cfl_PyInt_FromInt(qlen);
}


static PyObject *
Producer_close(Handle *self, PyObject *args, PyObject *kwargs) {
        rd_kafka_resp_err_t err;
        CallState cs;

        if (!self->rk)
                Py_RETURN_TRUE;

        CallState_begin(self, &cs);

        /* Flush any pending messages (wait indefinitely to ensure delivery) */
        err = rd_kafka_flush(self->rk, -1);

        /* Destroy the producer (even if flush had issues) */
        rd_kafka_destroy(self->rk);
        self->rk = NULL;

        if (!CallState_end(self, &cs))
                return NULL;

        /* If flush failed, warn but don't suppress original exception */
        if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
                PyErr_WarnFormat(PyExc_RuntimeWarning, 1,
                                 "Producer flush failed during close: %s",
                                 rd_kafka_err2str(err));
        }

        Py_RETURN_TRUE;
}


/**
 * @brief Validate arguments and parse all messages in the batch
 * @param self Producer handle
 * @param messages_list Python list of message dictionaries
 * @param default_partition Default partition for messages
 * @param default_dr_cb Default delivery callback
 * @param rkmessages Output array of librdkafka messages (must be pre-allocated)
 * @param msgstates Output array of message states (must be pre-allocated)
 * @param message_cnt Output parameter for message count
 * @returns 0 on success, -1 on error (with Python exception set)
 */
static int
Producer_validate_and_parse_batch(Handle *self,
                                  PyObject *messages_list,
                                  int default_partition,
                                  PyObject *default_dr_cb,
                                  rd_kafka_message_t *rkmessages,
                                  struct Producer_msgstate **msgstates,
                                  int *message_cnt) {
        int i;

        /* Validate messages_list is a list */
        if (!PyList_Check(messages_list)) {
                PyErr_SetString(PyExc_TypeError, "messages must be a list");
                return -1;
        }

        *message_cnt = (int)PyList_Size(messages_list);
        if (*message_cnt == 0) {
                return 0; /* Empty batch is valid */
        }

        /* Parse each message in the batch */
        for (i = 0; i < *message_cnt; i++) {
                PyObject *msg_dict  = PyList_GetItem(messages_list, i);
                PyObject *value_obj = NULL, *key_obj = NULL,
                         *headers_obj   = NULL;
                PyObject *partition_obj = NULL, *timestamp_obj = NULL;
                PyObject *callback_obj = NULL;
                const char *value = NULL, *key = NULL;
                Py_ssize_t value_len = 0, key_len = 0;
                int msg_partition   = default_partition;
                PyObject *msg_dr_cb = default_dr_cb;
#ifdef RD_KAFKA_V_HEADERS
                rd_kafka_headers_t *rd_headers = NULL;
#endif

                if (!PyDict_Check(msg_dict)) {
                        PyErr_Format(PyExc_TypeError,
                                     "Message at index %d must be a dict", i);
                        return -1;
                }

                /* Extract message fields */
                value_obj     = PyDict_GetItemString(msg_dict, "value");
                key_obj       = PyDict_GetItemString(msg_dict, "key");
                headers_obj   = PyDict_GetItemString(msg_dict, "headers");
                partition_obj = PyDict_GetItemString(msg_dict, "partition");
                timestamp_obj = PyDict_GetItemString(msg_dict, "timestamp");
                callback_obj  = PyDict_GetItemString(msg_dict, "callback");

                /* Parse value */
                if (value_obj && value_obj != Py_None) {
                        if (PyBytes_Check(value_obj)) {
                                value     = PyBytes_AsString(value_obj);
                                value_len = PyBytes_Size(value_obj);
                        } else if (PyUnicode_Check(value_obj)) {
                                value = PyUnicode_AsUTF8AndSize(value_obj,
                                                                &value_len);
                        } else {
                                PyErr_Format(PyExc_TypeError,
                                             "Message value at index %d must "
                                             "be bytes or str",
                                             i);
                                return -1;
                        }
                }

                /* Parse key */
                if (key_obj && key_obj != Py_None) {
                        if (PyBytes_Check(key_obj)) {
                                key     = PyBytes_AsString(key_obj);
                                key_len = PyBytes_Size(key_obj);
                        } else if (PyUnicode_Check(key_obj)) {
                                key =
                                    PyUnicode_AsUTF8AndSize(key_obj, &key_len);
                        } else {
                                PyErr_Format(PyExc_TypeError,
                                             "Message key at index %d must be "
                                             "bytes or str",
                                             i);
                                return -1;
                        }
                }

                /* Parse partition */
                if (partition_obj && partition_obj != Py_None) {
                        if (!PyLong_Check(partition_obj)) {
                                PyErr_Format(
                                    PyExc_TypeError,
                                    "Message partition at index %d must be int",
                                    i);
                                return -1;
                        }
                        msg_partition = (int)PyLong_AsLong(partition_obj);
                }

                /* Parse timestamp - currently not supported in batch mode */
                if (timestamp_obj && timestamp_obj != Py_None) {
                        PyErr_Format(PyExc_NotImplementedError,
                                     "Message timestamps are not currently "
                                     "supported in batch mode");
                        return -1;
                }

                /* Parse callback */
                if (callback_obj && callback_obj != Py_None) {
                        msg_dr_cb = callback_obj;
                }

#ifdef RD_KAFKA_V_HEADERS
                /* Parse headers */
                if (headers_obj && headers_obj != Py_None) {
                        if (!(rd_headers = py_headers_to_c(headers_obj))) {
                                PyErr_Format(PyExc_ValueError,
                                             "Invalid headers at index %d", i);
                                return -1;
                        }
                }
#endif

                /* Create msgstate for this message */
                msgstates[i] = Producer_msgstate_new(self, msg_dr_cb);

                /* Fill librdkafka message structure */
                rkmessages[i].payload   = (void *)value;
                rkmessages[i].len       = value_len;
                rkmessages[i].key       = (void *)key;
                rkmessages[i].key_len   = key_len;
                rkmessages[i].partition = msg_partition;
                rkmessages[i]._private  = msgstates[i];
                rkmessages[i].err       = RD_KAFKA_RESP_ERR_NO_ERROR;

#ifdef RD_KAFKA_V_HEADERS
                /* Note: headers are not directly supported in
                 * rd_kafka_produce_batch This is a limitation of the current
                 * librdkafka batch API */
                if (rd_headers) {
                        rd_kafka_headers_destroy(rd_headers);
                        rd_headers = NULL;
                        /* Could warn user that headers are ignored in batch
                         * mode */
                }
#endif
        }

        return 0;
}

/**
 * @brief Execute batch produce and handle errors
 * @param messages_list Original Python message list (for error annotation)
 * @param rkt Topic handle
 * @param partition Default partition
 * @param rkmessages Array of librdkafka messages
 * @param msgstates Array of message states
 * @param message_cnt Number of messages
 * @returns Number of messages successfully queued
 */
static int
Producer_execute_and_handle_errors(PyObject *messages_list,
                                   rd_kafka_topic_t *rkt,
                                   int partition,
                                   rd_kafka_message_t *rkmessages,
                                   struct Producer_msgstate **msgstates,
                                   int message_cnt) {
        int good = 0;
        int i;

        /* Call librdkafka batch produce */
        good = rd_kafka_produce_batch(rkt, partition, RD_KAFKA_MSG_F_COPY,
                                      rkmessages, message_cnt);

        /* Handle results and cleanup failed messages */
        for (i = 0; i < message_cnt; i++) {
                if (rkmessages[i].err != RD_KAFKA_RESP_ERR_NO_ERROR) {
                        /* Message failed, cleanup msgstate */
                        if (msgstates[i]) {
                                Producer_msgstate_destroy(msgstates[i]);
                                msgstates[i] = NULL;
                        }

                        /* Set error on the Python message dict for user
                         * feedback */
                        PyObject *msg_dict  = PyList_GetItem(messages_list, i);
                        PyObject *error_obj = KafkaError_new0(
                            rkmessages[i].err, "Message failed: %s",
                            rd_kafka_err2str(rkmessages[i].err));
                        if (error_obj) {
                                PyDict_SetItemString(msg_dict, "_error",
                                                     error_obj);
                                Py_DECREF(error_obj);
                        }
                }
        }

        return good;
}


/**
 * @brief Produce a batch of messages.
 * @returns Number of messages successfully queued for producing.
 */
static PyObject *
Producer_produce_batch(Handle *self, PyObject *args, PyObject *kwargs) {
        const char *topic;
        PyObject *messages_list = NULL;
        int partition           = RD_KAFKA_PARTITION_UA;
        PyObject *dr_cb = NULL, *dr_cb2 = NULL;
        rd_kafka_message_t *rkmessages       = NULL;
        struct Producer_msgstate **msgstates = NULL;
        int message_cnt                      = 0;
        int good                             = 0;
        rd_kafka_topic_t *rkt                = NULL;

        static char *kws[] = {"topic",    "messages",    "partition",
                              "callback", "on_delivery", NULL};

        /* Parse arguments */
        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "sO|iOO", kws, &topic,
                                         &messages_list, &partition, &dr_cb,
                                         &dr_cb2))
                return NULL;

        /* Handle callback aliases */
        if (dr_cb2 && !dr_cb)
                dr_cb = dr_cb2;
        if (!dr_cb || dr_cb == Py_None)
                dr_cb = self->u.Producer.default_dr_cb;

        /* Get preliminary message count for allocation */
        if (!PyList_Check(messages_list)) {
                PyErr_SetString(PyExc_TypeError, "messages must be a list");
                return NULL;
        }

        message_cnt = (int)PyList_Size(messages_list);
        if (message_cnt == 0) {
                return cfl_PyInt_FromInt(0);
        }

        if (!self->rk) {
                PyErr_SetString(PyExc_RuntimeError, ERR_MSG_PRODUCER_CLOSED);
                return NULL;
        }

        /* Allocate arrays for librdkafka messages and msgstates */
        rkmessages = calloc(message_cnt, sizeof(*rkmessages));
        msgstates  = calloc(message_cnt, sizeof(*msgstates));
        if (!rkmessages || !msgstates) {
                PyErr_NoMemory();
                goto cleanup;
        }

        /* Get topic handle */
        if (!(rkt = rd_kafka_topic_new(self->rk, topic, NULL))) {
                cfl_PyErr_Format(RD_KAFKA_RESP_ERR__INVALID_ARG,
                                 "Invalid topic: %s", topic);
                goto cleanup;
        }

        /* FUNCTION 1: Validate arguments and parse all messages */
        if (Producer_validate_and_parse_batch(self, messages_list, partition,
                                              dr_cb, rkmessages, msgstates,
                                              &message_cnt) != 0) {
                goto cleanup;
        }

        /* FUNCTION 2: Execute batch and handle errors */
        good = Producer_execute_and_handle_errors(
            messages_list, rkt, partition, rkmessages, msgstates, message_cnt);

cleanup:
        /* Cleanup resources */
        if (rkt)
                rd_kafka_topic_destroy(rkt);
        if (rkmessages)
                free(rkmessages);
        if (msgstates)
                free(msgstates);

        if (PyErr_Occurred())
                return NULL;

        return cfl_PyInt_FromInt(good);
}

static PyObject *Producer_init_transactions(Handle *self, PyObject *args) {
        CallState cs;
        rd_kafka_error_t *error;
        double tmout = -1.0;

        if (!PyArg_ParseTuple(args, "|d", &tmout))
                return NULL;

        if (!self->rk) {
                PyErr_SetString(PyExc_RuntimeError, ERR_MSG_PRODUCER_CLOSED);
                return NULL;
        }

        CallState_begin(self, &cs);

        error = rd_kafka_init_transactions(self->rk, cfl_timeout_ms(tmout));

        if (!CallState_end(self, &cs)) {
                if (error) /* Ignore error in favour of callstate exception */
                        rd_kafka_error_destroy(error);
                return NULL;
        }

        if (error) {
                cfl_PyErr_from_error_destroy(error);
                return NULL;
        }

        Py_RETURN_NONE;
}

static PyObject *Producer_begin_transaction(Handle *self) {
        rd_kafka_error_t *error;

        if (!self->rk) {
                PyErr_SetString(PyExc_RuntimeError, ERR_MSG_PRODUCER_CLOSED);
                return NULL;
        }

        error = rd_kafka_begin_transaction(self->rk);

        if (error) {
                cfl_PyErr_from_error_destroy(error);
                return NULL;
        }

        Py_RETURN_NONE;
}

static PyObject *Producer_send_offsets_to_transaction(Handle *self,
                                                      PyObject *args) {
        CallState cs;
        rd_kafka_error_t *error;
        PyObject *metadata = NULL, *offsets = NULL;
        rd_kafka_topic_partition_list_t *c_offsets;
        rd_kafka_consumer_group_metadata_t *cgmd;
        double tmout = -1.0;

        if (!PyArg_ParseTuple(args, "OO|d", &offsets, &metadata, &tmout))
                return NULL;

        if (!self->rk) {
                PyErr_SetString(PyExc_RuntimeError, ERR_MSG_PRODUCER_CLOSED);
                return NULL;
        }

        if (!(c_offsets = py_to_c_parts(offsets)))
                return NULL;

        if (!(cgmd = py_to_c_cgmd(metadata))) {
                rd_kafka_topic_partition_list_destroy(c_offsets);
                return NULL;
        }

        CallState_begin(self, &cs);

        error = rd_kafka_send_offsets_to_transaction(self->rk, c_offsets, cgmd,
                                                     cfl_timeout_ms(tmout));

        rd_kafka_consumer_group_metadata_destroy(cgmd);
        rd_kafka_topic_partition_list_destroy(c_offsets);

        if (!CallState_end(self, &cs)) {
                if (error) /* Ignore error in favour of callstate exception */
                        rd_kafka_error_destroy(error);
                return NULL;
        }

        if (error) {
                cfl_PyErr_from_error_destroy(error);
                return NULL;
        }

        Py_RETURN_NONE;
}

static PyObject *Producer_commit_transaction(Handle *self, PyObject *args) {
        CallState cs;
        rd_kafka_error_t *error;
        double tmout = -1.0;

        if (!PyArg_ParseTuple(args, "|d", &tmout))
                return NULL;

        if (!self->rk) {
                PyErr_SetString(PyExc_RuntimeError, ERR_MSG_PRODUCER_CLOSED);
                return NULL;
        }

        CallState_begin(self, &cs);

        error = rd_kafka_commit_transaction(self->rk, cfl_timeout_ms(tmout));

        if (!CallState_end(self, &cs)) {
                if (error) /* Ignore error in favour of callstate exception */
                        rd_kafka_error_destroy(error);
                return NULL;
        }

        if (error) {
                cfl_PyErr_from_error_destroy(error);
                return NULL;
        }

        Py_RETURN_NONE;
}

static PyObject *Producer_abort_transaction(Handle *self, PyObject *args) {
        CallState cs;
        rd_kafka_error_t *error;
        double tmout = -1.0;

        if (!PyArg_ParseTuple(args, "|d", &tmout))
                return NULL;

        if (!self->rk) {
                PyErr_SetString(PyExc_RuntimeError, ERR_MSG_PRODUCER_CLOSED);
                return NULL;
        }

        CallState_begin(self, &cs);

        error = rd_kafka_abort_transaction(self->rk, cfl_timeout_ms(tmout));

        if (!CallState_end(self, &cs)) {
                if (error) /* Ignore error in favour of callstate exception */
                        rd_kafka_error_destroy(error);
                return NULL;
        }

        if (error) {
                cfl_PyErr_from_error_destroy(error);
                return NULL;
        }

        Py_RETURN_NONE;
}

static void *Producer_purge(Handle *self, PyObject *args, PyObject *kwargs) {
        int in_queue       = 1;
        int in_flight      = 1;
        int blocking       = 1;
        int purge_strategy = 0;

        rd_kafka_resp_err_t err;
        static char *kws[] = {"in_queue", "in_flight", "blocking", NULL};

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|bbb", kws, &in_queue,
                                         &in_flight, &blocking))
                return NULL;

        if (!self->rk) {
                PyErr_SetString(PyExc_RuntimeError, ERR_MSG_PRODUCER_CLOSED);
                return NULL;
        }

        if (in_queue)
                purge_strategy = RD_KAFKA_PURGE_F_QUEUE;
        if (in_flight)
                purge_strategy |= RD_KAFKA_PURGE_F_INFLIGHT;
        if (blocking)
                purge_strategy |= RD_KAFKA_PURGE_F_NON_BLOCKING;

        err = rd_kafka_purge(self->rk, purge_strategy);

        if (err) {
                cfl_PyErr_Format(err, "Purge failed: %s",
                                 rd_kafka_err2str(err));
                return NULL;
        }

        Py_RETURN_NONE;
}

static PyObject *Producer_enter(Handle *self) {
        Py_INCREF(self);
        return (PyObject *)self;
}

static PyObject *Producer_exit(Handle *self, PyObject *args) {
        PyObject *exc_type, *exc_value, *exc_traceback;

        if (!PyArg_UnpackTuple(args, "__exit__", 3, 3, &exc_type, &exc_value,
                               &exc_traceback))
                return NULL;

        Producer_close(self, (PyObject *)NULL, (PyObject *)NULL);

        /* Return None to propagate any exceptions from the with block */
        Py_RETURN_NONE;
}


static PyMethodDef Producer_methods[] = {
    {"produce", (PyCFunction)Producer_produce, METH_VARARGS | METH_KEYWORDS,
     ".. py:function:: produce(topic, [value], [key], [partition], "
     "[on_delivery], [timestamp], [headers])\n"
     "\n"
     "  Produce message to topic.\n"
     "  This is an asynchronous operation, an application may use the "
     "``callback`` (alias ``on_delivery``) argument to pass a function "
     "(or lambda) that will be called from :py:func:`poll()` when the "
     "message has been successfully delivered or permanently fails delivery.\n"
     "\n"
     "  Currently message headers are not supported on the message returned to "
     "the "
     "callback. The ``msg.headers()`` will return None even if the original "
     "message "
     "had headers set.\n"
     "\n"
     "  :param str topic: Topic to produce message to\n"
     "  :param str|bytes value: Message payload\n"
     "  :param str|bytes key: Message key\n"
     "  :param int partition: Partition to produce to, else uses the "
     "configured built-in partitioner.\n"
     "  :param func on_delivery(err,msg): Delivery report callback to call "
     "(from :py:func:`poll()` or :py:func:`flush()`) on successful or "
     "failed delivery\n"
     "  :param int timestamp: Message timestamp (CreateTime) in milliseconds "
     "since epoch UTC (requires librdkafka >= v0.9.4, "
     "api.version.request=true, and broker >= 0.10.0.0). Default value is "
     "current time.\n"
     "\n"
     "  :param dict|list headers: Message headers to set on the message. The "
     "header key must be a string while the value must be binary, unicode or "
     "None. Accepts a list of (key,value) or a dict. (Requires librdkafka >= "
     "v0.11.4 and broker version >= 0.11.0.0)\n"
     "  :rtype: None\n"
     "  :raises BufferError: if the internal producer message queue is "
     "full (``queue.buffering.max.messages`` exceeded)\n"
     "  :raises KafkaException: for other errors, see exception code\n"
     "  :raises NotImplementedError: if timestamp is specified without "
     "underlying library support.\n"
     "\n"},

    {"poll", (PyCFunction)Producer_poll, METH_VARARGS | METH_KEYWORDS,
     ".. py:function:: poll([timeout])\n"
     "\n"
     "  Polls the producer for events and calls the corresponding "
     "callbacks (if registered).\n"
     "\n"
     "  Callbacks:\n"
     "\n"
     "  - ``on_delivery`` callbacks from :py:func:`produce()`\n"
     "  - ...\n"
     "\n"
     "  :param float timeout: Maximum time to block waiting for events. "
     "(Seconds)\n"
     "  :returns: Number of events processed (callbacks served)\n"
     "  :rtype: int\n"
     "\n"},
    {"close", (PyCFunction)Producer_close, METH_VARARGS | METH_KEYWORDS,
     ".. py:function:: close()\n"
     "\n"
     "   Request to close the producer on demand.\n"
     "\n"
     "  :rtype: bool\n"
     "  :returns: True if producer close requested successfully, False "
     "otherwise\n"
     "\n"},
    {"flush", (PyCFunction)Producer_flush, METH_VARARGS | METH_KEYWORDS,
     ".. py:function:: flush([timeout])\n"
     "\n"
     "   Wait for all messages in the Producer queue to be delivered.\n"
     "   This is a convenience method that calls :py:func:`poll()` until "
     ":py:func:`len()` is zero or the optional timeout elapses.\n"
     "\n"
     "  :param: float timeout: Maximum time to block (requires librdkafka >= "
     "v0.9.4). (Seconds)\n"
     "  :returns: Number of messages still in queue.\n"
     "\n"
     ".. note:: See :py:func:`poll()` for a description on what "
     "callbacks may be triggered.\n"
     "\n"},

    {"produce_batch", (PyCFunction)Producer_produce_batch,
     METH_VARARGS | METH_KEYWORDS,
     ".. py:function:: produce_batch(topic, messages, [partition], "
     "[on_delivery])\n"
     "\n"
     "  Produce a batch of messages to topic.\n"
     "  This is an asynchronous operation that efficiently sends multiple "
     "messages\n"
     "  in a single batch, reducing overhead compared to individual produce() "
     "calls.\n"
     "\n"
     "  Each message in the batch can have individual delivery callbacks, or "
     "a\n"
     "  single callback can be applied to all messages in the batch.\n"
     "\n"
     "  :param str topic: Topic to produce messages to\n"
     "  :param list messages: List of message dictionaries. Each message dict "
     "can contain:\n"
     "    - 'value' (str|bytes): Message payload (optional)\n"
     "    - 'key' (str|bytes): Message key (optional)\n"
     "    - 'partition' (int): Specific partition (optional, overrides batch "
     "partition)\n"
     "    - 'timestamp' (int): Message timestamp in milliseconds (optional)\n"
     "    - 'callback' (func): Per-message delivery callback (optional)\n"
     "  :param int partition: Default partition for all messages (optional)\n"
     "  :param func on_delivery(err,msg): Default delivery callback for all "
     "messages\n"
     "  :returns: Number of messages successfully queued for producing\n"
     "  :rtype: int\n"
     "\n"
     "  :raises TypeError: if messages is not a list or message format is "
     "invalid\n"
     "  :raises BufferError: if the internal producer message queue is full\n"
     "  :raises KafkaException: for other errors\n"
     "\n"
     "  .. note:: Message headers are not currently supported in batch mode "
     "due to\n"
     "            librdkafka API limitations. Use individual produce() calls "
     "if headers are needed.\n"
     "\n"
     "  .. note:: Failed messages will have an '_error' field added to their "
     "dict\n"
     "            containing the error information.\n"
     "\n"
     "  Example::\n"
     "\n"
     "    messages = [\n"
     "        {'value': 'message 1', 'key': 'key1'},\n"
     "        {'value': 'message 2', 'key': 'key2', 'partition': 1},\n"
     "        {'value': 'message 3', 'callback': my_callback}\n"
     "    ]\n"
     "    count = producer.produce_batch('my-topic', messages)\n"
     "    print(f'Successfully queued {count} messages')\n"
     "\n"},
    {"purge", (PyCFunction)Producer_purge, METH_VARARGS | METH_KEYWORDS,
     ".. py:function:: purge([in_queue=True], [in_flight=True], "
     "[blocking=True])\n"
     "\n"
     "   Purge messages currently handled by the producer instance.\n"
     "   The application will need to call poll() or flush() "
     "afterwards to serve the delivery report callbacks of the purged "
     "messages.\n"
     "\n"
     "  :param: bool in_queue: Purge messages from internal queues. By "
     "default, true.\n"
     "  :param: bool in_flight: Purge messages in flight to or from the "
     "broker. By default, true.\n"
     "  :param: bool blocking: If set to False, will not wait on background "
     "thread queue "
     "purging to finish. By default, true.\n"
     "\n"},
    {"list_topics", (PyCFunction)list_topics, METH_VARARGS | METH_KEYWORDS,
     list_topics_doc},
    {"init_transactions", (PyCFunction)Producer_init_transactions, METH_VARARGS,
     ".. py:function: init_transactions([timeout])\n"
     "\n"
     "  Initializes transactions for the producer instance.\n"
     "\n"
     "  This function ensures any transactions initiated by previous\n"
     "  instances of the producer with the same `transactional.id` are\n"
     "  completed.\n"
     "  If the previous instance failed with a transaction in progress\n"
     "  the previous transaction will be aborted.\n"
     "  This function needs to be called before any other transactional\n"
     "  or produce functions are called when the `transactional.id` is\n"
     "  configured.\n"
     "\n"
     "  If the last transaction had begun completion (following\n"
     "  transaction commit) but not yet finished, this function will\n"
     "  await the previous transaction's completion.\n"
     "\n"
     "  When any previous transactions have been fenced this function\n"
     "  will acquire the internal producer id and epoch, used in all\n"
     "  future transactional messages issued by this producer instance.\n"
     "\n"
     "  Upon successful return from this function the application has to\n"
     "  perform at least one of the following operations within \n"
     "  `transaction.timeout.ms` to avoid timing out the transaction\n"
     "  on the broker:\n"
     "  * produce() (et.al)\n"
     "  * send_offsets_to_transaction()\n"
     "  * commit_transaction()\n"
     "  * abort_transaction()\n"
     "\n"
     "  :param float timeout: Maximum time to block in seconds.\n"
     "\n"
     "  :raises: KafkaError: Use exc.args[0].retriable() to check if the\n"
     "                       operation may be retried, else treat the\n"
     "                       error as a fatal error.\n"},
    {"begin_transaction", (PyCFunction)Producer_begin_transaction, METH_NOARGS,
     ".. py:function:: begin_transaction()\n"
     "\n"
     "  Begin a new transaction.\n"
     "\n"
     "  init_transactions() must have been called successfully (once)\n"
     "  before this function is called.\n"
     "\n"
     "  Any messages produced or offsets sent to a transaction, after\n"
     "  the successful return of this function will be part of the\n"
     "  transaction and committed or aborted atomically.\n"
     "\n"
     "  Complete the transaction by calling commit_transaction() or\n"
     "  Abort the transaction by calling abort_transaction().\n"
     "\n"
     "  :raises: KafkaError: Use exc.args[0].retriable() to check if the\n"
     "                       operation may be retried, else treat the\n"
     "                       error as a fatal error.\n"},
    {"send_offsets_to_transaction",
     (PyCFunction)Producer_send_offsets_to_transaction, METH_VARARGS,
     ".. py:function:: send_offsets_to_transaction(positions,"
     " group_metadata, [timeout])\n"
     "\n"
     "  Sends a list of topic partition offsets to the consumer group\n"
     "  coordinator for group_metadata and marks the offsets as part\n"
     "  of the current transaction.\n"
     "  These offsets will be considered committed only if the\n"
     "  transaction is committed successfully.\n"
     "\n"
     "  The offsets should be the next message your application will\n"
     "  consume, i.e., the last processed message's offset + 1 for each\n"
     "  partition.\n"
     "  Either track the offsets manually during processing or use\n"
     "  consumer.position() (on the consumer) to get the current offsets\n"
     "  for the partitions assigned to the consumer.\n"
     "\n"
     "  Use this method at the end of a consume-transform-produce loop\n"
     "  prior to committing the transaction with commit_transaction().\n"
     "\n"
     "  Note: The consumer must disable auto commits\n"
     "        (set `enable.auto.commit` to false on the consumer).\n"
     "\n"
     "  Note: Logical and invalid offsets (e.g., OFFSET_INVALID) in\n"
     "  offsets will be ignored. If there are no valid offsets in\n"
     "  offsets the function will return successfully and no action\n"
     "  will be taken.\n"
     "\n"
     "  :param list(TopicPartition) offsets: current consumer/processing\n"
     "                                       position(offsets) for the\n"
     "                                       list of partitions.\n"
     "  :param object group_metadata: consumer group metadata retrieved\n"
     "                                from the input consumer's\n"
     "                                get_consumer_group_metadata().\n"
     "  :param float timeout: Amount of time to block in seconds.\n"
     "\n"
     "  :raises: KafkaError: Use exc.args[0].retriable() to check if the\n"
     "           operation may be retried, or\n"
     "           exc.args[0].txn_requires_abort() if the current\n"
     "           transaction has failed and must be aborted by calling\n"
     "           abort_transaction() and then start a new transaction\n"
     "           with begin_transaction().\n"
     "           Treat any other error as a fatal error.\n"
     "\n"},
    {"commit_transaction", (PyCFunction)Producer_commit_transaction,
     METH_VARARGS,
     ".. py:function:: commit_transaction([timeout])\n"
     "\n"
     "  Commmit the current transaction.\n"
     "  Any outstanding messages will be flushed (delivered) before\n"
     "  actually committing the transaction.\n"
     "\n"
     "  If any of the outstanding messages fail permanently the current\n"
     "  transaction will enter the abortable error state and this\n"
     "  function will return an abortable error, in this case the\n"
     "  application must call abort_transaction() before attempting\n"
     "  a new transaction with begin_transaction().\n"
     "\n"
     "  Note: This function will block until all outstanding messages\n"
     "  are delivered and the transaction commit request has been\n"
     "  successfully handled by the transaction coordinator, or until\n"
     "  the timeout expires, which ever comes first. On timeout the\n"
     "  application may call the function again.\n"
     "\n"
     "  Note: Will automatically call flush() to ensure all queued\n"
     "  messages are delivered before attempting to commit the\n"
     "  transaction. Delivery reports and other callbacks may thus be\n"
     "  triggered from this method.\n"
     "\n"
     "  :param float timeout: The amount of time to block in seconds.\n"
     "\n"
     "  :raises: KafkaError: Use exc.args[0].retriable() to check if the\n"
     "           operation may be retried, or\n"
     "           exc.args[0].txn_requires_abort() if the current\n"
     "           transaction has failed and must be aborted by calling\n"
     "           abort_transaction() and then start a new transaction\n"
     "           with begin_transaction().\n"
     "           Treat any other error as a fatal error.\n"
     "\n"},
    {"abort_transaction", (PyCFunction)Producer_abort_transaction, METH_VARARGS,
     ".. py:function:: abort_transaction([timeout])\n"
     "\n"
     "  Aborts the current transaction.\n"
     "  This function should also be used to recover from non-fatal\n"
     "  abortable transaction errors when KafkaError.txn_requires_abort()\n"
     "  is True.\n"
     " \n"
     "  Any outstanding messages will be purged and fail with\n"
     "  _PURGE_INFLIGHT or _PURGE_QUEUE.\n"
     " \n"
     "  Note: This function will block until all outstanding messages\n"
     "  are purged and the transaction abort request has been\n"
     "  successfully handled by the transaction coordinator, or until\n"
     "  the timeout expires, which ever comes first. On timeout the\n"
     "  application may call the function again.\n"
     " \n"
     "  Note: Will automatically call purge() and flush()  to ensure\n"
     "  all queued and in-flight messages are purged before attempting\n"
     "  to abort the transaction.\n"
     "\n"
     "  :param float timeout: The maximum amount of time to block\n"
     "       waiting for transaction to abort in seconds.\n"
     "\n"
     "  :raises: KafkaError: Use exc.args[0].retriable() to check if the\n"
     "           operation may be retried.\n"
     "           Treat any other error as a fatal error.\n"
     "\n"},
    {"set_sasl_credentials", (PyCFunction)set_sasl_credentials,
     METH_VARARGS | METH_KEYWORDS, set_sasl_credentials_doc},
    {"__enter__", (PyCFunction)Producer_enter, METH_NOARGS,
     "Context manager entry."},
    {"__exit__", (PyCFunction)Producer_exit, METH_VARARGS,
     "Context manager exit. Automatically flushes and destroys the producer."},
    {NULL}};


static Py_ssize_t Producer__len__(Handle *self) {
        if (!self->rk)
                return 0;
        return rd_kafka_outq_len(self->rk);
}


static PySequenceMethods Producer_seq_methods = {
    (lenfunc)Producer__len__ /* sq_length */
};

static int Producer__bool__(Handle *self) {
        return 1;
}

static PyNumberMethods Producer_num_methods = {
    0,                         // nb_add
    0,                         // nb_subtract
    0,                         // nb_multiply
    0,                         // nb_remainder
    0,                         // nb_divmod
    0,                         // nb_power
    0,                         // nb_negative
    0,                         // nb_positive
    0,                         // nb_absolute
    (inquiry)Producer__bool__  // nb_bool
};


static int Producer_init(PyObject *selfobj, PyObject *args, PyObject *kwargs) {
        Handle *self = (Handle *)selfobj;
        char errstr[256];
        rd_kafka_conf_t *conf;

        if (self->rk) {
                PyErr_SetString(PyExc_RuntimeError,
                                "Producer already __init__:ialized");
                return -1;
        }

        self->type = RD_KAFKA_PRODUCER;

        if (!(conf = common_conf_setup(RD_KAFKA_PRODUCER, self, args, kwargs)))
                return -1;

        rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);

        self->rk =
            rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
        if (!self->rk) {
                cfl_PyErr_Format(rd_kafka_last_error(),
                                 "Failed to create producer: %s", errstr);
                rd_kafka_conf_destroy(conf);
                return -1;
        }

        /* Enable Token Refresh to be handled by background thread if OAuth
         * callback is provided */
        if (self->oauth_cb) {
                rd_kafka_sasl_background_callbacks_enable(self->rk);
        }

        /* Forward log messages to poll queue */
        if (self->logger)
                rd_kafka_set_log_queue(self->rk, NULL);

        /* Wait for the background thread to set the token. Caller owns
         * destroy on failure — wait_for_oauth_token_set no longer touches
         * self->rk (see refactor note in confluent_kafka.c). */
        if (self->oauth_cb) {
                int ret_wait_oauth = wait_for_oauth_token_set(self);
                if (ret_wait_oauth == -1) {
                        CallState cs;
                        CallState_begin(self, &cs);
                        rd_kafka_destroy(self->rk);
                        CallState_end(self, &cs);
                        self->rk = NULL;
                }
                return ret_wait_oauth;
        }

        return 0;
}


static PyObject *
Producer_new(PyTypeObject *type, PyObject *args, PyObject *kwargs) {
        return type->tp_alloc(type, 0);
}



PyTypeObject ProducerType = {
    PyVarObject_HEAD_INIT(NULL, 0) "cimpl.Producer", /*tp_name*/
    sizeof(Handle),                                  /*tp_basicsize*/
    0,                                               /*tp_itemsize*/
    (destructor)Producer_dealloc,                    /*tp_dealloc*/
    0,                                               /*tp_print*/
    0,                                               /*tp_getattr*/
    0,                                               /*tp_setattr*/
    0,                                               /*tp_compare*/
    0,                                               /*tp_repr*/
    &Producer_num_methods,                           /*tp_as_number*/
    &Producer_seq_methods,                           /*tp_as_sequence*/
    0,                                               /*tp_as_mapping*/
    0,                                               /*tp_hash */
    0,                                               /*tp_call*/
    0,                                               /*tp_str*/
    0,                                               /*tp_getattro*/
    0,                                               /*tp_setattro*/
    0,                                               /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE | Py_TPFLAGS_HAVE_GC, /*tp_flags*/
    "Asynchronous Kafka Producer\n"
    "\n"
    ".. py:function:: Producer(config)\n"
    "\n"
    "  :param dict config: Configuration properties. At a minimum "
    "``bootstrap.servers`` **should** be set\n"
    "\n"
    "  Create a new Producer instance using the provided configuration dict.\n"
    "\n"
    "\n"
    ".. py:function:: __len__(self)\n"
    "\n"
    "  Producer implements __len__ that can be used as len(producer) to obtain "
    "number of messages waiting.\n"
    "  :returns: Number of messages and Kafka protocol requests waiting to be "
    "delivered to broker.\n"
    "  :rtype: int\n"
    "\n",                            /*tp_doc*/
    (traverseproc)Producer_traverse, /* tp_traverse */
    (inquiry)Producer_clear,         /* tp_clear */
    0,                               /* tp_richcompare */
    0,                               /* tp_weaklistoffset */
    0,                               /* tp_iter */
    0,                               /* tp_iternext */
    Producer_methods,                /* tp_methods */
    0,                               /* tp_members */
    0,                               /* tp_getset */
    0,                               /* tp_base */
    0,                               /* tp_dict */
    0,                               /* tp_descr_get */
    0,                               /* tp_descr_set */
    0,                               /* tp_dictoffset */
    Producer_init,                   /* tp_init */
    0,                               /* tp_alloc */
    Producer_new                     /* tp_new */
};
