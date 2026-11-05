/**
 * Copyright 2026 Confluent Inc.
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
 * This file defines the ShareConsumer class and its methods.
 * When changing method signatures, parameters, or defaults, you MUST
 * also update the corresponding type definitions in:
 *   src/confluent_kafka/cimpl.pyi
 *
 * Failure to keep both in sync will result in incorrect type hints.
 */

#include "confluent_kafka.h"

/****************************************************************************
 *
 *
 * ShareConsumer
 *
 *
 ****************************************************************************/

typedef struct {
        Handle base;
        /* base.rk, base.u unused: share uses rkshare, no rebalance */

        rd_kafka_share_t *rkshare;

} ShareConsumerHandle;


static void ShareConsumer_clear0(ShareConsumerHandle *self) {
        /* Release the acknowledgement-commit callback registered at runtime
         * via ShareConsumer.set_acknowledgement_commit_callback(). The base
         * Handle callbacks (error_cb, stats_cb, ...) are owned by base and
         * freed by Handle_clear, and on_commit is rejected at config time
         * (see ShareConsumer_reject_incompatible_config), so nothing else
         * needs releasing here. */
        if (self->base.u.ShareConsumer.on_share_acknowledgement_commit) {
                Py_DECREF(
                    self->base.u.ShareConsumer.on_share_acknowledgement_commit);
                self->base.u.ShareConsumer.on_share_acknowledgement_commit =
                    NULL;
        }
}

static int ShareConsumer_clear(ShareConsumerHandle *self) {
        ShareConsumer_clear0(self);
        Handle_clear(&self->base);
        return 0;
}

static void ShareConsumer_dealloc(ShareConsumerHandle *self) {
        PyObject_GC_UnTrack(self);

        ShareConsumer_clear0(self);

        if (self->rkshare) {
                CallState cs;
                rd_kafka_error_t *destroy_error;
                CallState_begin(&self->base, &cs);
                destroy_error = rd_kafka_share_destroy_flags(
                    self->rkshare, RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE);
                CallState_end(&self->base, &cs);
                /* Destroy frees the handle (or leaks it on failure), so either
                 * way we're done with this pointer. */
                self->rkshare = NULL;
                /* If destroy failed the consumer is still alive, but this is
                 * the last reference to it, so it leaks and there is nothing
                 * left to retry with. We can't raise from dealloc, so report
                 * it as an unraisable exception, taking care not to clobber
                 * one that may already be in flight. */
                if (destroy_error) {
                        PyObject *pending_exc;
                        cfl_exception_fetch(&pending_exc);
                        PyErr_Format(PyExc_RuntimeError,
                                     "Failed to destroy share consumer; the "
                                     "handle has been leaked: %s",
                                     rd_kafka_error_string(destroy_error));
                        /* Pass the type, not self: self is at refcount 0, so
                         * WriteUnraisable would re-enter dealloc. */
                        PyErr_WriteUnraisable((PyObject *)Py_TYPE(self));
                        if (pending_exc)
                                cfl_exception_restore(pending_exc);
                        rd_kafka_error_destroy(destroy_error);
                }
        }

        /* TODO KIP-932: once ShareConsumer_clear0 is gone, drop the manual
         * pair above and just call ShareConsumer_clear(self) here. */
        Handle_clear(&self->base);

        Py_TYPE(self)->tp_free((PyObject *)self);
}

static int
ShareConsumer_traverse(ShareConsumerHandle *self, visitproc visit, void *arg) {
        if (self->base.u.ShareConsumer.on_share_acknowledgement_commit)
                Py_VISIT(
                    self->base.u.ShareConsumer.on_share_acknowledgement_commit);
        return Handle_traverse(&self->base, visit, arg);
}


/* Maps a librdkafka error code to the Python exception type to raise, kept in
 * one place so the two raise helpers below can't drift apart. A code that has
 * a natural Python counterpart is raised as that type so callers can catch it
 * idiomatically: a bad argument as ValueError, invalid-state and
 * concurrent-access misuse as their dedicated types. Anything else stays a
 * generic KafkaException. */
static PyObject *ShareConsumer_translate_error(rd_kafka_resp_err_t code) {
        switch (code) {
        case RD_KAFKA_RESP_ERR__STATE:
                return IllegalStateException;
        case RD_KAFKA_RESP_ERR__CONFLICT:
                return ConcurrentModificationException;
        case RD_KAFKA_RESP_ERR__INVALID_ARG:
                return PyExc_ValueError;
        default:
                return KafkaException;
        }
}

/* Raise the translated exception type for a bare error code. KafkaException
 * carries a KafkaError in args[0]; the non-KafkaException types
 * (IllegalStateException / ConcurrentModificationException / ValueError) carry
 * a plain message string — the Python type already conveys the code, and these
 * local errors have no retriable/fatal/abort flags worth preserving. */
static void
ShareConsumer_PyErr_Format(rd_kafka_resp_err_t code, const char *fmt, ...) {
        char buf[512];
        va_list ap;
        const char *msg;
        PyObject *etype;

        if (fmt) {
                va_start(ap, fmt);
                vsnprintf(buf, sizeof(buf), fmt, ap);
                va_end(ap);
        }
        msg   = fmt ? buf : rd_kafka_err2str(code);
        etype = ShareConsumer_translate_error(code);

        if (etype == KafkaException) {
                PyObject *eo = KafkaError_new0(code, "%s", msg);
                if (!eo)
                        return;
                PyErr_SetObject(KafkaException, eo);
                Py_DECREF(eo);
        } else {
                PyErr_SetString(etype, msg);
        }
}

/* Raise the translated exception type for an rd_kafka_error_t, then free it.
 * KafkaException keeps the full KafkaError (code + retriable/fatal/abort
 * flags); the non-KafkaException types keep only librdkafka's error string as
 * the message (the type conveys the code; these errors carry no flags). */
static void ShareConsumer_PyErr_from_error_destroy(rd_kafka_error_t *error) {
        rd_kafka_resp_err_t code = rd_kafka_error_code(error);
        PyObject *etype          = ShareConsumer_translate_error(code);
        if (etype == KafkaException) {
                PyObject *eo = KafkaError_new_from_error_destroy(error);
                if (!eo)
                        return;
                PyErr_SetObject(KafkaException, eo);
                Py_DECREF(eo);
        } else {
                const char *estr = rd_kafka_error_string(error);
                PyErr_SetString(
                    etype, (estr && *estr) ? estr : rd_kafka_err2str(code));
                rd_kafka_error_destroy(error);
        }
}


/**
 * @brief Subscribe to topics.
 */
static PyObject *ShareConsumer_subscribe(ShareConsumerHandle *self,
                                         PyObject *args,
                                         PyObject *kwargs) {
        PyObject *tlist;
        static char *kws[] = {"topics", NULL};
        rd_kafka_topic_partition_list_t *c_topics;
        rd_kafka_resp_err_t err;
        Py_ssize_t i;

        if (!self->rkshare) {
                PyErr_SetString(IllegalStateException,
                                ERR_MSG_SHARE_CONSUMER_CLOSED);
                return NULL;
        }

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O", kws, &tlist))
                return NULL;

        if (!PyList_Check(tlist)) {
                PyErr_SetString(PyExc_TypeError,
                                "expected list of topic unicode strings");
                return NULL;
        }

        /* Build partition list from topic name strings */
        c_topics = rd_kafka_topic_partition_list_new((int)PyList_Size(tlist));
        for (i = 0; i < PyList_Size(tlist); i++) {
                PyObject *o = PyList_GetItem(tlist, i);
                PyObject *uo, *uo8 = NULL;
                if (!(uo = cfl_PyObject_Unistr(o))) {
                        PyErr_SetString(PyExc_TypeError,
                                        "expected list of unicode strings");
                        rd_kafka_topic_partition_list_destroy(c_topics);
                        return NULL;
                }
                /* TODO KIP-932: cfl_PyUnistr_AsUTF8 can return NULL; passing
                 * NULL to rd_kafka_topic_partition_list_add would crash.
                 * Consumer.c has the same gap (pre-existing); fix at least
                 * here for ShareConsumer. */
                rd_kafka_topic_partition_list_add(c_topics,
                                                  cfl_PyUnistr_AsUTF8(uo, &uo8),
                                                  RD_KAFKA_PARTITION_UA);
                Py_XDECREF(uo8);
                Py_DECREF(uo);
        }

        err = rd_kafka_share_subscribe(self->rkshare, c_topics);

        rd_kafka_topic_partition_list_destroy(c_topics);

        if (err) {
                ShareConsumer_PyErr_Format(err, "Failed to subscribe: %s",
                                           rd_kafka_err2str(err));
                return NULL;
        }

        Py_RETURN_NONE;
}


/**
 * @brief Unsubscribe from current subscription.
 */
static PyObject *ShareConsumer_unsubscribe(ShareConsumerHandle *self,
                                           PyObject *ignore) {
        rd_kafka_resp_err_t err;

        if (!self->rkshare) {
                PyErr_SetString(IllegalStateException,
                                ERR_MSG_SHARE_CONSUMER_CLOSED);
                return NULL;
        }

        err = rd_kafka_share_unsubscribe(self->rkshare);

        if (err) {
                ShareConsumer_PyErr_Format(err, "Failed to unsubscribe: %s",
                                           rd_kafka_err2str(err));
                return NULL;
        }

        Py_RETURN_NONE;
}


/**
 * @brief Get current topic subscription.
 */
static PyObject *ShareConsumer_subscription(ShareConsumerHandle *self,
                                            PyObject *ignore) {
        rd_kafka_topic_partition_list_t *c_topics;
        rd_kafka_resp_err_t err;
        PyObject *topics;
        int i;

        if (!self->rkshare) {
                PyErr_SetString(IllegalStateException,
                                ERR_MSG_SHARE_CONSUMER_CLOSED);
                return NULL;
        }

        err = rd_kafka_share_subscription(self->rkshare, &c_topics);

        if (err) {
                ShareConsumer_PyErr_Format(err,
                                           "Failed to get subscription: %s",
                                           rd_kafka_err2str(err));
                return NULL;
        }

        /* Return List[str] of topic names, not List[TopicPartition]. */
        topics = PyList_New(c_topics->cnt);
        if (!topics) {
                rd_kafka_topic_partition_list_destroy(c_topics);
                return NULL;
        }
        for (i = 0; i < c_topics->cnt; i++) {
                PyObject *s = PyUnicode_FromString(c_topics->elems[i].topic);
                if (!s) {
                        Py_DECREF(topics);
                        rd_kafka_topic_partition_list_destroy(c_topics);
                        return NULL;
                }
                PyList_SET_ITEM(topics, i, s);
        }

        rd_kafka_topic_partition_list_destroy(c_topics);

        return topics;
}


/**
 * @brief Poll for a batch of messages from the share consumer.
 *
 */
static PyObject *ShareConsumer_poll(ShareConsumerHandle *self,
                                    PyObject *args,
                                    PyObject *kwargs) {
        double tmout                    = -1.0f;
        static char *kws[]              = {"timeout", NULL};
        rd_kafka_messages_t *rkmessages = NULL;
        size_t rkmessages_size          = 0;
        rd_kafka_error_t *error         = NULL;
        PyObject *msglist;
        PyObject *records;
        /* Cached for the process lifetime; see the lazy lookup below. */
        static PyObject *Messages_type = NULL;
        CallState cs;
        const int CHUNK_TIMEOUT_MS = 200; /* 200ms chunks for signal checking */
        int total_timeout_ms;
        int chunk_timeout_ms;
        int chunk_count = 0;
        size_t i;

        if (!self->rkshare) {
                PyErr_SetString(IllegalStateException,
                                ERR_MSG_SHARE_CONSUMER_CLOSED);
                return NULL;
        }

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|d", kws, &tmout))
                return NULL;

        total_timeout_ms = cfl_timeout_ms(tmout);

        CallState_begin(&self->base, &cs);

        /* Chunked polling pattern for signal interruptibility. */
        while (1) {
                chunk_timeout_ms = calculate_chunk_timeout(
                    total_timeout_ms, chunk_count, CHUNK_TIMEOUT_MS);

                /* chunk_timeout_ms==0 means a non-blocking drain. share_poll()
                 * allocates rkmessages for us and sets it to NULL when there's
                 * nothing to return, so it's safe to call again each loop. */
                error = rd_kafka_share_poll(self->rkshare, chunk_timeout_ms,
                                            &rkmessages);

                /* Exit on error */
                if (error) {
                        break;
                }

                rkmessages_size = rd_kafka_messages_count(rkmessages);

                /* Exit if messages received */
                if (rkmessages_size > 0) {
                        break;
                }

                /* Exit if total timeout has been exhausted. */
                if (chunk_timeout_ms == 0) {
                        break;
                }

                chunk_count++;

                /* Check for Ctrl+C before next chunk */
                if (check_signals_between_chunks(&self->base, &cs)) {
                        rd_kafka_messages_destroy(rkmessages);
                        return NULL;
                }
        }

        if (!CallState_end(&self->base, &cs)) {
                rd_kafka_messages_destroy(rkmessages);
                if (error)
                        rd_kafka_error_destroy(error);
                return NULL;
        }

        /* Handle error from rd_kafka_share_poll() */
        if (error) {
                rd_kafka_messages_destroy(rkmessages);
                ShareConsumer_PyErr_from_error_destroy(error);
                return NULL;
        }

        /* Build Python list from all returned messages. */
        msglist = PyList_New(rkmessages_size);
        if (!msglist) {
                rd_kafka_messages_destroy(rkmessages);
                return NULL;
        }

        for (i = 0; i < rkmessages_size; i++) {
                rd_kafka_message_t *rkm = rd_kafka_messages_get(rkmessages, i);
                PyObject *msgobj        = Message_new0(&self->base, rkm);
                if (!msgobj) {
                        /* Cleanup on Message_new0 failure:
                         * - msglist DECREF releases successfully-built msgobjs
                         *   for indices [0, i) (PyList_SET_ITEM stole their
                         *   refs); their headers were detached into them.
                         * - rd_kafka_messages_destroy() frees every rkm in the
                         *   batch; the detached ones lose only their body, so
                         *   there is no double-free. */
                        Py_DECREF(msglist);
                        rd_kafka_messages_destroy(rkmessages);
                        return NULL;
                }

#ifdef RD_KAFKA_V_HEADERS
                /** Have to detach headers outside Message_new0 because it
                 * declares the rk message as a const */
                rd_kafka_message_detach_headers(
                    rkm, &((Message *)msgobj)->c_headers);
#endif
                PyList_SET_ITEM(msglist, i, msgobj);
        }

        rd_kafka_messages_destroy(rkmessages);

        /* Wrap the plain list in Messages so callers get records() /
         * count() / is_empty(). The class object is the same for the whole
         * process, so look it up on the first poll and hold that reference
         * forever -- this is on the hot path and the per-call import+attr
         * lookup isn't free. The GIL serializes this first init. */
        if (!Messages_type) {
                Messages_type =
                    cfl_PyObject_lookup("confluent_kafka._model", "Messages");
                if (!Messages_type) {
                        Py_DECREF(msglist);
                        return NULL;
                }
                /* Kept for the process lifetime, so it's intentionally never
                 * DECREF'd. */
        }

        /* _from_list takes over msglist as-is (no copy) -- we built it for
         * this and don't touch it again. */
        records =
            PyObject_CallMethod(Messages_type, "_from_list", "O", msglist);
        Py_DECREF(msglist);

        return records;
}


/**
 * @brief Acknowledge previously polled message.
 *
 * Internally delegates to rd_kafka_share_acknowledge_offset() because the
 * Python Message object does not retain the underlying rd_kafka_message_t
 * pointer (Message_new0 copies the fields out into Python objects).
 *
 * TODO KIP-932: Java splits ack APIs by message kind — successful records
 * go through acknowledge(message), error/GAP records through
 * acknowledge_offset(topic, partition, offset), and crossing the wires
 * throws. NJC clients (this one included) accept either. Revisit once the
 * Java-vs-NJC alignment is settled.
 */
static PyObject *ShareConsumer_acknowledge(ShareConsumerHandle *self,
                                           PyObject *args,
                                           PyObject *kwargs) {
        Message *msg      = NULL;
        int ack_type      = (int)RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT;
        PyObject *uo8     = NULL;
        const char *topic = NULL;
        rd_kafka_resp_err_t err;
        static char *kws[] = {"message", "ack_type", NULL};

        if (!self->rkshare) {
                PyErr_SetString(IllegalStateException,
                                ERR_MSG_SHARE_CONSUMER_CLOSED);
                return NULL;
        }

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O!|i", kws,
                                         &MessageType, &msg, &ack_type))
                return NULL;

        /* Validation (ack_type range, topic, partition, offset) is left to
         * librdkafka so every failure surfaces as KafkaException. Pass NULL
         * topic through for the None case rather than raising ValueError. */

        if (msg->topic && msg->topic != Py_None) {
                topic = cfl_PyUnistr_AsUTF8(msg->topic, &uo8);
                if (!topic) {
                        Py_XDECREF(uo8);
                        return NULL;
                }
        }

        err = rd_kafka_share_acknowledge_offset(
            self->rkshare, topic, msg->partition, msg->offset,
            (rd_kafka_share_AcknowledgeType_t)ack_type);

        Py_XDECREF(uo8);

        if (err) {
                ShareConsumer_PyErr_Format(err,
                                           "Failed to acknowledge message: %s",
                                           rd_kafka_err2str(err));
                return NULL;
        }

        Py_RETURN_NONE;
}


/**
 * @brief Acknowledge a message by topic/partition/offset directly.
 */
static PyObject *ShareConsumer_acknowledge_offset(ShareConsumerHandle *self,
                                                  PyObject *args,
                                                  PyObject *kwargs) {
        const char *topic;
        int partition;
        long long offset;
        int ack_type = (int)RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT;
        rd_kafka_resp_err_t err;
        static char *kws[] = {"topic", "partition", "offset", "ack_type", NULL};

        if (!self->rkshare) {
                PyErr_SetString(IllegalStateException,
                                ERR_MSG_SHARE_CONSUMER_CLOSED);
                return NULL;
        }

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "siL|i", kws, &topic,
                                         &partition, &offset, &ack_type))
                return NULL;

        /* ack_type, partition and offset are all validated by
         * rd_kafka_share_acknowledge_offset()*/

        err = rd_kafka_share_acknowledge_offset(
            self->rkshare, topic, (int32_t)partition, (int64_t)offset,
            (rd_kafka_share_AcknowledgeType_t)ack_type);

        if (err) {
                ShareConsumer_PyErr_Format(err,
                                           "Failed to acknowledge offset: %s",
                                           rd_kafka_err2str(err));
                return NULL;
        }

        Py_RETURN_NONE;
}


/**
 * @brief Synchronously commit pending acknowledgements.
 *
 * Implicit mode auto-converts the records returned by the latest poll() to
 * ACCEPT inside librdkafka before sending. Blocks until all broker replies
 * arrive or the timeout expires.
 *
 * @returns dict mapping TopicPartition -> None on success or KafkaException on
 *          per-partition failure. Empty dict when no acknowledgements are
 *          pending.
 */
static PyObject *ShareConsumer_commit_sync(ShareConsumerHandle *self,
                                           PyObject *args,
                                           PyObject *kwargs) {
        /* TODO KIP-932: check if kwargs is needed */
        /* TODO KIP-932: pick the right default timeout */
        double tmout                             = 60.0;
        rd_kafka_error_t *error                  = NULL;
        rd_kafka_topic_partition_list_t *c_parts = NULL;
        PyObject *result                         = NULL;
        CallState cs;
        static char *kws[] = {"timeout", NULL};

        if (!self->rkshare) {
                PyErr_SetString(IllegalStateException,
                                ERR_MSG_SHARE_CONSUMER_CLOSED);
                goto err;
        }

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|d", kws, &tmout))
                goto err;

        CallState_begin(&self->base, &cs);
        error = rd_kafka_share_commit_sync(self->rkshare, cfl_timeout_ms(tmout),
                                           &c_parts);
        if (!CallState_end(&self->base, &cs))
                goto err;

        if (error) {
                ShareConsumer_PyErr_from_error_destroy(error);
                error = NULL;
                goto err;
        }

        /* NULL c_parts just means nothing was pending -- empty dict. */
        if (!c_parts)
                return PyDict_New();

        result = c_parts_to_dict_topic_partition_to_exception(c_parts);
        rd_kafka_topic_partition_list_destroy(c_parts);
        return result;

err:
        if (c_parts)
                rd_kafka_topic_partition_list_destroy(c_parts);
        if (error)
                rd_kafka_error_destroy(error);
        return NULL;
}


/**
 * @brief Asynchronously commit pending acknowledgements.
 *
 * Returns immediately; broker results are delivered via the
 * share_acknowledgement_commit_cb (when configured).
 */
static PyObject *ShareConsumer_commit_async(ShareConsumerHandle *self,
                                            PyObject *ignore) {
        rd_kafka_error_t *error;
        CallState cs;

        if (!self->rkshare) {
                PyErr_SetString(IllegalStateException,
                                ERR_MSG_SHARE_CONSUMER_CLOSED);
                return NULL;
        }

        CallState_begin(&self->base, &cs);
        error = rd_kafka_share_commit_async(self->rkshare);
        if (!CallState_end(&self->base, &cs)) {
                if (error)
                        rd_kafka_error_destroy(error);
                return NULL;
        }

        if (error) {
                ShareConsumer_PyErr_from_error_destroy(error);
                return NULL;
        }

        Py_RETURN_NONE;
}


/**
 * @brief Trampoline for the share-consumer acknowledgement-commit callback.
 */
static void ShareConsumer_acknowledgement_commit_cb(
    rd_kafka_share_t *rkshare,
    rd_kafka_share_partition_offsets_list_t *partitions,
    rd_kafka_resp_err_t err,
    void *opaque) {
        ShareConsumerHandle *self = opaque;
        PyObject *offsets         = NULL;
        PyObject *exception       = NULL;
        PyObject *args            = NULL;
        PyObject *result          = NULL;
        PyObject *cb              = NULL;
        CallState *cs;

        /* Own a ref to the callback for the whole call: the user callback (or
         * a finalizer) can clear or replace the registration mid-flight.
         * INCREF only after CallState_get reacquires the GIL. */
        cb = self->base.u.ShareConsumer.on_share_acknowledgement_commit;
        if (!cb)
                return;

        cs = CallState_get(&self->base);
        Py_INCREF(cb);

        offsets = partitions
                      ? c_share_partition_offsets_list_to_py_dict(partitions)
                      : PyDict_New();
        if (!offsets)
                goto crash;

        if (err) {
                /* Passing NULL for the message makes KafkaError use err's
                 * default string. */
                PyObject *kafka_error = KafkaError_new_or_None(err, NULL);
                exception = PyObject_CallFunctionObjArgs(KafkaException,
                                                         kafka_error, NULL);
                Py_DECREF(kafka_error);
                if (!exception)
                        goto crash;
        } else {
                Py_INCREF(Py_None);
                exception = Py_None;
        }

        args = Py_BuildValue("(OO)", offsets, exception);
        if (!args) {
                ShareConsumer_PyErr_Format(RD_KAFKA_RESP_ERR__FAIL,
                                           "Unable to build callback args");
                goto crash;
        }

        result = PyObject_CallObject(cb, args);
        if (!result)
                goto crash;

        goto done;

crash:
        CallState_fetch_exception(cs);
        CallState_crash(cs);
        /* NULL is fine: rd_kafka_yield() only sets a thread-local flag and
         * never reads the handle. We couldn't pass the real rk
         * since rd_kafka_share_t keeps its rd_kafka_t private.
         * TODO KIP-932: pass the real handle once a
         * share -> rd_kafka_t accessor exists. */
        rd_kafka_yield(NULL);

done:
        Py_XDECREF(cb);
        Py_XDECREF(offsets);
        Py_XDECREF(exception);
        Py_XDECREF(args);
        Py_XDECREF(result);
        CallState_resume(cs);
}


/**
 * @brief Set or clear the acknowledgement-commit callback at runtime.
 *
 * Pass None to clear.
 */
static PyObject *
ShareConsumer_set_acknowledgement_commit_callback(ShareConsumerHandle *self,
                                                  PyObject *args,
                                                  PyObject *kwargs) {
        PyObject *callback = NULL;
        PyObject *old;
        rd_kafka_error_t *error;
        static char *kws[] = {"callback", NULL};

        if (!self->rkshare) {
                PyErr_SetString(IllegalStateException,
                                ERR_MSG_SHARE_CONSUMER_CLOSED);
                return NULL;
        }

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O", kws, &callback))
                return NULL;

        if (callback != Py_None && !PyCallable_Check(callback)) {
                PyErr_SetString(PyExc_TypeError,
                                "callback must be callable or None");
                return NULL;
        }

        error = rd_kafka_share_set_acknowledgement_commit_cb(
            self->rkshare,
            callback == Py_None ? NULL
                                : ShareConsumer_acknowledgement_commit_cb,
            self);

        if (error) {
                ShareConsumer_PyErr_from_error_destroy(error);
                return NULL;
        }

        /* Swap the stashed PyObject only after librdkafka accepts the
         * registration so a failed registration leaves the prior callback
         * intact. Store the new value before dropping the old ref: releasing
         * the old callback may run a finalizer, and GC traverse must not see
         * the field pointing at a freed object. */
        old = self->base.u.ShareConsumer.on_share_acknowledgement_commit;
        if (callback == Py_None) {
                self->base.u.ShareConsumer.on_share_acknowledgement_commit =
                    NULL;
        } else {
                Py_INCREF(callback);
                self->base.u.ShareConsumer.on_share_acknowledgement_commit =
                    callback;
        }
        Py_XDECREF(old);

        Py_RETURN_NONE;
}


/**
 * @brief Set or reset the SASL username/password at runtime.
 *
 * The new credentials are used the next time a connection is opened; a
 * connection that is already up keeps its current login until it reconnects.
 * Only applies to the PLAIN and SCRAM mechanisms.
 *
 * Don't call this from inside the acknowledgement-commit callback: re-entering
 * the consumer there corrupts the in-flight callback's saved state.
 */
static PyObject *ShareConsumer_set_sasl_credentials(ShareConsumerHandle *self,
                                                    PyObject *args,
                                                    PyObject *kwargs) {
        const char *username = NULL;
        const char *password = NULL;
        rd_kafka_error_t *error;
        CallState cs;
        static char *kws[] = {"username", "password", NULL};

        if (!self->rkshare) {
                PyErr_SetString(IllegalStateException,
                                ERR_MSG_SHARE_CONSUMER_CLOSED);
                return NULL;
        }

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "ss", kws, &username,
                                         &password))
                return NULL;

        CallState_begin(&self->base, &cs);
        error = rd_kafka_share_sasl_set_credentials(self->rkshare, username,
                                                    password);
        if (!CallState_end(&self->base, &cs)) {
                if (error) /* Ignore error in favour of callstate exception */
                        rd_kafka_error_destroy(error);
                return NULL;
        }

        if (error) {
                ShareConsumer_PyErr_from_error_destroy(error);
                return NULL;
        }

        Py_RETURN_NONE;
}


/**
 * @brief Close the share consumer.
 */
static PyObject *ShareConsumer_close(ShareConsumerHandle *self,
                                     PyObject *ignore) {
        rd_kafka_error_t *error;
        rd_kafka_error_t *destroy_error;
        CallState cs;

        if (!self->rkshare)
                Py_RETURN_NONE;

        CallState_begin(&self->base, &cs);
        error         = rd_kafka_share_consumer_close(self->rkshare);
        destroy_error = rd_kafka_share_destroy(self->rkshare);
        if (!destroy_error)
                self->rkshare = NULL;
        if (!CallState_end(&self->base, &cs)) {
                if (error)
                        rd_kafka_error_destroy(error);
                if (destroy_error)
                        rd_kafka_error_destroy(destroy_error);
                return NULL;
        }

        /* close was clean — surface the destroy error instead */
        if (!error)
                error = destroy_error;
        /* close already errored — keep it, free destroy's */
        else if (destroy_error)
                rd_kafka_error_destroy(destroy_error);

        if (error) {
                ShareConsumer_PyErr_from_error_destroy(error);
                return NULL;
        }

        Py_RETURN_NONE;
}


/**
 * @brief Context manager entry — returns self.
 */
static PyObject *ShareConsumer_enter(ShareConsumerHandle *self,
                                     PyObject *ignore) {
        Py_INCREF(self);
        return (PyObject *)self;
}

/**
 * @brief Context manager exit — calls close().
 */
static PyObject *ShareConsumer_exit(ShareConsumerHandle *self, PyObject *args) {
        PyObject *exc_type, *exc_value, *exc_traceback;

        if (!PyArg_UnpackTuple(args, "__exit__", 3, 3, &exc_type, &exc_value,
                               &exc_traceback))
                return NULL;

        /* Cleanup: call close() */
        if (self->rkshare) {
                PyObject *result = ShareConsumer_close(self, NULL);
                if (!result)
                        return NULL;
                Py_DECREF(result);
        }

        Py_RETURN_NONE;
}


/**
 * @brief ShareConsumer methods.
 */
static PyMethodDef ShareConsumer_methods[] = {
    {"subscribe", (PyCFunction)ShareConsumer_subscribe,
     METH_VARARGS | METH_KEYWORDS,
     ".. py:function:: subscribe(topics)\n"
     "\n"
     "  Set subscription to supplied list of topics\n"
     "\n"
     "  Subscriptions are not incremental: this list replaces the current\n"
     "  subscription, if any. Passing an empty list is equivalent to\n"
     "  :py:func:`unsubscribe`.\n"
     "\n"
     "  :param list(str) topics: List of topics to subscribe to.\n"
     "  :raises ValueError: if topics contains empty or duplicate names\n"
     "  :raises KafkaException: on other errors\n"
     "  :raises IllegalStateException: if called on a closed share consumer\n"
     "\n"},

    {"unsubscribe", (PyCFunction)ShareConsumer_unsubscribe, METH_NOARGS,
     ".. py:function:: unsubscribe()\n"
     "\n"
     "  Unsubscribe from the current topic subscription.\n"
     "\n"
     "  :raises KafkaException: on error\n"
     "  :raises IllegalStateException: if called on a closed share consumer\n"
     "\n"},

    {"subscription", (PyCFunction)ShareConsumer_subscription, METH_NOARGS,
     ".. py:function:: subscription()\n"
     "\n"
     "  Get current topic subscription.\n"
     "\n"
     "  :returns: List of subscribed topics\n"
     "  :rtype: list(str)\n"
     "  :raises KafkaException: on error\n"
     "  :raises IllegalStateException: if called on a closed share consumer\n"
     "\n"},

    {"poll", (PyCFunction)ShareConsumer_poll, METH_VARARGS | METH_KEYWORDS,
     ".. py:function:: poll([timeout=-1])\n"
     "\n"
     "  Poll for a batch of messages from the share consumer.\n"
     "\n"
     "  The consumer must be subscribed to at least one topic before\n"
     "  polling. In explicit acknowledgement mode, all records returned by\n"
     "  the previous poll must be acknowledged before polling again.\n"
     "\n"
     "  The application must check each Message object's error() method\n"
     "  to distinguish between proper messages (error() returns None)\n"
     "  and errors.\n"
     "\n"
     "  :param float timeout: Maximum time to block waiting for messages "
     "(seconds).\n"
     "                        Default: -1 (infinite)\n"
     "  :returns: Messages, a container of Message objects. "
     "Returns an empty Messages if no messages are available within "
     "the timeout.\n"
     "  :rtype: Messages\n"
     "  :raises KafkaException: on error\n"
     "  :raises IllegalStateException: if the consumer is not subscribed to "
     "any topic, if it is in explicit acknowledgement mode and not all "
     "records from the previous poll have been acknowledged, or if the "
     "share consumer is closed.\n"
     "  :raises KeyboardInterrupt: if Ctrl+C pressed during consumption\n"
     "\n"},

    {"acknowledge", (PyCFunction)ShareConsumer_acknowledge,
     METH_VARARGS | METH_KEYWORDS,
     ".. py:function:: acknowledge(message, "
     "[ack_type=AcknowledgeType.ACCEPT])\n"
     "\n"
     "  Acknowledge a previously polled message in explicit acknowledgement\n"
     "  mode, recording how the broker should treat the record (accept it,\n"
     "  release it for redelivery, or reject it). This only updates local\n"
     "  state; the acknowledgement is sent to the broker on the next\n"
     "  :py:func:`poll`, :py:func:`commit_sync`, or :py:func:`commit_async`.\n"
     "\n"
     "  :param Message message: A message returned by poll().\n"
     "  :param AcknowledgeType ack_type: ACCEPT (default), RELEASE, or "
     "REJECT.\n"
     "  :raises TypeError: if message is not a Message instance or ack_type "
     "is not an integer.\n"
     "  :raises IllegalStateException: if the consumer is not in explicit\n"
     "                          acknowledgement mode, the message is no "
     "longer\n"
     "                          in-flight, or the consumer is closed.\n"
     "  :raises ValueError: if ack_type is invalid, message.topic() is "
     "None, or the partition or offset is negative.\n"
     "\n"},

    {"acknowledge_offset", (PyCFunction)ShareConsumer_acknowledge_offset,
     METH_VARARGS | METH_KEYWORDS,
     ".. py:function:: acknowledge_offset(topic, partition, offset, "
     "[ack_type=AcknowledgeType.ACCEPT])\n"
     "\n"
     "  Acknowledge a message by topic/partition/offset.\n"
     "\n"
     "  Use this when only the coordinates of a message are known and the\n"
     "  Message object returned by :py:func:`poll` is not available. Like\n"
     "  :py:func:`acknowledge` (with which it is otherwise equivalent),\n"
     "  this only updates local state; the acknowledgement is sent to the\n"
     "  broker on the next :py:func:`poll`, :py:func:`commit_sync`, or\n"
     "  :py:func:`commit_async`.\n"
     "\n"
     "  :param str topic: Topic name.\n"
     "  :param int partition: Partition id.\n"
     "  :param int offset: Offset to acknowledge.\n"
     "  :param AcknowledgeType ack_type: ACCEPT (default), RELEASE, or "
     "REJECT.\n"
     "  :raises TypeError: if topic is not a str, partition/offset are not "
     "integers, or ack_type is not an integer.\n"
     "  :raises IllegalStateException: if the consumer is not in explicit\n"
     "                          acknowledgement mode, the offset is not\n"
     "                          in-flight, the offset is a GAP record, or "
     "the\n"
     "                          consumer is closed.\n"
     "  :raises ValueError: if ack_type is invalid or the partition or "
     "offset is negative.\n"
     "\n"},

    {"commit_sync", (PyCFunction)ShareConsumer_commit_sync,
     METH_VARARGS | METH_KEYWORDS,
     ".. py:function:: commit_sync([timeout=60])\n"
     "\n"
     "  Synchronously commit pending acknowledgements and block until the\n"
     "  broker responds or the timeout expires.\n"
     "\n"
     "  In implicit acknowledgement mode "
     "(``share.acknowledgement.mode=implicit``,\n"
     "  the default), all records returned by the previous :py:func:`poll` "
     "call\n"
     "  are auto-converted to ACCEPT before being sent. In explicit mode, "
     "only\n"
     "  records previously passed to :py:func:`acknowledge` /\n"
     "  :py:func:`acknowledge_offset` are sent.\n"
     "\n"
     "  :param float timeout: Maximum time to block (seconds). Default: 60.\n"
     "                        Pass -1 for infinite.\n"
     "  :returns: Dict mapping TopicPartition to None on success or "
     "KafkaException\n"
     "           on per-partition failure. Empty dict when no\n"
     "           acknowledgements are pending.\n"
     "  :rtype: dict(TopicPartition, KafkaException | None)\n"
     "  :raises KafkaException: on error\n"
     "  :raises IllegalStateException: if called on a closed share consumer\n"
     "  :raises TypeError: if timeout is not a float\n"
     "\n"},

    {"commit_async", (PyCFunction)ShareConsumer_commit_async, METH_NOARGS,
     ".. py:function:: commit_async()\n"
     "\n"
     "  Asynchronously commit pending acknowledgements. Returns immediately;\n"
     "  broker results are delivered to the callback registered via\n"
     "  :py:func:`set_acknowledgement_commit_callback`, if any.\n"
     "\n"
     "  :returns: None\n"
     "  :raises KafkaException: on error\n"
     "  :raises IllegalStateException: if called on a closed share consumer\n"
     "  :raises TypeError: if any arguments are passed\n"
     "\n"},

    {"set_acknowledgement_commit_callback",
     (PyCFunction)ShareConsumer_set_acknowledgement_commit_callback,
     METH_VARARGS | METH_KEYWORDS,
     ".. py:function:: set_acknowledgement_commit_callback(callback)\n"
     "\n"
     "  Register a callback invoked with the acknowledged offsets once the\n"
     "  broker responds to an acknowledgement commit. It is always dispatched\n"
     "  on the application thread, from within whichever consumer call is\n"
     "  serving the response queue (:py:func:`poll`, :py:func:`commit_sync`,\n"
     "  :py:func:`commit_async`, or :py:func:`close`), never from a background\n"
     "  thread.\n"
     "\n"
     "  :param callback: A callable\n"
     "      ``callback(offsets, exception)`` where ``offsets`` is a\n"
     "      ``Dict[TopicPartition, set[int]]`` of acknowledged offsets\n"
     "      per partition and ``exception`` is a :py:class:`KafkaException`\n"
     "      on failure or ``None`` on success. Pass ``None`` to clear the\n"
     "      currently registered callback.\n"
     "  :raises TypeError: if ``callback`` is neither callable nor None.\n"
     "  :raises IllegalStateException: if called from within the\n"
     "      acknowledgement-commit callback. This applies to the core share\n"
     "      consumer APIs.\n"
     "  :raises IllegalStateException: if called on a closed share consumer.\n"
     "\n"},

    {"close", (PyCFunction)ShareConsumer_close, METH_NOARGS,
     ".. py:function:: close()\n"
     "\n"
     "  Close the share consumer, cleanly releasing its resources.\n"
     "\n"
     "  Any pending acknowledgements are committed and the consumer leaves\n"
     "  the share group before returning. This blocks until the broker has\n"
     "  confirmed the outstanding acknowledgements and the group has been\n"
     "  left, up to roughly ``socket.timeout.ms``.\n"
     "\n"
     "  :raises KafkaException: on error\n"
     "\n"},

    {"set_sasl_credentials", (PyCFunction)ShareConsumer_set_sasl_credentials,
     METH_VARARGS | METH_KEYWORDS,
     ".. py:function:: set_sasl_credentials(username, password)\n"
     "\n"
     "  Set or reset the SASL credentials used by this share consumer.\n"
     "  The new credentials overwrite the previous ones and take effect the\n"
     "  next time the consumer (re)authenticates to a broker; existing broker\n"
     "  connections are not disconnected. Applicable only to the SASL PLAIN\n"
     "  and SCRAM mechanisms.\n"
     "\n"
     "  :param str username: New SASL username.\n"
     "  :param str password: New SASL password.\n"
     "  :raises TypeError: if username or password is not a str.\n"
     "  :raises KafkaException: on error.\n"
     "  :raises IllegalStateException: if called on a closed share consumer.\n"
     "\n"},

    {"__enter__", (PyCFunction)ShareConsumer_enter, METH_NOARGS,
     "Context manager entry."},
    {"__exit__", (PyCFunction)ShareConsumer_exit, METH_VARARGS,
     "Context manager exit. Automatically closes the share consumer."},

    {NULL}};


/**
 * @brief Reject config keys that don't apply to share consumers before
 *        common_conf_setup sees them. Scans both the positional config
 *        dict (args[0]) and kwargs; common_conf_setup merges them later
 *        with kwargs winning, so checking presence in either source
 *        matches the eventual resolved view. Keeps confluent_kafka.c
 *        ignorant of share consumers — no flag on Handle, no
 *        share-specific branches in the shared config setup path.
 *
 *        Only on_commit lands here. It's a binding-level offset-commit
 *        callback rather than a conf property, so it can't be caught
 *        further down; share consumers acknowledge records instead of
 *        committing offsets, so the callback would never fire. Reject it
 *        outright rather than hold a callback that does nothing.
 *
 * @returns 0 if no rejected keys are present, -1 on rejection (a Python
 *          ValueError is set).
 */
static int ShareConsumer_reject_incompatible_config(PyObject *args,
                                                    PyObject *kwargs) {
        static const char *const share_rejected_keys[] = {"on_commit", NULL};
        PyObject *positional_dict                      = NULL;
        int i;

        if (args && PyTuple_Check(args) && PyTuple_Size(args) >= 1) {
                PyObject *cand = PyTuple_GetItem(args, 0);
                if (cand && PyDict_Check(cand))
                        positional_dict = cand;
        }
        for (i = 0; share_rejected_keys[i]; i++) {
                const char *key = share_rejected_keys[i];
                if ((positional_dict &&
                     PyDict_GetItemString(positional_dict, key)) ||
                    (kwargs && PyDict_GetItemString(kwargs, key))) {
                        PyErr_Format(PyExc_ValueError,
                                     "%s is not supported on ShareConsumer",
                                     key);
                        return -1;
                }
        }
        return 0;
}


/**
 * @brief Initialize ShareConsumer.
 */
static int
ShareConsumer_init(PyObject *selfobj, PyObject *args, PyObject *kwargs) {
        ShareConsumerHandle *self = (ShareConsumerHandle *)selfobj;
        char errstr[512];
        rd_kafka_conf_t *conf;

        if (self->rkshare) {
                PyErr_SetString(PyExc_RuntimeError,
                                "ShareConsumer already initialized");
                return -1;
        }

        if (ShareConsumer_reject_incompatible_config(args, kwargs) == -1)
                return -1;

        self->base.type = RD_KAFKA_CONSUMER;
        /* RD_KAFKA_CONSUMER is intentional, not a copy-paste from
         * Consumer.c: it makes common_conf_setup enforce "group.id must be
         * set", which share consumers also need. on_commit is filtered
         * above before reaching common_conf_setup. */
        if (!(conf = common_conf_setup(RD_KAFKA_CONSUMER, &self->base, args,
                                       kwargs)))
                return -1; /* Exception raised by common_conf_setup() */

        self->rkshare =
            rd_kafka_share_consumer_new(conf, errstr, sizeof(errstr));
        if (!self->rkshare) {
                ShareConsumer_PyErr_Format(
                    rd_kafka_last_error(),
                    "Failed to create share consumer: %s", errstr);
                rd_kafka_conf_destroy(conf);
                return -1;
        }

        /* Forward the share consumer's internal log queue onto rk_rep so
         * log_cb fires during share_poll / commit_sync /
         * commit_async drains. common_conf_setup sets log.queue=true
         * whenever `logger` is configured; without this forwarding the
         * records sit in rk_logq forever. Mirrors Consumer.c:1819-1820. */
        if (self->base.logger) {
                rd_kafka_error_t *error =
                    rd_kafka_share_set_log_queue(self->rkshare, NULL);
                if (error) {
                        ShareConsumer_PyErr_Format(
                            rd_kafka_error_code(error),
                            "Failed to set share log queue: %s",
                            rd_kafka_error_string(error));
                        rd_kafka_error_destroy(error);
                        CallState cs;
                        CallState_begin(&self->base, &cs);
                        rd_kafka_error_t *destroy_error =
                            rd_kafka_share_destroy(self->rkshare);
                        if (destroy_error)
                                rd_kafka_error_destroy(destroy_error);
                        CallState_end(&self->base, &cs);
                        self->rkshare = NULL;
                        return -1;
                }
        }

        /* OAuth wiring: enable SASL background callbacks so the
         * oauthbearer_token_refresh_cb fires on librdkafka's own thread
         * during init (otherwise we'd have the chicken-and-egg problem
         * — can't poll until connected, can't connect without a token).
         * Then block until the initial token is set. Mirrors
         * Consumer.c:1812-1833. */
        if (self->base.oauth_cb) {
                rd_kafka_error_t *oauth_err =
                    rd_kafka_share_sasl_background_callbacks_enable(
                        self->rkshare);
                if (oauth_err) {
                        ShareConsumer_PyErr_from_error_destroy(oauth_err);
                        CallState cs;
                        CallState_begin(&self->base, &cs);
                        rd_kafka_error_t *destroy_error =
                            rd_kafka_share_destroy(self->rkshare);
                        if (destroy_error)
                                rd_kafka_error_destroy(destroy_error);
                        CallState_end(&self->base, &cs);
                        self->rkshare = NULL;
                        return -1;
                }

                int ret_wait_oauth = wait_for_oauth_token_set(&self->base);
                if (ret_wait_oauth == -1) {
                        CallState cs;
                        CallState_begin(&self->base, &cs);
                        rd_kafka_error_t *destroy_error =
                            rd_kafka_share_destroy(self->rkshare);
                        if (destroy_error)
                                rd_kafka_error_destroy(destroy_error);
                        CallState_end(&self->base, &cs);
                        self->rkshare = NULL;
                }
                return ret_wait_oauth;
        }

        return 0;
}


/**
 * @brief ShareConsumer __new__ method.
 */
static PyObject *
ShareConsumer_new(PyTypeObject *type, PyObject *args, PyObject *kwargs) {
        return type->tp_alloc(type, 0);
}


/**
 * @brief ShareConsumer type definition.
 */
PyTypeObject ShareConsumerType = {
    PyVarObject_HEAD_INIT(NULL, 0) "cimpl.ShareConsumer", /*tp_name*/
    sizeof(ShareConsumerHandle),                          /*tp_basicsize*/
    0,                                                    /*tp_itemsize*/
    (destructor)ShareConsumer_dealloc,                    /*tp_dealloc*/
    0,                                                    /*tp_print*/
    0,                                                    /*tp_getattr*/
    0,                                                    /*tp_setattr*/
    0,                                                    /*tp_compare*/
    0,                                                    /*tp_repr*/
    0,                                                    /*tp_as_number*/
    0,                                                    /*tp_as_sequence*/
    0,                                                    /*tp_as_mapping*/
    0,                                                    /*tp_hash */
    0,                                                    /*tp_call*/
    0,                                                    /*tp_str*/
    0,                                                    /*tp_getattro*/
    0,                                                    /*tp_setattro*/
    0,                                                    /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE | Py_TPFLAGS_HAVE_GC, /*tp_flags*/
    "A high-level Apache Kafka share consumer\n"
    "\n"
    ".. py:function:: ShareConsumer(config)\n"
    "\n"
    "Create a new ShareConsumer instance using the provided configuration "
    "*dict*.\n"
    "Share consumers enable queue-like consumption where each partition can be "
    "assigned to multiple consumers. Messages are delivered to only one "
    "consumer.\n"
    "\n"
    "Each message returned by :py:func:`poll` is *acquired* for a limited "
    "time and must be acknowledged before its lock expires. An "
    "unacknowledged record is released back to the share group and may be "
    "redelivered, possibly to a different consumer. The lock duration is a "
    "broker/group setting (``group.share.record.lock.duration.ms``, 30 "
    "seconds by default) and is not configured on this client.\n"
    "\n"
    "``share.acknowledgement.mode`` selects how records are acknowledged. In "
    "``implicit`` mode (the default) the records from a poll are accepted "
    "automatically on the next :py:func:`poll` or commit. In ``explicit`` "
    "mode the application must mark each record with :py:func:`acknowledge` "
    "(or :py:func:`acknowledge_offset`) and then call :py:func:`commit_sync` "
    "or :py:func:`commit_async`.\n"
    "\n"
    "The application must call :py:func:`poll` at least once every "
    "``max.poll.interval.ms``; otherwise the broker considers the member "
    "failed and removes it from the group.\n"
    "\n"
    ":param dict config: Configuration properties. At a minimum, "
    "``group.id`` **must** be set and ``bootstrap.servers`` **should** be "
    "set.\n"
    "\n"
    ".. note::\n"
    "   Always call :py:func:`close` when finished, or use the consumer as\n"
    "   a context manager. close() commits pending acknowledgements and\n"
    "   leaves the share group; skipping it leaks resources and holds the\n"
    "   records acquired by this consumer until their locks expire.\n"
    "\n"
    ".. warning::\n"
    "   The share consumer is not thread-safe. A single instance must not\n"
    "   be used concurrently from more than one thread; a method invoked\n"
    "   while another call is already in progress on a different thread\n"
    "   raises :py:class:`ConcurrentModificationException`. Use a separate\n"
    "   consumer instance per thread.\n"
    "\n",                                 /*tp_doc*/
    (traverseproc)ShareConsumer_traverse, /* tp_traverse */
    (inquiry)ShareConsumer_clear,         /* tp_clear */
    0,                                    /* tp_richcompare */
    0,                                    /* tp_weaklistoffset */
    0,                                    /* tp_iter */
    0,                                    /* tp_iternext */
    ShareConsumer_methods,                /* tp_methods */
    0,                                    /* tp_members */
    0,                                    /* tp_getset */
    0,                                    /* tp_base */
    0,                                    /* tp_dict */
    0,                                    /* tp_descr_get */
    0,                                    /* tp_descr_set */
    0,                                    /* tp_dictoffset */
    ShareConsumer_init,                   /* tp_init */
    0,                                    /* tp_alloc */
    ShareConsumer_new                     /* tp_new */
};
