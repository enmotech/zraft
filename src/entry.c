#include <string.h>
#include <stdint.h>

#include "assert.h"
#include "entry.h"
#include "event.h"

void entryBatchesDestroy(struct raft_entry *entries, const size_t n)
{
    void *batch = NULL;
    size_t i;
    if (entries == NULL) {
        assert(n == 0);
        return;
    }
    assert(n > 0);
    for (i = 0; i < n; i++) {
	if (entries[i].batch == NULL) {
		raft_entry_free(entries[i].buf.base);
		continue;
	}

        if (entries[i].batch != batch) {
            batch = entries[i].batch;
	    raft_free(entries[i].batch);
        }
    }
    raft_free(entries);
}

void entryNonBatchDestroyPrefix(struct raft_entry *entries,
                               size_t n,
                               size_t prefix)
{
	size_t i;
	if (entries == NULL) {
		assert(n == 0);
		return;
	}
	assert(n > 0);
	for (i = 0; i < n; i++) {
		if (i >= prefix)
			break;

		if (entries[i].batch == NULL) {
			raft_entry_free(entries[i].buf.base);
			continue;
		}
	}
}


int entryCopy(const struct raft_entry *src, struct raft_entry *dst)
{
    dst->term = src->term;
    dst->type = src->type;
    dst->buf.len = src->buf.len;
    if (src->buf.len > 0) {
	    dst->buf.base = raft_entry_malloc(dst->buf.len);
	    if (dst->buf.base == NULL) {
                evtErrf("%s", "entry malloc");
                return RAFT_NOMEM;
	    }
	    memcpy(dst->buf.base, src->buf.base, dst->buf.len);
    } else {
	    dst->buf.base = NULL;
    }
    dst->batch = NULL;
    return 0;
}

int entryBatchCopy(const struct raft_entry *src,
                   struct raft_entry **dst,
                   const size_t n)
{
    size_t size = 0;
    void *batch;
    uint8_t *cursor;
    unsigned i;

    if (n == 0) {
        *dst = NULL;
        return 0;
    }

    /* Calculate the total size of the entries content and allocate the
     * batch. */
    for (i = 0; i < n; i++) {
        size += src[i].buf.len;
    }
    if (size > 0) {
	    batch = raft_malloc(size);
	    if (batch == NULL) {
		    evtErrf("%s", "malloc");
		    return RAFT_NOMEM;
	    }
    } else
	    batch = NULL;

    /* Copy the entries. */
    *dst = raft_malloc(n * sizeof **dst);
    if (*dst == NULL) {
        raft_free(batch);
        evtErrf("%s", "malloc");
        return RAFT_NOMEM;
    }

    cursor = batch;

    for (i = 0; i < n; i++) {
        (*dst)[i].term = src[i].term;
        (*dst)[i].type = src[i].type;
        (*dst)[i].buf.base = cursor;
        (*dst)[i].buf.len = src[i].buf.len;
        (*dst)[i].batch = batch;
	if (src[i].buf.len > 0) {
		memcpy((*dst)[i].buf.base, src[i].buf.base, src[i].buf.len);
		cursor += src[i].buf.len;
	} else {
		(*dst)[i].buf.base = NULL;
	}
    }
    return 0;
}
