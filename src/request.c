#include <string.h>
#include "request.h"
#include "assert.h"
#include "event.h"

void requestRegInit(struct request_registry *reg)
{
	reg->slots = NULL;
	reg->size = 0;
	reg->front = reg->back = 0;
}

void requestRegClose(struct request_registry *reg)
{
	raft_free(reg->slots);
	reg->slots = NULL;
}

size_t requestRegNumRequests(struct request_registry *reg)
{
    assert(reg);
    if (reg->front <= reg->back)
        return reg->back - reg->front;

    return reg->size - reg->front + reg->back;
}

static size_t positionAt(struct request_registry *reg, size_t i)
{
    assert(reg->size > 0);
    assert((reg->size & (reg->size - 1)) == 0);

    return (reg->front + i) & (reg->size - 1);
}

static struct request_slot *slotAt(struct request_registry *reg, size_t i)
{
    return &reg->slots[positionAt(reg, i)];
}

static size_t numRequestsForIndex(struct request_registry *reg,
				  raft_index index)
{
	size_t n = requestRegNumRequests(reg);

	if (n == 0)
		return 1;
	assert(slotAt(reg, 0)->index < index);
	return index - slotAt(reg, 0)->index + 1;
}

static int ensureCapacity(struct request_registry *reg, size_t required)
{
    size_t i;
    size_t n;
    size_t size = reg->size == 0 ? 16 : reg->size;
    struct request_slot *slots;

    if (required < reg->size)
        return 0;

    while( required >= size) size <<= 1;

    slots = raft_calloc(size, sizeof(*slots));
    if (slots == NULL) {
        evtErrf("%s", "calloc request slots failed");
        return RAFT_NOMEM;
    }

    n = requestRegNumRequests(reg);
    for (i = 0; i < n; i++)
        memcpy(&slots[i], slotAt(reg, i), sizeof(*slots));

    raft_free(reg->slots);
    reg->slots = slots;
    reg->size = size;
    reg->front = 0;
    reg->back = n;
    return 0;
}

int requestRegEnqueue(struct request_registry *reg, struct request *req)
{
    assert(req);
    int rv;
    size_t back;
    size_t required;
    struct request_slot *slot;

    required = numRequestsForIndex(reg, req->index);
    rv = ensureCapacity(reg, required);
    if (rv != 0) {
        evtErrf("ensure capacity for %llu failed %d", required, rv);
	return rv;
    }

    assert(reg->size > 0);
    assert((reg->size & (reg->size - 1)) == 0);
    back = (reg->front + required - 1) & (reg->size - 1);
    slot = &reg->slots[back];
    slot->req = req;
    slot->index = req->index;

    reg->back = (back + 1) & (reg->size - 1);
    return 0;
}

static void clearFromFront(struct request_registry *reg)
{
    struct request_slot *slot;

    while(requestRegNumRequests(reg)) {
        assert(reg->size > 0);
        assert((reg->size & (reg->size - 1)) == 0);
        slot = &reg->slots[reg->front];
        if (slot->req || slot->index)
            break;
        reg->front = (reg->front + 1) & (reg->size - 1);
    }
}

static void clearFromBack(struct request_registry *reg)
{
    size_t back;
    struct request_slot *slot;

    while(requestRegNumRequests(reg)) {
        assert(reg->size > 0);
        assert((reg->size & (reg->size - 1)) == 0);
        back = (reg->front + requestRegNumRequests(reg) - 1) & (reg->size - 1);
        slot = &reg->slots[back];
        if (slot->req || slot->index)
            break;
        reg->back = back;
    }
}

static struct request_slot *slotForIndex(struct request_registry *reg,
					 raft_index index)
{
    struct request_slot *slot;
    size_t n = requestRegNumRequests(reg);

    if (n <= 0)
        return NULL;

    if (index < slotAt(reg, 0)->index || index > slotAt(reg, n - 1)->index)
	    return NULL;

    slot = slotAt(reg, index - slotAt(reg, 0)->index);
    assert(slot->index == 0 || slot->index == index);
    return slot;
}

struct request *requestRegFind(struct request_registry *reg, raft_index index)
{
    struct request_slot *slot = slotForIndex(reg, index);

    if (slot == NULL)
	    return NULL;
    return slot->req;
}

struct request *requestRegDel(struct request_registry *reg, raft_index index)
{
    struct request *req;
    struct request_slot *slot = slotForIndex(reg, index);

    if (slot == NULL)
	    return NULL;
    req = slot->req;
    slot->req = NULL;
    slot->index = 0;
    clearFromFront(reg);
    clearFromBack(reg);
    return req;
}

struct request *requestRegDequeue(struct request_registry *reg)
{
    size_t n = requestRegNumRequests(reg);

    if (n == 0)
        return NULL;
    return requestRegDel(reg, slotAt(reg, 0)->index);
}

struct request *requestRegFirst(struct request_registry *reg)
{
    struct request_slot *slot;
    size_t n = requestRegNumRequests(reg);

    if (n == 0)
        return NULL;
    slot = slotAt(reg, 0);
    assert(slot->index && slot->req);
    return requestRegFind(reg, slot->index);
}
