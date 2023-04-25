#include "configuration.h"

#include "assert.h"
#include "byte.h"
#include "event.h"

/* Current encoding format version. */
#define ENCODING_FORMAT 1

int getRaftRole(struct raft *r, raft_id id)
{
    unsigned i;
    for ( i = 0; i < r->configuration.n; i++) {
        if (id == r->configuration.servers[i].id) {
            return r->configuration.servers[i].role;
        }
    }
    return -1;
}

void configurationInit(struct raft_configuration *c)
{
    c->servers = NULL;
    c->n = 0;
    c->phase = RAFT_CONF_NORMAL;
}

void configurationClose(struct raft_configuration *c)
{

    assert(c != NULL);
    assert(c->n == 0 || c->servers != NULL);
    if (c->servers != NULL) {
        raft_free(c->servers);
    }
}

unsigned configurationIndexOf(const struct raft_configuration *c,
                              const raft_id id)
{
    unsigned i;
    assert(c != NULL);
    for (i = 0; i < c->n; i++) {
        if (c->servers[i].id == id) {
            return i;
        }
    }
    return c->n;
}

bool configurationIsVoter(const struct raft_configuration *c,
                          const struct raft_server *s, int group)
{
    bool voter = false;

    if (s->group & RAFT_GROUP_OLD & group)
        voter = (s->role == RAFT_VOTER || s->role == RAFT_LOGGER);

    if (s->group & RAFT_GROUP_NEW & group) {
        assert(c->phase == RAFT_CONF_JOINT);
        voter = voter || (s->role_new == RAFT_VOTER
                            || s->role_new == RAFT_LOGGER);
    }

    return voter;
}

bool configurationIsSpare(const struct raft_configuration *c,
                          const struct raft_server *s, int group)
{
    bool spare = false;

    if (s->group & RAFT_GROUP_OLD & group)
        spare = s->role == RAFT_SPARE;

    if (s->group & RAFT_GROUP_NEW & group) {
        assert(c->phase == RAFT_CONF_JOINT);
        spare = spare && (s->role_new == RAFT_SPARE);
    }

    return spare;
}

int configurationJointToNormalCopy(const struct raft_configuration *src,
                                   struct raft_configuration *dst)
{
    size_t i;
    int rv;

    assert(src->phase == RAFT_CONF_JOINT);
    configurationInit(dst);
    for (i = 0; i < src->n; i++) {
        struct raft_server *server = &src->servers[i];
        if (!(server->group & RAFT_GROUP_NEW))
            continue;
        rv = configurationAdd(dst, server->id, server->role_new,
                              server->role_new,
                              RAFT_GROUP_OLD);
        if (rv != 0) {
            evtErrf("add conf failed id %d role %d", server->id, server->role);
            return rv;
        }
    }
    dst->phase = RAFT_CONF_NORMAL;
    return 0;
}



unsigned configurationIndexOfVoter(const struct raft_configuration *c,
                                   const raft_id id)
{
    unsigned i;
    unsigned j = 0;
    assert(c != NULL);
    assert(id > 0);

    for (i = 0; i < c->n; i++) {
        if (c->servers[i].id == id) {
            if (configurationIsVoter(c, &c->servers[i], RAFT_GROUP_ANY)) {
                return j;
            }
            return c->n;
        }

        if (configurationIsVoter(c, &c->servers[i], RAFT_GROUP_ANY)) {
            j++;
        }
    }

    return c->n;
}

const struct raft_server *configurationGet(const struct raft_configuration *c,
                                           const raft_id id)
{
    size_t i;
    assert(c != NULL);
    assert(id > 0);

    /* Grab the index of the server with the given ID */
    i = configurationIndexOf(c, id);

    if (i == c->n) {
        /* No server with matching ID. */
        return NULL;
    }
    assert(i < c->n);

    return &c->servers[i];
}

unsigned configurationVoterCount(const struct raft_configuration *c, int group)
{
    unsigned i;
    unsigned n = 0;
    assert(c != NULL);

    for (i = 0; i < c->n; i++) {
        if (configurationIsVoter(c, &c->servers[i], group)) {
            n++;
        }
    }
    return n;
}

int configurationCopy(const struct raft_configuration *src,
                      struct raft_configuration *dst)
{
    size_t i;
    int rv;
    configurationInit(dst);
    for (i = 0; i < src->n; i++) {
        struct raft_server *server = &src->servers[i];
        rv = configurationAdd(dst, server->id, server->role, server->role_new,
                              server->group);
        if (rv != 0) {
            evtErrf("add conf failed id %d role %d", server->id, server->role);
            return rv;
        }
    }
    dst->phase = src->phase;
    return 0;
}

int configurationAdd(struct raft_configuration *c,
                     raft_id id,
                     int role,
                     int role_new,
                     int group)
{
    struct raft_server *servers;
    struct raft_server *server;
    size_t i;
    assert(c != NULL);
    assert(id != 0);

    if (role != RAFT_STANDBY && role != RAFT_VOTER && role != RAFT_SPARE && role != RAFT_LOGGER) {
        evtErrf("conf add bad role %d", role);
        return RAFT_BADROLE;
    }

    /* Check that neither the given id or address is already in use */
    for (i = 0; i < c->n; i++) {
        server = &c->servers[i];
        if (server->id == id) {
            evtErrf("conf add duplicated id %llx", id);
            return RAFT_DUPLICATEID;
        }
    }

    /* Grow the servers array.. */
    servers = raft_realloc(c->servers, (c->n + 1) * sizeof *server);
    if (servers == NULL) {
        evtErrf("conf add realloc failed, id %llx role %d", id, role);
        return RAFT_NOMEM;
    }
    c->servers = servers;
    i = c->n;
    /* make sure the array is sorted */
    while (i > 0) {
        server = &servers[i - 1];
        if (server->id > id)
            servers[i--] = *server;
        else
            break;
    }
    /* Fill the newly allocated slot (the last one) with the given details. */
    server = &servers[i];
    server->id = id;
    server->role = role;
    server->role_new = role_new;
    server->group = group;
    c->n++;

    return 0;
}

int configurationRemove(struct raft_configuration *c, const raft_id id)
{
    unsigned i;
    unsigned j;
    struct raft_server *servers;
    assert(c != NULL);

    i = configurationIndexOf(c, id);
    if (i == c->n) {
        evtErrf("conf remove bad id %llx", id);
        return RAFT_BADID;
    }

    assert(i < c->n);

    /* If this is the last server in the configuration, reset everything. */
    if (c->n - 1 == 0) {
        raft_free(c->servers);
        c->n = 0;
        c->servers = NULL;
        return 0;
    }

    /* Create a new servers array. */
    servers = raft_calloc(c->n - 1, sizeof *servers);
    if (servers == NULL) {
        evtErrf("conf remove calloc failed, id %llx", id);
        return RAFT_NOMEM;
    }

    /* Copy the first part of the servers array into a new array, excluding the
     * i'th server. */
    for (j = 0; j < i; j++) {
        servers[j] = c->servers[j];
    }

    /* Copy the second part of the servers array into a new array. */
    for (j = i + 1; j < c->n; j++) {
        servers[j - 1] = c->servers[j];
    }

    /* Release the old servers array */
    raft_free(c->servers);

    c->servers = servers;
    c->n--;

    return 0;
}

void configurationJointRemove(struct raft_configuration *c, raft_id id)
{
    assert(c->phase == RAFT_CONF_NORMAL);
    size_t i;

    for (i = 0; i < c->n; ++i) {
        if (c->servers[i].id == id) {
            c->servers[i].group = RAFT_GROUP_OLD;
            continue;
        }
        c->servers[i].group = RAFT_GROUP_OLD | RAFT_GROUP_NEW;
        c->servers[i].role_new = c->servers[i].role;
    }
    c->phase = RAFT_CONF_JOINT;
}

void configurationJointReset(struct raft_configuration *c)
{
    assert(c->phase == RAFT_CONF_JOINT);
    size_t i;

    for (i = 0; i < c->n; ++i) {
        c->servers[i].group = RAFT_GROUP_OLD;
    }
    c->phase = RAFT_CONF_NORMAL;
}

size_t configurationEncodedSize(const struct raft_configuration *c)
{
    size_t n = 0;
    unsigned i;

    /* We need one byte for the encoding format version */
    n++;
    /* then 8 bytes for phase */
    n += sizeof(uint64_t);
    /* Then 8 bytes for number of servers. */
    n += sizeof(uint64_t);

    /* Then some space for each server. */
    for (i = 0; i < c->n; i++) {
        n += sizeof(uint64_t);            /* Server ID */
        n++;                              /* Voting flag */
        n += sizeof(uint16_t);            /* New group role and group */
    };

    return bytePad64(n);
}


void configurationEncodeToBuf(const struct raft_configuration *c, void *buf)
{
    void *cursor = buf;
    unsigned i;

    /* Encoding format version */
    bytePut8(&cursor, ENCODING_FORMAT);
    bytePut64Unaligned(&cursor, c->phase);
    /* Number of servers. */
    bytePut64Unaligned(&cursor, c->n); /* cursor might not be 8-byte aligned */

    for (i = 0; i < c->n; i++) {
        struct raft_server *server = &c->servers[i];
        bytePut64Unaligned(&cursor, server->id); /* might not be aligned */
        assert(server->role < 255);
        bytePut8(&cursor, (uint8_t)server->role);
        bytePut8(&cursor, (uint8_t)server->role_new);
        bytePut8(&cursor, (uint8_t)server->group);
    };
}

int configurationDecodeFromBuf(const void *buf, struct raft_configuration *c)
{
    size_t i;
    size_t n;

    /* Check the encoding format version */
    if (byteGet8(&buf) != ENCODING_FORMAT) {
        evtErrf("%s", "malformed");
	    return RAFT_MALFORMED;
    }
    /* then read the phase */
    c->phase = (size_t)byteGet64Unaligned(&buf);
    /* Read the number of servers. */
    n = (size_t)byteGet64Unaligned(&buf);

    /* Decode the individual servers. */
    for (i = 0; i < n; i++) {
        raft_id id;
        int role;
        int role_new;
        int group;
        int rv;
        /* Server ID. */
        id = byteGet64Unaligned(&buf);
        /* Role code. */
        role = byteGet8(&buf);
        role_new = byteGet8(&buf);
        group = byteGet8(&buf);
        rv = configurationAdd(c, id, role, role_new, group);
        if (rv != 0) {
            evtErrf("conf add %llx failed", id, rv);
            return rv;
        }
    }

    return 0;
}

static int defaultEncode(void *ptr,
			 const struct raft_configuration *c,
			 struct raft_buffer *buf)
{
    (void)ptr;
    assert(c != NULL);
    assert(buf != NULL);

    /* The configuration can't be empty. */
    assert(c->n > 0);

    buf->len = configurationEncodedSize(c);
    buf->base = raft_entry_malloc(buf->len);
    if (buf->base == NULL) {
        evtErrf("%s", "malloc failed");
        return RAFT_NOMEM;
    }

    configurationEncodeToBuf(c, buf->base);

    return 0;
}

static int defaultDecode(void *ptr,
			 const struct raft_buffer *buf,
			 struct raft_configuration *c)
{
    (void)ptr;


    assert(c != NULL);
    assert(buf != NULL);

    /* TODO: use 'if' instead of assert for checking buffer boundaries */
    assert(buf->len > 0);

    /* Check that the target configuration is empty. */
    assert(c->n == 0);
    assert(c->servers == NULL);

    return configurationDecodeFromBuf(buf->base, c);
}

static struct raft_configuration_codec defaultCodec = {
	NULL,
	defaultEncode,
	defaultDecode,
};

static struct raft_configuration_codec *currentCodec = &defaultCodec;

int configurationEncode(const struct raft_configuration *c,
			struct raft_buffer *buf)
{
	return currentCodec->encode(currentCodec->data, c, buf);
}

int configurationDecode(const struct raft_buffer *buf,
			struct raft_configuration *c)
{
	return currentCodec->decode(currentCodec->data, buf, c);
}


void raft_configuration_codec_set(struct raft_configuration_codec *codec)
{
	currentCodec = codec;
}

void raft_configuration_codec_set_default(void)
{
	currentCodec = &defaultCodec;
}

int configurationServerRole(struct raft_configuration *c, raft_id id)
{
    const struct raft_server *server = configurationGet(c, id);

    if (server == NULL)
        return RAFT_STANDBY;
    if (server->group & RAFT_GROUP_NEW)
        return server->role_new;
    return server->role;
}
