/*
 * Copyright (c) 2020, Spencer T. Parkin <spencertparkin at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "server.h"

/* A quick explanation of Disjoint Set Forests (DSFs)...
 *
 * A DSF is a collection of sets, each set a collection of elements.
 * When an element is added to the DSF, it is placed in its own set,
 * and then that set is placed in the DSF.  The main features of a
 * DSF are 1) the ability to quickly tell, given two elements in the DSF,
 * whether or not they belong to the same set; and 2) the ability to
 * quickly merge (or unionize) the sets containing any two given elements.
 * The average time-complexity of either of these two operations is the same
 * as that for simply looking up an element in a dictionary of N things,
 * which is likely O(ln N), where N is the number of elements in the dictionary.
 * The worst-case running time is O(N), but this is never repeated as the
 * data-structure will optimize itself with use.
 *
 * One application of a DSF is as an easy way to generate a random spanning
 * tree of a well connected graph.  Let a graph G(V,E) be a set of edges E
 * and vertices V.  Add all vertices to a DSF.  Load all edges into a list L.
 * Now, until size(L)==0, randomly remove an edge from L, span the vertices
 * connected by the edge if they're not members of the same set in the DSF,
 * then merge the sets containing those vertices if applicable.  The resulting
 * span collection is now a random spanning tree of the graph.
 *
 * See "Introduction to Algorithms", by Cormen, Leiserson & Rivest, chapter 22.
 */

/*-----------------------------------------------------------------------------
 * Disjoint Set Forest Methods
 *----------------------------------------------------------------------------*/

static dsetf_element *findSetRep(dsetf_element *ele);
static int sameSetRep(dsetf_element *ele_a, dsetf_element *ele_b);
static dict* findSet(dsetf_element* given_ele, dsetf* dsf);

/* Factory method to return a DSF that *can* hold a "value". */
robj *dsetfTypeCreate(void) {
    return createDisjointSetForestObject();
}

/* Add the specified element to the DSF as a member of its own singleton set.
 *
 * If the value was already in the DSF, nothing is done and 0 is
 * returned; otherwise, the new element is added and 1 is returned.
 */
int dsetfTypeAdd(robj *subject, sds value) {
    if (subject->encoding == OBJ_ENCODING_HT) {
        dsetf *dsf = subject->ptr;
        dictEntry *de = dictAddRaw(dsf->d, value, NULL);
        if (de) {
            dictSetKey(dsf->d, de, sdsdup(value)); /* Must dup since we want to own the memory here. */
            dsetf_element *dsf_ele = zmalloc(sizeof(dsetf_element));
            dsf_ele->rep = NULL;
            dsf_ele->rank = 1;
            dsf_ele->stale_ele = NULL;
            dsf_ele->stale_rep = NULL;
            dictSetVal(dsf->d, de, dsf_ele);
            dsf->card++;
            return 1;
        }
    } else {
        serverPanic("Unknown DSF encoding");
    }
    return 0;
}

/* Remove the specified element from the DSF.
 *
 * If it was the member of a singleton, then the singleton set is
 * removed from the DSF also.  Note that this always has O(N ln N)
 * time-complexity, where N is the number of elements in the DSF.
 * That may seem discouraging, but removal is not one of the
 * useful features of a DSF.  Nevertheless, it is provided here
 * for completeness.
 */
int dsetfTypeRemove(robj *subject, sds value) {
    if (subject->encoding == OBJ_ENCODING_HT) {
        dsetf *dsf = subject->ptr;
        dictEntry* de = dictFind(dsf->d, value);
        if (!de)
            return -1;
        
        void *doomed_key = de->key;
        dsetf_element *doomed_ele = (dsetf_element*)de->v.val;

        /* The doomed element may or may not be the set representative.
         * In any case, find an element that can safely serve as the
         * new representative of the set.
         */
        void* rep_key = NULL;
        dict* set = findSet(doomed_ele, dsf);
        serverAssert(set != NULL);
        dictIterator *di = dictGetIterator(set);
        while (true) {
            dictEntry *se = dictNext(di);
            if (se == NULL)
                break;
            if (0 != sdscmp(se->key, doomed_key)) {
                rep_key = se->key;
                break;
            }
        }
        dictReleaseIterator(di);

        /* Now simply point all set elements to the new representative.
         */
        if (rep_key != NULL)
        {
            de = dictFind(dsf->d, rep_key);
            serverAssert(de != NULL);
            dsetf_element *rep_ele = de->v.val;

            di = dictGetIterator(set);
            while (true) {
                dictEntry *se = dictNext(di);
                if (se == NULL)
                    break;
                
                de = dictFind(dsf->d, se->key);
                serverAssert(de != NULL);
                dsetf_element *ele = de->v.val;
                
                if (ele != rep_ele)
                    ele->rep = rep_ele;
            }
            dictReleaseIterator(di);

            rep_ele->rep = NULL;
            rep_ele->rank = (dictSize(set) > 2) ? 2 : 1;
        }

        /* At this point, no set elements should be pointing to
         * the doomed element, so it should be safe to delete it.
         */
        dictRelease(set);
        dictDelete(dsf->d, doomed_key);
        return 1;
    } else {
        serverPanic("Unknown DSF encoding");
    }
    return 0;
}

/* Return the set in the DSF containing the given element.
 *
 * This must necessarily iterate all elements of the DSF,
 * so it is not very efficient.  But again, determining the
 * set containing a given element is not a key feature of
 * the DSF data-structure.  Nevertheless, it is provided
 * here for completeness.
 */
dict *dsetfTypeFindSet(robj *subject, sds value) {
    if (subject->encoding == OBJ_ENCODING_HT) {
        dsetf *dsf = subject->ptr;
        dictEntry *de = dictFind(dsf->d, value);
        if (!de)
            return NULL;
        return findSet((dsetf_element*)de->v.val, dsf);
    }
    else {
        serverPanic("Unknown DSF encoding");
    }
    return NULL;
}

/* Tell us if the sets containing the two given elements are
 * indeed the same set.  Return 1 if so; 0, otherwise.  If
 * ether of the two elements are not members of the DSF,
 * (i.e., not members of a set that is a member of the DSF),
 * then -1 is returned.
 */
int dsetfTypeAreComembers(robj* subject, sds value_a, sds value_b) {
    if (subject->encoding == OBJ_ENCODING_HT) {
        dsetf* dsf = subject->ptr;
        dictEntry *de_a = dictFind(dsf->d, value_a);
        if (!de_a)
            return -1;
        dictEntry *de_b = dictFind(dsf->d, value_b);
        if (!de_b)
            return -1;
        return sameSetRep((dsetf_element*)de_a->v.val, (dsetf_element*)de_b->v.val);
    } else {
        serverPanic("Unkown DSF encoding");
    }
    return 0;
}

/* Merge or unionize the sets containing the two given elements.
 *
 * If the two given elements already belong to the same set,
 * then nothing is done and 0 is returned; otherwise, the two
 * sets are merged, and 1 is returned.  If either of the two
 * given elements are not memebers of the DSF, -1 is returned.
 */
int dsetfTypeMerge(robj* subject, sds value_a, sds value_b) {
    if (subject->encoding == OBJ_ENCODING_HT) {
        dsetf* dsf = subject->ptr;
        dictEntry* de_a = dictFind(dsf->d, value_a);
        if (!de_a)
            return -1;
        dictEntry* de_b = dictFind(dsf->d, value_b);
        if (!de_b)
            return -1;
        dsetf_element* rep_a = findSetRep((dsetf_element*)de_a->v.val);
        dsetf_element* rep_b = findSetRep((dsetf_element*)de_b->v.val);
        if (rep_a == rep_b)
            return 0;
        else {
            dsf->card--;
            /* For correctness, it doesn't matter whether A or B becomes
             * the new representative of the set.  However, by using rank,
             * we can make the choice between A and B so as to prevent the
             * rank of the resulting union from growing.  The rank is a
             * rough measure of (upper-bound on) how many jumps it takes
             * to find the representative of a set in the DSF, given one
             * of that set's elements.
             */
            if (rep_a->rank < rep_b->rank)
                rep_a->rep = rep_b;
            else if(rep_b->rank < rep_a->rank)
                rep_b->rep = rep_a;
            else
            {
                /* Here the choice to use A or B is arbitrary, but
                 * we must bump up the rank.
                 */
                rep_a->rep = rep_b;
                rep_b->rank++;
            }
            return 1;
        }
    } else {
        serverPanic("Unknown DSF encoding");
    }
    return 0;
}

/* Retrieve a random element from the DSF.
 *
 * 1 is returned and the given string is populated if the DSF
 * is non-empty; otherwise, zero is returned and the given
 * string is left untouched.
 */
int dsetfTypeRandomElement(robj *subject, sds *sdsele) {
    if(subject->encoding == OBJ_ENCODING_HT) {
        dsetf *dsf = subject->ptr;
        dictEntry *de = dictGetFairRandomKey(dsf->d);
        if (!de)
            return 0;
        *sdsele = dictGetKey(de);
        return 1;
    } else {
        serverPanic("Unknown DSF encoding");
    }
    return 0;
}

/* Retrieve the number of atomic elements in the given DSF.
 * Note that this is not the cardinality of the DSF.
 */
unsigned long dsetfTypeSize(const robj* subject) {
    if (subject->encoding == OBJ_ENCODING_HT) {
        const dsetf* dsf = subject->ptr;
        return dictSize(dsf->d);
    } else {
        serverPanic("Unknown DSF encoding");
    }
    return 0L;
}

/* Retrieve the number of set elements in the given DSF.
 * This is the cardinality of the DSF.
 */
unsigned long dsetfTypeCard(const robj* subject) {
    if (subject->encoding == OBJ_ENCODING_HT) {
        const dsetf* dsf = subject->ptr;
        return dsf->card;
    } else {
        serverPanic("Unknown DSF encoding");
    }
    return 0L;
}

static uint64_t patchPointerDictTypeHash(const void* key) {
    uint64_t value = (uint64_t)key;
    return dictGenHashFunction(&value, sizeof(value));
}

static int patchPointerDictTypeKeyCompare(void* privdata, const void* key1, const void* key2) {
    UNUSED(privdata);
    if ((uint64_t)key1 < (uint64_t)key2)
        return -1;
    if ((uint64_t)key1 > (uint64_t)key2)
        return 1;
    return 0;
}

/* Patch current element value pointers using the stale pointer data.
 *
 * To keep this better than O(N^2), we first build an index.  This will
 * give us a running time of O(N ln N).
 */
bool dsetfTypePatchPointers(robj* subject)
{
    dictType patchPointerDictType = {
        patchPointerDictTypeHash,
        NULL,
        NULL,
        patchPointerDictTypeKeyCompare,
        NULL,
        NULL
    };

    dict* d = dictCreate(&patchPointerDictType, NULL);
    bool patch_succeeded = true;

    /* Build a map from bad pointers to good pointers.
     */
    dsetf* dsf = subject->ptr;
    dictIterator* di = dictGetIterator(dsf->d);
    while (true) {
        dictEntry* de = dictNext(di);
        if (!de)
            break;

        dsetf_element* ele = de->v.val;
        if (DICT_OK != dictAdd(d, ele->stale_ele, ele)) {
            patch_succeeded = false;
            break;
        }
    }
    dictReleaseIterator(di);

    /* Now use the map to patch representative pointers.
     */
    if (patch_succeeded) {
        dictIterator* di = dictGetIterator(dsf->d);
        while (true) {
            dictEntry* de = dictNext(di);
            if (!de)
                break;

            dsetf_element* ele = de->v.val;
            if (ele->stale_rep != NULL) {
                dictEntry* map_de = dictFind(d, ele->stale_rep);
                if (!map_de) {
                    patch_succeeded = false;
                    break;
                }

                ele->rep = map_de->v.val;
                ele->stale_rep = NULL;
            }

            ele->stale_ele = NULL;
        }
        dictReleaseIterator(di);
    }

    dictRelease(d);

    return patch_succeeded;
}

/*-----------------------------------------------------------------------------
 * Disjoint Set Forest Commands
 *----------------------------------------------------------------------------*/

void dsfaddCommand(client *c) {
    robj *dsf = NULL;
    int j = 0, added = 0;

    dsf = lookupKeyWrite(c->db, c->argv[1]);
    if (dsf == NULL) {
        dsf = dsetfTypeCreate();
        dbAdd(c->db, c->argv[1], dsf);
    }

    if (dsf->type != OBJ_DSF) {
        addReply(c, shared.wrongtypeerr);
        return;
    }

    for (j = 2; j < c->argc; j++) {
        if (dsetfTypeAdd(dsf, c->argv[j]->ptr))
            added++;
    }

    if (added > 0) {
        signalModifiedKey(c, c->db, c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_DSF, "dsfadd", c->argv[1], c->db->id);
        server.dirty += added;
    }
    
    addReplyLongLong(c, added);
}

void dsfremCommand(client *c) {
    robj *dsf = NULL;
    int j = 0, deleted = 0;

    dsf = lookupKeyWrite(c->db, c->argv[1]);
    if (dsf == NULL) {
        addReply(c, shared.czero);
        return;
    }
    
    if (dsf->type != OBJ_DSF) {
        addReply(c, shared.wrongtypeerr);
        return;
    }

    for (j = 2; j < c->argc; j++) {
        if (dsetfTypeRemove(dsf, c->argv[j]->ptr)) {
            deleted++;
            if (dsetfTypeSize(dsf) == 0) {
                dbDelete(c->db, c->argv[1]);
                notifyKeyspaceEvent(NOTIFY_GENERIC, "del", c->argv[1], c->db->id);
                break;
            }
        }
    }

    if (deleted > 0) {
        signalModifiedKey(c, c->db, c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_DSF, "dsfrem", c->argv[1], c->db->id);
        server.dirty += deleted;
    }
    
    addReplyLongLong(c, deleted);
}

void dsfarecomembersCommand(client* c) {
    robj *dsf = lookupKeyRead(c->db, c->argv[1]);
    if (dsf == NULL) {
        addReply(c, shared.czero);
        return;
    }
    
    if (dsf->type != OBJ_DSF) {
        addReply(c, shared.wrongtypeerr);
        return;
    }

    if (dsetfTypeAreComembers(dsf, c->argv[2]->ptr, c->argv[3]->ptr))
        addReply(c, shared.cone);
    else
        addReply(c, shared.czero);
}

void dsfunionCommand(client *c) {
    int added = 0;
    
    robj *dsf = lookupKeyWrite(c->db, c->argv[1]);
    if (dsf == NULL) {
        dsf = dsetfTypeCreate();
        dbAdd(c->db, c->argv[1], dsf);
        if (1 == dsetfTypeAdd(dsf, c->argv[2]->ptr))
            added++;
        if (1 == dsetfTypeAdd(dsf, c->argv[3]->ptr))
            added++;
    }

    if (dsf->type != OBJ_DSF) {
        addReply(c, shared.wrongtypeerr);
        return;
    }

    if (added > 0) {
        signalModifiedKey(c, c->db, c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_DSF, "dsfunion", c->argv[1], c->db->id);
        server.dirty += added;
    }

    if (1 == dsetfTypeMerge(dsf, c->argv[2]->ptr, c->argv[3]->ptr))
        addReply(c, shared.cone);
    else
        addReply(c, shared.czero);
}

void dsfcardCommand(client *c) {
    robj* dsf = lookupKeyRead(c->db, c->argv[1]);
    if (dsf == NULL) {
        addReply(c, shared.czero);
        return;
    }

    if (dsf->type != OBJ_DSF) {
        addReply(c, shared.wrongtypeerr);
        return;
    }

    unsigned long card = dsetfTypeCard(dsf);
    addReplyLongLong(c, card);
}

void dsfsizeCommand(client *c) {
    robj *dsf = lookupKeyRead(c->db, c->argv[1]);
    if (dsf == NULL) {
        addReply(c, shared.czero);
        return;
    }

    if (dsf->type != OBJ_DSF) {
        addReply(c, shared.wrongtypeerr);
        return;
    }

    unsigned long size = dsetfTypeSize(dsf);
    addReplyLongLong(c, size);
}

void dsffindsetCommand(client *c) {
    robj *dsf = lookupKeyRead(c->db, c->argv[1]);
    if (dsf == NULL) {
        addReply(c, shared.czero);
        return;
    }

    if (dsf->type != OBJ_DSF) {
        addReply(c, shared.wrongtypeerr);
        return;
    }

    dict* set = dsetfTypeFindSet(dsf, c->argv[2]->ptr);
    if (set == NULL) {
        addReply(c, shared.czero);
    } else {
        addReplyArrayLen(c, dictSize(set));
        dictIterator *di = dictGetIterator(set);
        while (true) {
            dictEntry *de = dictNext(di);
            if (de == NULL)
                break;
            addReplyBulkSds(c, sdsdup(de->key));
        }
        dictRelease(set);
    }
}

void dsfrandmemberCommand(client* c) {
    robj* dsf = lookupKeyRead(c->db, c->argv[1]);
    if (dsf == NULL) {
        addReply(c, shared.czero);
        return;
    }

    if (dsf->type != OBJ_DSF) {
        addReply(c, shared.wrongtypeerr);
        return;
    }

    sds element = sdsempty();
    if (1 != dsetfTypeRandomElement(dsf, &element)) {
        addReply(c, shared.czero);
        return;
    }

    addReplyBulkSds(c, sdsdup(element));
}

static dsetf_element *findSetRep(dsetf_element *ele) {
    serverAssert(ele != NULL);
    dsetf_element *rep = ele;
    while (rep->rep)
        rep = rep->rep;
    /* The following loop is not required for correctness and
     * is merely an optimization.  It does not add to the
     * time-complexity of the operation.  Also, this can
     * technically change the rank of the set, but fixing
     * it up would ruin the time-complexity of the operation.
     */
    while (ele->rep)
    {
        dsetf_element *parent = ele->rep;
        ele->rep = rep;
        ele = parent;
    }
    return ele;
}

static int sameSetRep(dsetf_element *ele_a, dsetf_element *ele_b) {
    return findSetRep(ele_a) == findSetRep(ele_b);
}

static dict *findSet(dsetf_element* given_ele, dsetf *dsf) {
    /* Note that we use the set dictionary type here
     * since we don't own the value memory in the dictionary.
     */
    dict *set = dictCreate(&setDictType, NULL);
    dictIterator *di = dictGetIterator(dsf->d);
    while (true) {
        dictEntry *de = dictNext(di);
        if (de == NULL)
            break;
        dsetf_element *ele = (dsetf_element*)de->v.val;
        if (sameSetRep(given_ele, ele)) {
            dictSetVal(set, dictAddRaw(set, sdsdup(de->key), NULL), ele);
        }
    }
    dictReleaseIterator(di);
    return set;
}
