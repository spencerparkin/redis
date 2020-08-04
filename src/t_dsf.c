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

/* A quick explanation of Disjoint Set Forests (DSFs).
 *
 * A DSF is a collection of sets, each set a collection of values.
 * When a value is added to the DSF, it is placed in its own set,
 * and then that set is placed in the DSF.  The main features of a
 * DSF are 1) the ability to quickly tell, given two elements in the DSF,
 * whether or not they belong to the same set; and 2) the ability to
 * quickly merge (or unionize) the sets containing any two given elements.
 * The average time-complexity of either of these two operations is the same
 * as that for simply looking up an element in a dictionary of N things,
 * which is likely O(ln N), where N is the number of elements in the dictionary.
 * The worst-case running time is O(N), but this is never repeated as the
 * data-structure will optimize itself with use.
 */

/*-----------------------------------------------------------------------------
 * Disjoint Set Forest Commands
 *----------------------------------------------------------------------------*/

static dsetf_element *findSetRep(dsetf_element *ele);
static int sameSetRep(dsetf_element *ele_a, dsetf_element *ele_b);

/* Factory method to return a DSF that *can* hold a "value". */
robj *dsetfTypeCreate(void) {
    return createDisjointSetForestObject();
}

/* Add the specified value to the DSF as a singleton set.
 *
 * If the value was already in the DSF, nothing is done and 0 is
 * returned; otherwise, the new element is added and 1 is returned.
 *
 * Note that unlike a set, a DSF does not support removal.
 * We *could* support removal, but it is not a fast operation,
 * nor is it a feature of what makes DSF data-structures useful.
 */
int dsetfTypeAdd(robj *subject, sds value) {
    if (subject->encoding == OBJ_ENCODING_HT) {
        dict *d = subject->ptr;
        dictEntry *de = dictAddRaw(d,value,NULL);
        if (de) {
            dictSetKey(d,de,sdsdup(value)); /* Why? */
            dsetf_element *dsf_ele = zmalloc(sizeof(dsetf_element));
            dsf_ele->rep = NULL;
            dictSetVal(d,de,dsf_ele);
            return 1;
        }
    } else {
        serverPanic("Unknown DSF encoding");
    }
    return 0;
}

/* Tell us if the sets containing the two given elements are
 * indeed the same set.  Return 1 if so; 0, otherwise.  If
 * ether of the two elements are not members of the DSF,
 * (i.e., not members of a set that is a member of the DSF),
 * then -1 is returned.
 */
int dsetfTypeAreComembers(robj* subject, sds value_a, sds value_b) {
    if (subject->encoding == OBJ_ENCODING_HT) {
        dict *d = subject->ptr;
        dictEntry *de_a = dictFind(d,value_a);
        if (!de_a)
            return -1;
        dictEntry *de_b = dictFind(d,value_b);
        if (!de_b)
            return -1;
        return sameSetRep((dsetf_element*)de_a->v.val, (dsetf_element*)de_b->v.val);
    } else {
        serverPanic("Unkown DSF encoding");
    }
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
        dict* d = subject->ptr;
        dictEntry* de_a = dictFind(d, value_a);
        if (!de_a)
            return -1;
        dictEntry* de_b = dictFind(d, value_b);
        if (!de_b)
            return -1;
        dsetf_element* rep_a = findSetRep((dsetf_element*)de_a->v.val);
        dsetf_element* rep_b = findSetRep((dsetf_element*)de_b->v.val);
        if (rep_a == rep_b)
            return 0;
        else {
            /* Arbitrarily choose A over B.  It does not matter which we choose. */
            rep_b->rep = rep_a;
            return 1;
        }
    } else {
        serverPanic("Unknown DSF encoding");
    }
    return 0;
}

static dsetf_element *findSetRep(dsetf_element *ele) {
    dsetf_element *rep = ele;
    while (rep->rep)
        rep = rep->rep;
    /* The following loop is not required for correctness and
     * is merely an optimization.  It does not add to the
     * time-complexity of the operation. */
    while (ele->rep)
    {
        dsetf_element *next_ele = ele->rep;
        ele->rep = rep;
        ele = next_ele;
    }
    return ele;
}

static int sameSetRep(dsetf_element *ele_a, dsetf_element *ele_b) {
    return findSetRep(ele_a) == findSetRep(ele_b);
}