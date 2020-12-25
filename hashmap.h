#ifndef HASHMAP_H
#define HASHMAP_H

#include <stdlib.h>
#include <pthread.h>

struct entry{
    void* key;
    void* value;
    struct entry* next;

    pthread_mutex_t enrty_lock;
};

struct hash_map{
    struct entry* entries;

    size_t (*hash)(void*); 
    int (*cmp)(void*,void*);
    void (*key_destruct)(void*);
    void (*value_destruct)(void*);

    pthread_mutex_t map_lock;
};

struct hash_map* hash_map_new(size_t (*hash)(void*), int (*cmp)(void*,void*),
    void (*key_destruct)(void*), void (*value_destruct)(void*));

void hash_map_put_entry_move(struct hash_map* map, void* k, void* v);

void hash_map_remove_entry(struct hash_map* map, void* k);

void* hash_map_get_value_ref(struct hash_map* map, void* k);

void hash_map_destroy(struct hash_map* map);

#endif
