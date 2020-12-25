#include "hashmap.h"

#include <stdlib.h>

#define T_SIZE (1000)                    // the hash table size
#define ENTRY(map, i) (map->entries[i])  // the i-th entry

struct hash_map* hash_map_new(size_t (*hash)(void*), int (*cmp)(void*, void*),
                              void (*key_destruct)(void*),
                              void (*value_destruct)(void*)) {
  if (hash == NULL || cmp == NULL || key_destruct == NULL ||
      value_destruct == NULL) {
    return NULL;
  }

  // init map
  struct hash_map* map = (struct hash_map*)malloc(sizeof(struct hash_map));
  map->entries = (struct entry*)malloc(sizeof(struct entry) * T_SIZE);

  // set up entries in map
  int i;
  for (i = 0; i < T_SIZE; i++) {
    // The first of each chain does not hold value
    map->entries[i].value = NULL;
    map->entries[i].key = NULL;
    map->entries[i].next = NULL;
    pthread_mutex_init(&(map->entries[i].enrty_lock), NULL);  // map lock
  }

  // init functions
  map->hash = hash;
  map->cmp = cmp;
  map->key_destruct = key_destruct;
  map->value_destruct = value_destruct;

  // init lock
  pthread_mutex_init(&map->map_lock, NULL);  // map lock
  return map;
}

void hash_map_put_entry_move(struct hash_map* map, void* k, void* v) {
  if (map == NULL || k == NULL || v == NULL) {
    return;
  }

  size_t index = map->hash(k) % T_SIZE;

  struct entry* ptr = &ENTRY(map, index); 
  pthread_mutex_lock(&(ptr->enrty_lock));
  struct entry* ptr_next = ptr->next;

  while (ptr_next != NULL) {
    pthread_mutex_lock(&(ptr_next->enrty_lock));

    // replace the original k and v
    if (map->cmp(ptr_next->key, k)) {
      // free the original space
      map->key_destruct(ptr_next->key);
      map->value_destruct(ptr_next->value);
      ptr_next->key = k;
      ptr_next->value = v;
      pthread_mutex_unlock(&(ptr_next->enrty_lock));
      pthread_mutex_unlock(&(ptr->enrty_lock));
      return;
    }
    pthread_mutex_unlock(&(ptr->enrty_lock));
    ptr = ptr_next;
    ptr_next = ptr_next->next;
  }

  // create new entry
  struct entry* new = malloc(sizeof(struct entry));
  pthread_mutex_init(&new->enrty_lock, NULL);
  pthread_mutex_lock(&new->enrty_lock);
  new->key = k;
  new->value = v;
  new->next = NULL;
  ptr->next = new;
  pthread_mutex_unlock(&new->enrty_lock);
  pthread_mutex_unlock(&ptr->enrty_lock);
}

void hash_map_remove_entry(struct hash_map* map, void* k) {
  if (map == NULL || k == NULL) {
    return;
  }

  size_t index = map->hash(k) % T_SIZE;
  struct entry* ptr = &(ENTRY(map, index));
  pthread_mutex_lock(&(ptr->enrty_lock));
  struct entry* ptr_next = ptr->next;

  while (ptr_next != NULL) {
    pthread_mutex_lock(&(ptr_next->enrty_lock));
    if (map->cmp(ptr_next->key, k)) {
      map->key_destruct(ptr_next->key);
      map->value_destruct(ptr_next->value);

      ptr_next->key = NULL;
      ptr_next->value = NULL;
      ptr->next = ptr_next->next;
      pthread_mutex_unlock(&(ptr_next->enrty_lock));
      pthread_mutex_unlock(&(ptr->enrty_lock));
      free(ptr_next);
      return;
    }
    pthread_mutex_unlock(&(ptr->enrty_lock));
    ptr = ptr_next;
    ptr_next = ptr_next->next;
  }
}

void* hash_map_get_value_ref(struct hash_map* map, void* k) {
  if (map == NULL || k == NULL) {
    return NULL;
  }

  size_t index = map->hash(k) % T_SIZE;
  struct entry* ptr = &(ENTRY(map, index));

  pthread_mutex_lock(&(ptr->enrty_lock));
  struct entry* ptr_next = ptr->next;

  while (ptr_next != NULL) {
    pthread_mutex_lock(&(ptr_next->enrty_lock));
    pthread_mutex_unlock(&(ptr->enrty_lock));

    if (map->cmp(ptr_next->key, k)) {
      pthread_mutex_unlock(&(ptr_next->enrty_lock));
      return ptr_next->value;
    }
    ptr = ptr_next;
    ptr_next = ptr_next->next;
  }
  pthread_mutex_unlock(&(ptr->enrty_lock));
  return NULL;
}

void hash_map_destroy(struct hash_map* map) {
  if (map == NULL) {
    return;
  }

  int i;
  for (i = 0; i < T_SIZE; i++) {
    struct entry* ptr = &(ENTRY(map, i));
    pthread_mutex_lock(&(ptr->enrty_lock));
    struct entry* ptr_next = ptr->next;
    pthread_mutex_unlock(&(ptr->enrty_lock));

    while (ptr_next != NULL) {
      struct entry* temp = ptr_next;
      pthread_mutex_lock(&(temp->enrty_lock));

      ptr_next = ptr_next->next;

      map->key_destruct(temp->key);
      map->value_destruct(temp->value);

      pthread_mutex_unlock(&(temp->enrty_lock));
      free(temp);
    }
  }

  free(map->entries);
  free(map);
}