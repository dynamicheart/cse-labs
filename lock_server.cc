// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

lock_server::lock_server():
  nacquire (0)
{
  pthread_mutex_init(&map_mutex, NULL);
  pthread_cond_init(&state_cv, NULL);
}

lock_server::~lock_server()
{
  pthread_mutex_destroy(&map_mutex);
  pthread_cond_destroy(&state_cv);
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request from clt %d\n", clt);
  r = nacquire;
  return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
	// Your lab4 code goes here
  pthread_mutex_lock(&map_mutex);
  if(state_map.find(lid) != state_map.end()){
    while(state_map[lid] == LOCKED){
      pthread_cond_wait(&state_cv, &map_mutex);
    }
  }
  state_map[lid] = LOCKED;
  pthread_mutex_unlock(&map_mutex);
  return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
	// Your lab4 code goes here
  pthread_mutex_lock(&map_mutex);
  if((state_map.find(lid) != state_map.end()) && state_map[lid] == LOCKED){
    state_map[lid] = FREE;
    pthread_cond_broadcast(&state_cv);
  }else{
    ret = lock_protocol::NOENT;
  }
  pthread_mutex_unlock(&map_mutex);
  return ret;
}

