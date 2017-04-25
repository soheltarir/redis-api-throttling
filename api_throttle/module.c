#include "redismodule.h"
#include <stdlib.h>
#include <time.h>
#include <string.h>


 RedisModuleString *getCacheKey(RedisModuleCtx *ctx, const char *viewName, const char *userId, long long ldRedisDb) {
	/*
	This function returns the seconds after which the cache key should be expired.
	*/
	char strCacheKey[255];
	sprintf(strCacheKey, ":%lld:throttle_%s_%s", ldRedisDb, viewName, userId);
	// Convert to RedisModuleString
	RedisModuleString *rstrCacheKey;
	rstrCacheKey = RedisModule_CreateString(ctx, strCacheKey, strlen(strCacheKey));
	return rstrCacheKey;
}


int CheckRateLimit(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
	/*
	Arguments should be <apiViewName> <userId> <maxLimit> <per hour/min> <redisDB>
	*/
	// We must have at least 5 arguments
	if (argc < 5) {
		return RedisModule_WrongArity(ctx);
	}
	// Get the Correct Redis DB
	long long ldRedisDb;
	if (argc != 6) {
		ldRedisDb = 0;
	}
	else if (RedisModule_StringToLongLong(argv[5], &ldRedisDb) != REDISMODULE_OK) {
		ldRedisDb = 0;
	}

	RedisModule_AutoMemory(ctx);

	size_t view_name_len, user_id_len, identifier_len;
	long dCacheKeyTTL;		// In seconds

	const char *viewName = RedisModule_StringPtrLen(argv[1], &view_name_len);
	const char *userId = RedisModule_StringPtrLen(argv[2], &user_id_len);
	
	const char *identifier = RedisModule_StringPtrLen(argv[4], &identifier_len);
	if (strcmp(identifier, "min") == 0) {
		dCacheKeyTTL = 60 * 1000;
	}
	else if (strcmp(identifier, "hour") == 0) {
		dCacheKeyTTL = 60 * 60 * 1000;
	}
	else {
		return RedisModule_ReplyWithError(ctx, "Invalid rate limit identifier, should be hour/min.");
	}

	long long count = 0;
	long long maxLimit;
	if (RedisModule_StringToLongLong(argv[3], &maxLimit) != REDISMODULE_OK) {
        return RedisModule_ReplyWithError(ctx, "Unable to read rate limit provided.");
    }
	
	RedisModuleString *rstrCacheKey = getCacheKey(ctx, viewName, userId, ldRedisDb); 
	RedisModule_SelectDb(ctx, ldRedisDb);
	RedisModuleKey *key = RedisModule_OpenKey(ctx, rstrCacheKey, REDISMODULE_READ | REDISMODULE_WRITE);

	RedisModuleString *rstrReply;
	long ttl;

	if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
		RedisModule_StringSet(key, RedisModule_CreateStringFromLongLong(ctx, 1));
		// Set Expiry
		RedisModule_SetExpire(key, dCacheKeyTTL);
		ttl = dCacheKeyTTL/1000;
		count = 1;
	}
	else if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_STRING) {
		ttl = (long)RedisModule_GetExpire(key)/1000;
		size_t key_value_len;
		char* strCurrCount = RedisModule_StringDMA(key, &key_value_len, REDISMODULE_WRITE);
		count = atoi(strCurrCount) + 1;
		sprintf(strCurrCount, "%lld", count);
	}
	else {
		RedisModule_ReplyWithError(ctx, "Error Opening Key!");
        return REDISMODULE_ERR;
	}
	RedisModule_CloseKey(key);

	RedisModule_FreeString(ctx, rstrCacheKey);

	if (count <= maxLimit) {
		char strReply[32];
		sprintf(strReply, "%s|%ld", "TRUE", ttl);
		rstrReply = RedisModule_CreateString(ctx, strReply, strlen(strReply));
		return RedisModule_ReplyWithString(ctx, rstrReply);
	}
	else {
		char strReply[32];
		sprintf(strReply, "%s|%ld", "FALSE", ttl);
		rstrReply = RedisModule_CreateString(ctx, strReply, strlen(strReply));
		return RedisModule_ReplyWithString(ctx, rstrReply);
	}
}


int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (RedisModule_Init(ctx,"RATE_LIMITER",1,REDISMODULE_APIVER_1) == REDISMODULE_ERR) 
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"RATE_LIMITER.CHECK", CheckRateLimit, "readonly", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    return REDISMODULE_OK;
}
