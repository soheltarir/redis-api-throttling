#include "redismodule.h"
#include <stdlib.h>
#include <time.h>
#include <string.h>


 RedisModuleString *getCacheKey(RedisModuleCtx *ctx, const char *viewName, const char *userId) {
	/*
	This function returns the seconds after which the cache key should be expired.
	*/
	char strCacheKey[255];
	sprintf(strCacheKey, "throttle_%s_%s", viewName, userId);
	// Convert to RedisModuleString
	RedisModuleString *rstrCacheKey;
	rstrCacheKey = RedisModule_CreateString(ctx, strCacheKey, strlen(strCacheKey));
	return rstrCacheKey;
}


int CheckRateLimit(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
	/*
	Arguments should be <apiViewName> <userId> <maxLimit> <per hour/min>
	*/
	// We must have at least 5 arguments
	if (argc < 5) {
		return RedisModule_WrongArity(ctx);
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
	
	RedisModuleString *rstrCacheKey = getCacheKey(ctx, viewName, userId); 
	RedisModuleKey *key = RedisModule_OpenKey(ctx, rstrCacheKey, REDISMODULE_READ | REDISMODULE_WRITE);

	if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
		RedisModule_StringSet(key, RedisModule_CreateStringFromLongLong(ctx,1));
		// Set Expiry
		RedisModule_SetExpire(key, dCacheKeyTTL);
		count = 1;
	}
	else if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_STRING) {
		// Read the current value
		size_t len;
		char *currValue = RedisModule_StringDMA(key, &len, REDISMODULE_READ);
		RedisModule_CloseKey(key);
		if (atoi(currValue) > maxLimit) {
			return RedisModule_ReplyWithLongLong(ctx, 0);
		}
		else {
			RedisModuleCallReply *rep = RedisModule_Call(ctx, "INCR", "s", rstrCacheKey);
			if (RedisModule_CallReplyType(rep) == REDISMODULE_REPLY_INTEGER) {
				count = RedisModule_CallReplyInteger(rep);
			}
			else {
				RedisModule_ReplyWithError(ctx, "INCR Command failed.");
	        	return REDISMODULE_ERR;
			}
		}
	}
	else {
		RedisModule_ReplyWithError(ctx, "Error Opening Key!");
        return REDISMODULE_ERR;
	}

	RedisModule_FreeString(ctx, rstrCacheKey);

	if (count <= maxLimit) {
		return RedisModule_ReplyWithLongLong(ctx, 1);
	}
	else {
		return RedisModule_ReplyWithLongLong(ctx, 0);
	}
}


int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (RedisModule_Init(ctx,"RATE_LIMITER",1,REDISMODULE_APIVER_1) == REDISMODULE_ERR) 
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"RATE_LIMITER.CHECK", CheckRateLimit, "readonly", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    return REDISMODULE_OK;
}
