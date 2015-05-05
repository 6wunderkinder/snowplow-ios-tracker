//
//  SnowplowEventStore.h
//  Snowplow
//
//  Copyright (c) 2013-2014 Snowplow Analytics Ltd. All rights reserved.
//
//  This program is licensed to you under the Apache License Version 2.0,
//  and you may not use this file except in compliance with the Apache License
//  Version 2.0. You may obtain a copy of the Apache License Version 2.0 at
//  http://www.apache.org/licenses/LICENSE-2.0.
//
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the Apache License Version 2.0 is distributed on
//  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the Apache License Version 2.0 for the specific
//  language governing permissions and limitations there under.
//
//  Authors: Jonathan Almeida
//  Copyright: Copyright (c) 2013-2014 Snowplow Analytics Ltd
//  License: Apache License Version 2.0
//

#import "Snowplow.h"
#import "SnowplowEventStore.h"
#import "SnowplowPayload.h"
#import "SnowplowUtils.h"
#import <FMDB.h>

@implementation SnowplowEventStore {
    @private
    NSString *        _dbPath;
    FMDatabaseQueue * _dbQueue;
}

static NSString * const _queryCreateTable       = @"CREATE TABLE IF NOT EXISTS 'events' (id INTEGER PRIMARY KEY, eventData BLOB, pending INTEGER, dateCreated TIMESTAMP DEFAULT CURRENT_TIMESTAMP)";
static NSString * const _querySelectAll         = @"SELECT * FROM 'events'";
static NSString * const _querySelectCount       = @"SELECT Count(*) FROM 'events'";
static NSString * const _queryInsertEvent       = @"INSERT INTO 'events' (eventData, pending) VALUES (?, 0)";
static NSString * const _querySelectId          = @"SELECT * FROM 'events' WHERE id=?";
static NSString * const _queryDeleteId          = @"DELETE FROM 'events' WHERE id=?";
static NSString * const _querySelectPending     = @"SELECT * FROM 'events' WHERE pending=1";
static NSString * const _querySelectNonPending  = @"SELECT * FROM 'events' WHERE pending=0";
static NSString * const _querySetPending        = @"UPDATE events SET pending=1 WHERE id=?";
static NSString * const _querySetNonPending     = @"UPDATE events SET pending=0 WHERE id=?";


@synthesize appId;

- (id) init {
    self = [super init];
    NSString *libraryPath = [NSSearchPathForDirectoriesInDomains(NSLibraryDirectory, NSUserDomainMask, YES) objectAtIndex:0];
    _dbPath = [libraryPath stringByAppendingPathComponent:@"snowplowEvents.sqlite"];
    if(self){
        _dbQueue = [FMDatabaseQueue databaseQueueWithPath:_dbPath];
        if (_dbQueue) {
            SnowplowDLog(@"db description: %@", _dbQueue.path);
            [self createTable];
        } else {
            SnowplowDLog(@"Failed to open database. Events in memory will not persist!");
        }
    }
    return self;
}

- (void) dealloc {
    [_dbQueue close];
}

- (BOOL) createTable {
    __block BOOL success = NO;
    [_dbQueue inDatabase:^(FMDatabase *db) {
        success = [db executeStatements:_queryCreateTable];
    }];
    return success;
}

- (long long int) insertEvent:(SnowplowPayload *)payload {
    return [self insertDictionaryData:[payload getPayloadAsDictionary]];
}

- (long long int) insertDictionaryData:(NSDictionary *)dict {
    __block long long int result = -1;
    [_dbQueue inDatabase:^(FMDatabase *db) {
        NSData *data = [NSJSONSerialization dataWithJSONObject:dict options:0 error:nil];
        [db executeUpdate:_queryInsertEvent, data];
        result = (long long int) [db lastInsertRowId];
    }];
    return result;
}

- (BOOL) removeEventWithId:(long long int)id_ {
    __block BOOL success = NO;
    [_dbQueue inDatabase:^(FMDatabase *db) {
        SnowplowDLog(@"Removing %lld from database now.", id_);
        success = [db executeUpdate:_queryDeleteId, [NSNumber numberWithLongLong:id_]];
    }];
    return success;
}

- (void) removeAllEvents {
    [_dbQueue inDatabase:^(FMDatabase *db) {
        FMResultSet *s = [db executeQuery:_querySelectAll];
        while ([s next]) {
            long long int index = [s longLongIntForColumn:@"ID"];
            [self removeEventWithId:index];
        }
    }];
}

- (BOOL) setPendingWithId:(long long int)id_ {
    __block BOOL success = NO;
    [_dbQueue inDatabase:^(FMDatabase *db) {
        success = [db executeUpdate:_querySetPending, id_];
    }];
    return success;
}

- (BOOL) removePendingWithId:(long long int)id_ {
    __block BOOL success = NO;
    [_dbQueue inDatabase:^(FMDatabase *db) {
        success = [db executeUpdate:_querySetNonPending, id_];
    }];
    return success;
}

- (NSUInteger) count {
    __block NSUInteger num = 0;
    [_dbQueue inDatabase:^(FMDatabase *db) {
        FMResultSet *s = [db executeQuery:_querySelectCount];
        while ([s next]) {
            num = [[NSNumber numberWithInt:[s intForColumnIndex:0]] integerValue];
        }
    }];
    return num;
}

- (NSDictionary *) getEventWithId:(long long int)id_ {
    __block NSDictionary *result = nil;
    [_dbQueue inDatabase:^(FMDatabase *db) {
        FMResultSet *s = [db executeQuery:_querySelectId, [NSNumber numberWithLongLong:id_]];
        while ([s next]) {
            NSData * data = [s dataForColumn:@"eventData"];
            SnowplowDLog(@"Item: %d %@ %@",
                 [s intForColumn:@"ID"],
                 [s dateForColumn:@"dateCreated"],
                 [[NSString alloc] initWithData:data encoding:NSUTF8StringEncoding]);
            result = [NSJSONSerialization JSONObjectWithData:data options:0 error:0];
        }
    }];
    return result;
}

- (NSArray *) getAllEvents {
    return [self getAllEventsWithQuery:_querySelectAll];
}

- (NSArray *) getAllNonPendingEvents {
    return [self getAllEventsWithQuery:_querySelectNonPending];
}

- (NSArray *) getAllPendingEvents {
    NSMutableArray *res = [[NSMutableArray alloc] init];
    [_dbQueue inDatabase:^(FMDatabase *db) {
        FMResultSet *s = [db executeQuery:_querySelectPending];
        while ([s next]) {
            [res addObject:[s dataForColumn:@"eventData"]];
        }
    }];
    return res;
}

- (NSArray *) getAllEventsWithQuery:(NSString *)query {
    NSMutableArray *res = [[NSMutableArray alloc] init];
    [_dbQueue inDatabase:^(FMDatabase *db) {
        FMResultSet *s = [db executeQuery:query];
        while ([s next]) {
            long long int index = [s longLongIntForColumn:@"ID"];
            NSData * data =[s dataForColumn:@"eventData"];
            NSDate * date = [s dateForColumn:@"dateCreated"];
            SnowplowDLog(@"Item: %lld %@ %@",
                 index,
                 [date description],
                 [[NSString alloc] initWithData:data encoding:NSUTF8StringEncoding]);
            NSDictionary *dict = [NSJSONSerialization JSONObjectWithData:data options:0 error:0];
            NSMutableDictionary * eventWithSqlMetadata = [[NSMutableDictionary alloc] init];
            [eventWithSqlMetadata setValue:dict forKey:@"eventData"];
            [eventWithSqlMetadata setValue:[NSNumber numberWithLongLong:index] forKey:@"ID"];
            [eventWithSqlMetadata setValue:date forKey:@"dateCreated"];
            [res addObject:eventWithSqlMetadata];
        }
    }];
    return res;
}

- (long long int) getLastInsertedRowId {
    __block long long int result = -1;
    [_dbQueue inDatabase:^(FMDatabase *db) {
        result = (long long int)[db lastInsertRowId];
    }];
    return result;
}

@end
