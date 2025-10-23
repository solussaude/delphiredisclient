program VerificationTests;

{$APPTYPE CONSOLE}

{$R *.res}

uses
  System.SysUtils,
  Redis.Commons in '..\sources\Redis.Commons.pas',
  Redis.Client in '..\sources\Redis.Client.pas',
  Redis.NetLib.INDY in '..\sources\Redis.NetLib.INDY.pas',
  Redis.Values in '..\sources\Redis.Values.pas',
  Redis.Command in '..\sources\Redis.Command.pas',
  Redis.NetLib.Factory in '..\sources\Redis.NetLib.Factory.pas',
  JsonDataObjects in '..\sources\JsonDataObjects.pas';

var
  lRedis: IRedisClient;
  lValue: TRedisString;
  lPassed: Integer;
  lFailed: Integer;

procedure Test(const TestName: string; TestProc: TProc);
begin
  Write(TestName + '... ');
  try
    TestProc();
    WriteLn('OK');
    Inc(lPassed);
  except
    on E: Exception do
    begin
      WriteLn('FAILED: ' + E.Message);
      Inc(lFailed);
    end;
  end;
end;

begin
  lPassed := 0;
  lFailed := 0;

  try
    WriteLn('=== Delphi Redis Client - Verification Tests ===');
    WriteLn;

    lRedis := TRedisClient.Create('127.0.0.1', 6379);
    lRedis.Connect;
    WriteLn('Connected to Redis on localhost:6379');
    WriteLn;

    WriteLn('--- Core Commands ---');
    Test('PING', procedure
    begin
      if lRedis.PING <> 'PONG' then
        raise Exception.Create('Expected PONG');
    end);

    Test('SET/GET', procedure
    begin
      lRedis.&SET('test:key', 'test_value');
      lValue := lRedis.GET('test:key');
      if lValue.IsNull or (lValue.Value <> 'test_value') then
        raise Exception.Create('Value mismatch');
      lRedis.DEL(['test:key']);
    end);

    Test('DEL', procedure
    begin
      lRedis.&SET('test:delkey', 'value');
      if lRedis.DEL(['test:delkey']) <> 1 then
        raise Exception.Create('DEL failed');
      lValue := lRedis.GET('test:delkey');
      if not lValue.IsNull then
        raise Exception.Create('Key should not exist');
    end);

    Test('GET nullable (non-existent key)', procedure
    begin
      lValue := lRedis.GET('test:nonexistent_' + FormatDateTime('hhnnsszzz', Now));
      if not lValue.IsNull then
        raise Exception.Create('Should be null');
    end);

    Test('EXISTS', procedure
    begin
      lRedis.&SET('test:exists', 'value');
      if not lRedis.EXISTS('test:exists') then
        raise Exception.Create('Key should exist');
      lRedis.DEL(['test:exists']);
      if lRedis.EXISTS('test:exists') then
        raise Exception.Create('Key should not exist');
    end);

    Test('INCR/DECR', procedure
    var
      v: Int64;
    begin
      lRedis.DEL(['test:counter']);
      v := lRedis.INCR('test:counter');
      if v <> 1 then raise Exception.Create('INCR failed');
      v := lRedis.INCR('test:counter');
      if v <> 2 then raise Exception.Create('INCR failed');
      v := lRedis.DECR('test:counter');
      if v <> 1 then raise Exception.Create('DECR failed');
      lRedis.DEL(['test:counter']);
    end);

    Test('LPUSH/LPOP', procedure
    var
      v: TRedisString;
    begin
      lRedis.DEL(['test:list']);
      lRedis.LPUSH('test:list', ['a', 'b', 'c']);
      v := lRedis.LPOP('test:list');
      if v.Value <> 'c' then raise Exception.Create('LPOP failed');
      lRedis.DEL(['test:list']);
    end);

    Test('RPUSH/RPOP', procedure
    var
      v: TRedisString;
    begin
      lRedis.DEL(['test:list']);
      lRedis.RPUSH('test:list', ['a', 'b', 'c']);
      v := lRedis.RPOP('test:list');
      if v.Value <> 'c' then raise Exception.Create('RPOP failed');
      lRedis.DEL(['test:list']);
    end);

    Test('LLEN', procedure
    var
      len: Integer;
    begin
      lRedis.DEL(['test:list']);
      lRedis.LPUSH('test:list', ['a', 'b', 'c']);
      len := lRedis.LLEN('test:list');
      if len <> 3 then raise Exception.Create('LLEN failed');
      lRedis.DEL(['test:list']);
    end);

    Test('HSET/HGET', procedure
    var
      v: TRedisString;
    begin
      lRedis.DEL(['test:hash']);
      lRedis.HSET('test:hash', 'field1', 'value1');
      v := lRedis.HGET('test:hash', 'field1');
      if v.Value <> 'value1' then raise Exception.Create('HGET failed');
      lRedis.DEL(['test:hash']);
    end);

    Test('HEXISTS', procedure
    begin
      lRedis.DEL(['test:hash']);
      lRedis.HSET('test:hash', 'field1', 'value1');
      if not lRedis.HEXISTS('test:hash', 'field1') then
        raise Exception.Create('HEXISTS failed');
      if lRedis.HEXISTS('test:hash', 'nonexistent') then
        raise Exception.Create('HEXISTS should return false');
      lRedis.DEL(['test:hash']);
    end);

    Test('SADD/SCARD', procedure
    var
      c: Integer;
    begin
      lRedis.DEL(['test:set']);
      lRedis.SADD('test:set', 'member1');
      lRedis.SADD('test:set', 'member2');
      c := lRedis.SCARD('test:set');
      if c <> 2 then raise Exception.Create('SCARD failed');
      lRedis.DEL(['test:set']);
    end);

    Test('SISMEMBER', procedure
    begin
      lRedis.DEL(['test:set']);
      lRedis.SADD('test:set', 'member1');
      if lRedis.SISMEMBER('test:set', 'member1') <> 1 then
        raise Exception.Create('SISMEMBER failed');
      if lRedis.SISMEMBER('test:set', 'nonexistent') <> 0 then
        raise Exception.Create('SISMEMBER should return 0');
      lRedis.DEL(['test:set']);
    end);

    Test('ZADD/ZCARD', procedure
    var
      c: Integer;
    begin
      lRedis.DEL(['test:zset']);
      lRedis.ZADD('test:zset', 100, 'member1');
      lRedis.ZADD('test:zset', 200, 'member2');
      c := lRedis.ZCARD('test:zset');
      if c <> 2 then raise Exception.Create('ZCARD failed');
      lRedis.DEL(['test:zset']);
    end);

    Test('ZRANK', procedure
    var
      rank: Int64;
    begin
      lRedis.DEL(['test:zset']);
      lRedis.ZADD('test:zset', 1, 'one');
      lRedis.ZADD('test:zset', 2, 'two');
      lRedis.ZADD('test:zset', 3, 'three');
      if not lRedis.ZRANK('test:zset', 'two', rank) then
        raise Exception.Create('ZRANK failed');
      if rank <> 1 then
        raise Exception.Create('ZRANK wrong value');
      lRedis.DEL(['test:zset']);
    end);

    Test('EXPIRE/TTL', procedure
    var
      ttl: Integer;
    begin
      lRedis.&SET('test:expire', 'value');
      lRedis.EXPIRE('test:expire', 60);
      ttl := lRedis.TTL('test:expire');
      if (ttl <= 0) or (ttl > 60) then
        raise Exception.Create('TTL failed: ' + IntToStr(ttl));
      lRedis.DEL(['test:expire']);
    end);

    Test('MSET/MGET', procedure
    begin
      lRedis.MSET(['test:k1', 'v1', 'test:k2', 'v2', 'test:k3', 'v3']);
      lValue := lRedis.GET('test:k1');
      if lValue.Value <> 'v1' then raise Exception.Create('MSET failed');
      lRedis.DEL(['test:k1', 'test:k2', 'test:k3']);
    end);

    WriteLn;
    WriteLn('--- Redis 6.2+ String/Key Commands ---');
    Test('GETEX (with EX seconds)', procedure
    var
      v: TRedisString;
      ttl: Integer;
    begin
      lRedis.&SET('test:getex', 'myvalue');
      v := lRedis.GETEX('test:getex', 10, True); // EX 10 seconds
      if v.Value <> 'myvalue' then
        raise Exception.Create('GETEX value mismatch');
      ttl := lRedis.TTL('test:getex');
      if (ttl <= 0) or (ttl > 10) then
        raise Exception.Create('GETEX expiration not set correctly');
      lRedis.DEL(['test:getex']);
    end);

    Test('GETEX (with PX milliseconds)', procedure
    var
      v: TRedisString;
      ttl: Integer;
    begin
      lRedis.&SET('test:getex2', 'myvalue2');
      v := lRedis.GETEX('test:getex2', 5000, False); // PX 5000 ms
      if v.Value <> 'myvalue2' then
        raise Exception.Create('GETEX value mismatch');
      ttl := lRedis.TTL('test:getex2');
      if (ttl <= 0) or (ttl > 5) then
        raise Exception.Create('GETEX PX expiration not set correctly');
      lRedis.DEL(['test:getex2']);
    end);

    // Redis 6.2+ GETDEL command
    Test('GETDEL', procedure
    var
      v: TRedisString;
    begin
      lRedis.&SET('test:getdel', 'delete_me');
      v := lRedis.GETDEL('test:getdel');
      if v.Value <> 'delete_me' then
        raise Exception.Create('GETDEL value mismatch');
      if lRedis.EXISTS('test:getdel') then
        raise Exception.Create('GETDEL should have deleted the key');
    end);

    Test('GETDEL (non-existent key)', procedure
    var
      v: TRedisString;
    begin
      v := lRedis.GETDEL('test:nonexistent_getdel');
      if not v.IsNull then
        raise Exception.Create('GETDEL should return null for non-existent key');
    end);

    // Redis 6.2+ COPY command
    Test('COPY', procedure
    var
      copied: Boolean;
      v: TRedisString;
    begin
      lRedis.&SET('test:source', 'original_value');
      lRedis.DEL(['test:dest']);
      copied := lRedis.COPY('test:source', 'test:dest', False);
      if not copied then
        raise Exception.Create('COPY failed');
      v := lRedis.GET('test:dest');
      if v.Value <> 'original_value' then
        raise Exception.Create('COPY value mismatch');
      // Source should still exist
      if not lRedis.EXISTS('test:source') then
        raise Exception.Create('COPY should not delete source');
      lRedis.DEL(['test:source', 'test:dest']);
    end);

    Test('COPY (with REPLACE)', procedure
    var
      copied: Boolean;
      v: TRedisString;
    begin
      lRedis.&SET('test:src2', 'value1');
      lRedis.&SET('test:dst2', 'value2');
      copied := lRedis.COPY('test:src2', 'test:dst2', True); // REPLACE
      if not copied then
        raise Exception.Create('COPY with REPLACE failed');
      v := lRedis.GET('test:dst2');
      if v.Value <> 'value1' then
        raise Exception.Create('COPY REPLACE value mismatch');
      lRedis.DEL(['test:src2', 'test:dst2']);
    end);

    WriteLn;
    WriteLn('--- Redis 6.2+ Set Commands ---');
    Test('SMOVE (move existing member)', procedure
    var
      moved: Boolean;
    begin
      lRedis.DEL(['test:set1', 'test:set2']);
      lRedis.SADD('test:set1', 'member1');
      lRedis.SADD('test:set1', 'member2');
      lRedis.SADD('test:set2', 'member3');
      moved := lRedis.SMOVE('test:set1', 'test:set2', 'member1');
      if not moved then
        raise Exception.Create('SMOVE failed');
      if lRedis.SISMEMBER('test:set1', 'member1') <> 0 then
        raise Exception.Create('SMOVE should remove member from source');
      if lRedis.SISMEMBER('test:set2', 'member1') <> 1 then
        raise Exception.Create('SMOVE should add member to destination');
      lRedis.DEL(['test:set1', 'test:set2']);
    end);

    Test('SMOVE (non-existent member)', procedure
    var
      moved: Boolean;
    begin
      lRedis.DEL(['test:set3', 'test:set4']);
      lRedis.SADD('test:set3', 'member1');
      moved := lRedis.SMOVE('test:set3', 'test:set4', 'nonexistent');
      if moved then
        raise Exception.Create('SMOVE should return false for non-existent member');
      lRedis.DEL(['test:set3', 'test:set4']);
    end);

    WriteLn;
    WriteLn('--- Redis 5.0+ Sorted Set Commands ---');
    Test('ZPOPMIN (single element)', procedure
    var
      result: TRedisArray;
    begin
      lRedis.DEL(['test:zset1']);
      lRedis.ZADD('test:zset1', 10, 'member1');
      lRedis.ZADD('test:zset1', 20, 'member2');
      lRedis.ZADD('test:zset1', 30, 'member3');
      result := lRedis.ZPOPMIN('test:zset1');
      if result.IsNull or (result.Count <> 2) then
        raise Exception.Create('ZPOPMIN should return member and score');
      if result.Items[0].Value <> 'member1' then
        raise Exception.Create('ZPOPMIN should return lowest score member');
      if lRedis.ZCARD('test:zset1') <> 2 then
        raise Exception.Create('ZPOPMIN should remove the element');
      lRedis.DEL(['test:zset1']);
    end);

    Test('ZPOPMIN (multiple elements)', procedure
    var
      result: TRedisArray;
    begin
      lRedis.DEL(['test:zset2']);
      lRedis.ZADD('test:zset2', 1, 'one');
      lRedis.ZADD('test:zset2', 2, 'two');
      lRedis.ZADD('test:zset2', 3, 'three');
      lRedis.ZADD('test:zset2', 4, 'four');
      result := lRedis.ZPOPMIN('test:zset2', 2);
      if result.IsNull or (result.Count <> 4) then
        raise Exception.Create('ZPOPMIN with count=2 should return 2 members with scores (4 items)');
      if result.Items[0].Value <> 'one' then
        raise Exception.Create('First element should be "one"');
      if result.Items[2].Value <> 'two' then
        raise Exception.Create('Second element should be "two"');
      if lRedis.ZCARD('test:zset2') <> 2 then
        raise Exception.Create('ZPOPMIN should leave 2 elements');
      lRedis.DEL(['test:zset2']);
    end);

    Test('ZPOPMAX (single element)', procedure
    var
      result: TRedisArray;
    begin
      lRedis.DEL(['test:zset3']);
      lRedis.ZADD('test:zset3', 10, 'member1');
      lRedis.ZADD('test:zset3', 20, 'member2');
      lRedis.ZADD('test:zset3', 30, 'member3');
      result := lRedis.ZPOPMAX('test:zset3');
      if result.IsNull or (result.Count <> 2) then
        raise Exception.Create('ZPOPMAX should return member and score');
      if result.Items[0].Value <> 'member3' then
        raise Exception.Create('ZPOPMAX should return highest score member');
      if lRedis.ZCARD('test:zset3') <> 2 then
        raise Exception.Create('ZPOPMAX should remove the element');
      lRedis.DEL(['test:zset3']);
    end);

    Test('ZPOPMAX (multiple elements)', procedure
    var
      result: TRedisArray;
    begin
      lRedis.DEL(['test:zset4']);
      lRedis.ZADD('test:zset4', 1, 'one');
      lRedis.ZADD('test:zset4', 2, 'two');
      lRedis.ZADD('test:zset4', 3, 'three');
      lRedis.ZADD('test:zset4', 4, 'four');
      result := lRedis.ZPOPMAX('test:zset4', 2);
      if result.IsNull or (result.Count <> 4) then
        raise Exception.Create('ZPOPMAX with count=2 should return 2 members with scores (4 items)');
      if result.Items[0].Value <> 'four' then
        raise Exception.Create('First element should be "four"');
      if result.Items[2].Value <> 'three' then
        raise Exception.Create('Second element should be "three"');
      if lRedis.ZCARD('test:zset4') <> 2 then
        raise Exception.Create('ZPOPMAX should leave 2 elements');
      lRedis.DEL(['test:zset4']);
    end);

    WriteLn;
    WriteLn('--- Redis 6.2+ Geo Commands ---');
    Test('GEOSEARCH (from member)', procedure
    var
      result: TRedisArray;
    begin
      lRedis.DEL(['test:geo1']);
      lRedis.GEOADD('test:geo1', 13.361389, 38.115556, 'Palermo');
      lRedis.GEOADD('test:geo1', 15.087269, 37.502669, 'Catania');
      lRedis.GEOADD('test:geo1', 12.496366, 41.902782, 'Roma');
      result := lRedis.GEOSEARCH('test:geo1', 'Palermo', 200, TRedisGeoUnit.Kilometers);
      if result.IsNull or (result.Count < 1) then
        raise Exception.Create('GEOSEARCH should find members within radius');
      lRedis.DEL(['test:geo1']);
    end);

    Test('GEOSEARCH (from coordinates)', procedure
    var
      result: TRedisArray;
    begin
      lRedis.DEL(['test:geo2']);
      lRedis.GEOADD('test:geo2', 13.361389, 38.115556, 'Palermo');
      lRedis.GEOADD('test:geo2', 15.087269, 37.502669, 'Catania');
      result := lRedis.GEOSEARCH('test:geo2', 15.0, 37.0, 100, TRedisGeoUnit.Kilometers);
      if result.IsNull or (result.Count < 1) then
        raise Exception.Create('GEOSEARCH from coordinates should find members');
      lRedis.DEL(['test:geo2']);
    end);

    Test('GEOSEARCHSTORE (from member)', procedure
    var
      count: Integer;
    begin
      lRedis.DEL(['test:geo3', 'test:geodest1']);
      lRedis.GEOADD('test:geo3', 13.361389, 38.115556, 'Palermo');
      lRedis.GEOADD('test:geo3', 15.087269, 37.502669, 'Catania');
      lRedis.GEOADD('test:geo3', 12.496366, 41.902782, 'Roma');
      count := lRedis.GEOSEARCHSTORE('test:geodest1', 'test:geo3', 'Palermo', 200, TRedisGeoUnit.Kilometers);
      if count < 1 then
        raise Exception.Create('GEOSEARCHSTORE should store results');
      if not lRedis.EXISTS('test:geodest1') then
        raise Exception.Create('GEOSEARCHSTORE should create destination key');
      lRedis.DEL(['test:geo3', 'test:geodest1']);
    end);

    Test('GEOSEARCHSTORE (from coordinates)', procedure
    var
      count: Integer;
    begin
      lRedis.DEL(['test:geo4', 'test:geodest2']);
      lRedis.GEOADD('test:geo4', 13.361389, 38.115556, 'Palermo');
      lRedis.GEOADD('test:geo4', 15.087269, 37.502669, 'Catania');
      count := lRedis.GEOSEARCHSTORE('test:geodest2', 'test:geo4', 15.0, 37.0, 100, TRedisGeoUnit.Kilometers);
      if count < 1 then
        raise Exception.Create('GEOSEARCHSTORE from coordinates should store results');
      lRedis.DEL(['test:geo4', 'test:geodest2']);
    end);

    lRedis.Disconnect;
    WriteLn;
    WriteLn('=== Test Results ===');
    WriteLn('Passed: ', lPassed);
    WriteLn('Failed: ', lFailed);
    WriteLn;

    if lFailed = 0 then
    begin
      WriteLn('All tests PASSED!');
      ExitCode := 0;
    end
    else
    begin
      WriteLn('Some tests FAILED!');
      ExitCode := 1;
    end;

  except
    on E: Exception do
    begin
      WriteLn;
      WriteLn('FATAL ERROR: ', E.ClassName, ': ', E.Message);
      ExitCode := 1;
    end;
  end;
  {$IF Defined(MSWINDOWS)}
  if DebugHook <> 0 then
  begin
    Write('Return to EXIT');
    Readln;
  end;
  {$ENDIF}


end.
