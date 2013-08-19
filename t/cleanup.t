#!/usr/bin/env perl
use strict;
use warnings;
use Test::More;
use Test::Deep;
use Redis::hiredis;
use Try::Tiny;
use MapReduce;

my $redis = Redis::hiredis->new(utf8 => 0);

$redis->connect('127.0.0.1', 6379);
$redis->select(9);
$redis->flushdb();

my $mr = MapReduce->new(
    name => 'test-cleanup',
    
    mapper  => sub { sleep 10; $_[1] },
    reducer => sub { sleep 10; $_[1] },
);

my @mappers = map { MapReduce::Mapper->new(daemon => 1) } 1 .. 5;
my $reducer = MapReduce::Reducer->new(daemon => 1);

my @pids = map { $_->child_pid } @mappers, $reducer;

ok kill 0 => $_ for @pids;

$mr->inputs([{ key => 1 }]);

@mappers = ();
$reducer = undef;

ok !kill 0 => $_ for @pids;

done_testing;

